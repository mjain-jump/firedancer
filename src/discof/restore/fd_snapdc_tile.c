#include "utils/fd_ssctrl.h"
#include "utils/fd_zstd_frame.h"

#include "../../disco/topo/fd_topo.h"
#include "../../disco/metrics/fd_metrics.h"

#include "generated/fd_snapdc_tile_seccomp.h"

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#define NAME "snapdc"

#define ZSTD_WINDOW_SZ (1UL<<25UL) /* 32MiB */

/* Maximum number of snapld_dc reader lanes (== max snapld tile count,
   see layout.snapld_tile_count). */
#define FD_SNAPDC_LD_LANE_MAX (64UL)

/* The snapdc tile is a state machine that decompresses the full and
   optionally incremental snapshot byte stream that it receives from the
   snapld tile.  In the event that the snapshot is already uncompressed,
   this tile simply copies the stream to the next tile in the pipeline. */

struct fd_snapdc_tile {
  uint full    : 1;
  uint is_zstd : 1;
  uint dirty   : 1;  /* in the middle of a frame? */
  int state;

  ulong tile_idx;
  ulong tile_count;
  ulong frame_idx;

  ZSTD_DCtx *     zstd;
  fd_zstd_frame_t zstd_frame[1];

  /* Reader lanes.  Each snapld tile publishes the stripes of the
     compressed file that it owns (stripe_idx%lane_cnt==lane) onto its
     own lane, marking the last frag of each stripe with EOM.  The
     stripes are consumed strictly round robin, which reassembles the
     original byte stream exactly. */
  struct {
    fd_wksp_t * mem;
    ulong       chunk0;
    ulong       wmark;
    ulong       mtu;
  } in[ FD_SNAPDC_LD_LANE_MAX ];
  ulong in_cnt;

  ulong frag_pos; /* read position within the frag currently being processed */

  /* Stripe rotation over the reader lanes.  lane_cnt is in_cnt on the
     local file path and 1 on the HTTP path (only reader 0 downloads). */
  ulong lane_cnt;
  ulong lane;                                  /* lane the next stripe comes from */
  uchar lane_done[ FD_SNAPDC_LD_LANE_MAX ];    /* reader has published LOAD_COMPLETE */

  /* Control messages are barriered across all in_cnt lanes: every
     reader forwards every control message, so a control frag at the
     head of every lane means all data published before it has been
     consumed. */
  ulong pending_control;
  uchar control_seen[ FD_SNAPDC_LD_LANE_MAX ];

  struct {
    fd_wksp_t * mem;
    ulong       chunk0;
    ulong       wmark;
    ulong       chunk;
    ulong       mtu;
  } out;

  struct {
    struct {
      ulong compressed_bytes_read;
      ulong decompressed_bytes_written;
    } full;

    struct {
      ulong compressed_bytes_read;
      ulong decompressed_bytes_written;
    } incremental;
  } metrics;
};
typedef struct fd_snapdc_tile fd_snapdc_tile_t;

FD_FN_PURE static ulong
scratch_align( void ) {
  return fd_ulong_max( alignof(fd_snapdc_tile_t), 32UL );
}

FD_FN_PURE static ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  (void)tile;
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_snapdc_tile_t), sizeof(fd_snapdc_tile_t)                   );
  l = FD_LAYOUT_APPEND( l, 32UL,                      ZSTD_estimateDStreamSize( ZSTD_WINDOW_SZ ) );
  return FD_LAYOUT_FINI( l, scratch_align() );
}

static inline int
should_shutdown( fd_snapdc_tile_t * ctx ) {
  return ctx->state==FD_SNAPSHOT_STATE_SHUTDOWN;
}

static void
metrics_write( fd_snapdc_tile_t * ctx ) {
  FD_MGAUGE_SET( SNAPDC, FULL_COMPRESSED_BYTES_READ,              ctx->metrics.full.compressed_bytes_read );
  FD_MGAUGE_SET( SNAPDC, FULL_DECOMPRESSED_BYTES_WRITTEN,         ctx->metrics.full.decompressed_bytes_written );

  FD_MGAUGE_SET( SNAPDC, INCREMENTAL_COMPRESSED_BYTES_READ,       ctx->metrics.incremental.compressed_bytes_read );
  FD_MGAUGE_SET( SNAPDC, INCREMENTAL_DECOMPRESSED_BYTES_WRITTEN,  ctx->metrics.incremental.decompressed_bytes_written );

  FD_MGAUGE_SET( SNAPDC, STATE,                                   (ulong)(ctx->state) );
}

static void
transition_malformed( fd_snapdc_tile_t *  ctx,
                      fd_stem_context_t * stem ) {
  if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR ) ) return;
  ctx->state = FD_SNAPSHOT_STATE_ERROR;
  fd_stem_publish( stem, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR, 0UL, 0UL, 0UL, 0UL, 0UL );
}

static inline void
clear_control_barrier( fd_snapdc_tile_t * ctx ) {
  ctx->pending_control = ULONG_MAX;
  fd_memset( ctx->control_seen, 0, sizeof(ctx->control_seen) );
}

static inline void
reset_lane_rotation( fd_snapdc_tile_t * ctx,
                     ulong              lane_cnt ) {
  ctx->lane_cnt = lane_cnt;
  ctx->lane     = 0UL;
  fd_memset( ctx->lane_done, 0, sizeof(ctx->lane_done) );
}

/* Rotates to the next lane that still has stripes coming. */
static inline void
lane_next( fd_snapdc_tile_t * ctx ) {
  for( ulong i=0UL; i<ctx->lane_cnt; i++ ) {
    ctx->lane = (ctx->lane+1UL)%ctx->lane_cnt;
    if( FD_LIKELY( !ctx->lane_done[ ctx->lane ] ) ) return;
  }
  /* Every reader is done; no more data frags will arrive. */
}

static inline void
handle_control_frag( fd_snapdc_tile_t *  ctx,
                     fd_stem_context_t * stem,
                     ulong               in_idx,
                     ulong               sig,
                     ulong               chunk,
                     ulong               sz ) {
  if( FD_UNLIKELY( sig==FD_SNAPSHOT_MSG_LOAD_COMPLETE ) ) return;

  /* All control messages except META reset the decompression stream */
  if( FD_UNLIKELY( sig!=FD_SNAPSHOT_MSG_META ) ) {
    ulong error = ZSTD_DCtx_reset( ctx->zstd, ZSTD_reset_session_only );
    if( FD_UNLIKELY( ZSTD_isError( error ) ) ) FD_LOG_ERR(( "ZSTD_DCtx_reset failed (%lu-%s)", error, ZSTD_getErrorName( error ) ));
  }

  if( ctx->state==FD_SNAPSHOT_STATE_ERROR && sig!=FD_SNAPSHOT_MSG_CTRL_FAIL ) {
    /* Control messages move along the snapshot load pipeline.  Since
       error conditions can be triggered by any tile in the pipeline,
       it is possible to be in error state and still receive otherwise
       valid messages.  Only a fail message can revert this. */
    return;
  };

  if( FD_UNLIKELY( sig==FD_SNAPSHOT_MSG_META ) ) {
    /* Forward META to snapin so it can update the advertised
       slot/hash for redirect-based downloads. */
    FD_TEST( sz<=ctx->out.mtu );
    void * dst = fd_chunk_to_laddr( ctx->out.mem, ctx->out.chunk );
    fd_memcpy( dst, fd_chunk_to_laddr_const( ctx->in[ in_idx ].mem, chunk ), sz );
    fd_stem_publish( stem, 0UL, sig, ctx->out.chunk, sz, 0UL, 0UL, 0UL );
    ctx->out.chunk = fd_dcache_compact_next( ctx->out.chunk, ctx->out.mtu, ctx->out.chunk0, ctx->out.wmark );
    return;
  }

  int forward_msg = 1;

  switch( sig ) {
    case FD_SNAPSHOT_MSG_CTRL_INIT_FULL:
    case FD_SNAPSHOT_MSG_CTRL_INIT_INCR: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
      ctx->state = FD_SNAPSHOT_STATE_PROCESSING;
      FD_TEST( sz==sizeof(fd_ssctrl_init_t) );
      fd_ssctrl_init_t const * msg = fd_chunk_to_laddr_const( ctx->in[ in_idx ].mem, chunk );
      ctx->full = sig==FD_SNAPSHOT_MSG_CTRL_INIT_FULL;
      ctx->is_zstd = !!msg->zstd;
      ctx->dirty       = 0;
      ctx->frame_idx   = 0UL;
      ctx->frag_pos = 0UL;
      reset_lane_rotation( ctx, msg->file ? ctx->in_cnt : 1UL );
      FD_TEST( fd_zstd_frame_new( ctx->zstd_frame ) );
      if( ctx->full ) {
        ctx->metrics.full.compressed_bytes_read      = 0UL;
        ctx->metrics.full.decompressed_bytes_written = 0UL;
      } else {
        ctx->metrics.incremental.compressed_bytes_read      = 0UL;
        ctx->metrics.incremental.decompressed_bytes_written = 0UL;
      }
      fd_ssctrl_init_t * msg_out = fd_chunk_to_laddr( ctx->out.mem, ctx->out.chunk );
      fd_memcpy( msg_out, msg, sz );
      fd_stem_publish( stem, 0UL, sig, ctx->out.chunk, sz, 0UL, 0UL, 0UL );
      ctx->out.chunk = fd_dcache_compact_next( ctx->out.chunk, ctx->out.mtu, ctx->out.chunk0, ctx->out.wmark );
      forward_msg = 0; // we forward the control message in the `fd_ssctrl_init_t` message
      break;
    }

    case FD_SNAPSHOT_MSG_CTRL_FINI: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );
      ctx->state = FD_SNAPSHOT_STATE_FINISHING;
      if( FD_UNLIKELY( ctx->is_zstd && ctx->dirty ) ) {
        FD_LOG_WARNING(( "encountered end-of-file in the middle of a compressed frame for %s snapshot",
                         ctx->full ? "full" : "incremental" ));
        transition_malformed( ctx, stem );
        forward_msg = 0;
        break;
      }
      break;
    }

    case FD_SNAPSHOT_MSG_CTRL_NEXT:
    case FD_SNAPSHOT_MSG_CTRL_DONE: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
      ctx->state = FD_SNAPSHOT_STATE_IDLE;
      break;
    }

    case FD_SNAPSHOT_MSG_CTRL_ERROR: {
      FD_TEST( ctx->state!=FD_SNAPSHOT_STATE_SHUTDOWN );
      ctx->state = FD_SNAPSHOT_STATE_ERROR;
      break;
    }

    case FD_SNAPSHOT_MSG_CTRL_FAIL: {
      FD_TEST( ctx->state!=FD_SNAPSHOT_STATE_SHUTDOWN );
      ctx->state = FD_SNAPSHOT_STATE_IDLE;
      reset_lane_rotation( ctx, ctx->in_cnt );
      break;
    }

    case FD_SNAPSHOT_MSG_CTRL_SHUTDOWN: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
      ctx->state = FD_SNAPSHOT_STATE_SHUTDOWN;
      break;
    }

    default: {
      FD_LOG_ERR(( "unexpected control frag %s (%lu) in state %s (%lu)",
                   fd_ssctrl_msg_ctrl_str( sig ), sig,
                   fd_ssctrl_state_str( (ulong)ctx->state ), (ulong)ctx->state ));
      break;
    }
  }

  /* Forward the control message down the pipeline */
  if( FD_LIKELY( forward_msg ) ) {
    fd_stem_publish( stem, 0UL, sig, 0UL, 0UL, 0UL, 0UL, 0UL );
  }
}

static inline void
finish_frame( fd_snapdc_tile_t * ctx ) {
  ctx->dirty = 0;
  ctx->frame_idx++;
  FD_TEST( fd_zstd_frame_new( ctx->zstd_frame ) );
}

/* Reads until the end of the current frame (or the end of the frag, if
   that comes earlier).  Returns 1 if bytes from the next frame remain
   and stem must reprocess this frag. */
static inline int
skip_unowned_frame( fd_snapdc_tile_t *  ctx,
                    fd_stem_context_t * stem,
                    uchar const *       data,
                    ulong               sz ) {
  FD_TEST( ctx->frame_idx%ctx->tile_count!=ctx->tile_idx );
  FD_TEST( ctx->dirty || ctx->frag_pos<sz );
  ctx->dirty = 1;

  ulong skipped = 0UL;
  int scan_result = fd_zstd_frame_advance( ctx->zstd_frame, data+ctx->frag_pos, sz-ctx->frag_pos, &skipped );
  switch( scan_result ) {
    case FD_ZSTD_FRAME_ERR: {
      transition_malformed( ctx, stem );
      return 0;
    }
    case FD_ZSTD_FRAME_MORE: {
      /* Current frame spans until the end of the frag, so we can move
         onto the next frag. */
      FD_TEST( skipped+ctx->frag_pos==sz );
      ctx->frag_pos = 0UL;
      return 0;
    }
    case FD_ZSTD_FRAME_END: {
      /* A frame ends within this frag.  If the frame end coincides with
         the frag end, wait for the next frag, otherwise reprocess
         the next frame in this frag. */
      FD_TEST( skipped && skipped<=sz-ctx->frag_pos );
      ctx->frag_pos += skipped;
      finish_frame( ctx );

      if( FD_UNLIKELY( ctx->frag_pos==sz ) ) {
        ctx->frag_pos = 0UL;
        return 0;
      }
      return 1;
    }
    default: FD_LOG_ERR(( "unexpected zstd frame scan result %d", scan_result ));
  }
}

/* Decompresses up to a single frame's data.  Returns 1 if the current
   frag needs to be reprocessed. */
static inline int
process_owned_frame( fd_snapdc_tile_t *  ctx,
                     fd_stem_context_t * stem,
                     uchar const *       data,
                     ulong               sz ) {
  FD_TEST( ctx->frame_idx%ctx->tile_count==ctx->tile_idx );
  FD_TEST( ctx->dirty || ctx->frag_pos<sz );
  ctx->dirty = 1;

  uchar * out          = fd_chunk_to_laddr( ctx->out.mem, ctx->out.chunk );
  ulong   in_sz        = sz-ctx->frag_pos;
  ulong   in_consumed  = 0UL;
  ulong   out_produced = 0UL;
  ulong frame_res = ZSTD_decompressStream_simpleArgs(
      ctx->zstd,
      out,
      ctx->out.mtu,
      &out_produced,
      data+ctx->frag_pos,
      in_sz,
      &in_consumed );
  if( FD_UNLIKELY( ZSTD_isError( frame_res ) ) ) {
    FD_LOG_WARNING(( "error while decompressing %s snapshot (%u-%s)",
                     ctx->full ? "full" : "incremental",
                     ZSTD_getErrorCode( frame_res ), ZSTD_getErrorName( frame_res ) ));
    transition_malformed( ctx, stem );
    return 0;
  }

  ctx->frag_pos += in_consumed;
  FD_TEST( ctx->frag_pos<=sz );

  if( FD_LIKELY( ctx->full ) ) {
    ctx->metrics.full.compressed_bytes_read      += in_consumed;
    ctx->metrics.full.decompressed_bytes_written += out_produced;
  } else {
    ctx->metrics.incremental.compressed_bytes_read      += in_consumed;
    ctx->metrics.incremental.decompressed_bytes_written += out_produced;
  }

  if( FD_UNLIKELY( frame_res && !in_consumed && !out_produced ) ) {
    if( FD_UNLIKELY( ctx->frag_pos<sz ) ) {
      /* No progress with remaining input would retry forever */
      transition_malformed( ctx, stem );
    } else {
      /* No progress with exhausted input means zstd needs the next frag */
      ctx->frag_pos = 0UL;
    }
    return 0;
  }

  if( FD_LIKELY( out_produced || !frame_res ) ) {
    ulong out_ctl = fd_frag_meta_ctl( 0UL, 0, !frame_res, 0 );
    fd_stem_publish( stem, 0UL, FD_SNAPSHOT_MSG_DATA, ctx->out.chunk, out_produced, out_ctl, 0UL, 0UL );
    ctx->out.chunk = fd_dcache_compact_next( ctx->out.chunk, out_produced, ctx->out.chunk0, ctx->out.wmark );
  }

  if( FD_UNLIKELY( !frame_res ) ) finish_frame( ctx );

  /* frame_res==0 means the frame ended exactly at the output boundary;
     re-polling then reports "new frame expected" and would mark the
     stream dirty at a clean EOF. */
  int maybe_more_output = (out_produced==ctx->out.mtu && frame_res!=0UL) || ctx->frag_pos<sz;
  if( FD_LIKELY( !maybe_more_output ) ) ctx->frag_pos = 0UL;
  return maybe_more_output;
}

static inline int
handle_data_frag( fd_snapdc_tile_t *  ctx,
                  fd_stem_context_t * stem,
                  ulong               in_idx,
                  ulong               chunk,
                  ulong               sz ) {
  if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR ) ) {
    /* Ignore all data frags after observing an error in the stream until
       we receive fail & init control messages to restart processing. */
    return 0;
  }
  if( FD_UNLIKELY( ctx->state!=FD_SNAPSHOT_STATE_PROCESSING ) ) {
    FD_LOG_ERR(( "received unexpected data frag in state %s (%lu)",
                 fd_ssctrl_state_str( (ulong)ctx->state ), (ulong)ctx->state ));
  }

  FD_TEST( in_idx<ctx->in_cnt && !ctx->lane_done[ in_idx ] );
  FD_TEST( chunk>=ctx->in[ in_idx ].chunk0 && chunk<=ctx->in[ in_idx ].wmark && sz<=ctx->in[ in_idx ].mtu && sz>=ctx->frag_pos );
  uchar const * data = fd_chunk_to_laddr_const( ctx->in[ in_idx ].mem, chunk );

  if( FD_UNLIKELY( !ctx->is_zstd ) ) {
    if( FD_UNLIKELY( ctx->tile_idx!=0UL ) ) return 0;
    FD_TEST( ctx->frag_pos<sz );
    uchar const * in  = data+ctx->frag_pos;
    uchar *       out = fd_chunk_to_laddr( ctx->out.mem, ctx->out.chunk );
    ulong cpy = fd_ulong_min( sz-ctx->frag_pos, ctx->out.mtu );
    fd_memcpy( out, in, cpy );
    fd_stem_publish( stem, 0UL, FD_SNAPSHOT_MSG_DATA, ctx->out.chunk, cpy, 0UL, 0UL, 0UL );
    ctx->out.chunk = fd_dcache_compact_next( ctx->out.chunk, cpy, ctx->out.chunk0, ctx->out.wmark );

    if( FD_LIKELY( ctx->full ) ) {
      ctx->metrics.full.compressed_bytes_read      += cpy;
      ctx->metrics.full.decompressed_bytes_written += cpy;
    } else {
      ctx->metrics.incremental.compressed_bytes_read      += cpy;
      ctx->metrics.incremental.decompressed_bytes_written += cpy;
    }

    ctx->frag_pos += cpy;
    FD_TEST( ctx->frag_pos<=sz );
    if( FD_UNLIKELY( ctx->frag_pos<sz ) ) return 1;
    ctx->frag_pos = 0UL;
    return 0;
  }

  if( ctx->frame_idx%ctx->tile_count!=ctx->tile_idx ) {
    return skip_unowned_frame( ctx, stem, data, sz );
  }

  return process_owned_frame( ctx, stem, data, sz );
}

static inline int
all_controls_seen( fd_snapdc_tile_t const * ctx ) {
  int all_seen = 1;
  for( ulong i=0UL; i<ctx->in_cnt; i++ ) all_seen &= !!ctx->control_seen[ i ];
  return all_seen;
}

static inline int
before_frag( fd_snapdc_tile_t * ctx,
             ulong              in_idx,
             ulong              seq    FD_PARAM_UNUSED,
             ulong              sig ) {
  /* A single reader tile is a single ordered stream, so none of the
     lane bookkeeping below applies. */
  if( FD_LIKELY( ctx->in_cnt==1UL ) ) return 0;

  /* In ERROR state the stream is abandoned: drop everything in flight
     until the FAIL that resets the pipeline. */
  if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR ) ) return sig!=FD_SNAPSHOT_MSG_CTRL_FAIL;

  /* An error can be raised by any reader at any point and must not
     wait for the other lanes. */
  if( FD_UNLIKELY( sig==FD_SNAPSHOT_MSG_CTRL_ERROR ) ) return 0;

  /* Once this lane has delivered the pending control message, hold its
     later frags until every lane has delivered the same one. */
  if( FD_UNLIKELY( ctx->pending_control!=ULONG_MAX && ctx->control_seen[ in_idx ] ) ) return -1;

  /* Stripes (and the end-of-lane markers that terminate them) are
     consumed strictly round robin, which is what makes the
     concatenation of the lanes the original compressed byte stream. */
  if( FD_LIKELY( sig==FD_SNAPSHOT_MSG_DATA || sig==FD_SNAPSHOT_MSG_LOAD_COMPLETE ) ) {
    if( FD_UNLIKELY( in_idx!=ctx->lane ) ) return -1;
  }

  return 0;
}

static inline void
handle_control_barrier( fd_snapdc_tile_t *  ctx,
                        fd_stem_context_t * stem,
                        ulong               in_idx,
                        ulong               sig,
                        ulong               chunk,
                        ulong               sz ) {
  /* META is published by the lead reader only, and an error must be
     acted on immediately; neither is barriered.  before_frag holds
     both behind an in-flight barrier. */
  if( FD_UNLIKELY( ctx->in_cnt==1UL ||
                   sig==FD_SNAPSHOT_MSG_CTRL_ERROR ||
                   sig==FD_SNAPSHOT_MSG_META ) ) {
    handle_control_frag( ctx, stem, in_idx, sig, chunk, sz );
    return;
  }

  if( FD_UNLIKELY( sig!=ctx->pending_control ) ) {
    FD_TEST( ctx->pending_control==ULONG_MAX || sig==FD_SNAPSHOT_MSG_CTRL_FAIL );
    clear_control_barrier( ctx );
    ctx->pending_control = sig;
  }

  FD_TEST( !ctx->control_seen[ in_idx ] );
  ctx->control_seen[ in_idx ] = 1U;
  if( FD_LIKELY( !all_controls_seen( ctx ) ) ) return;

  clear_control_barrier( ctx );
  handle_control_frag( ctx, stem, in_idx, sig, chunk, sz );
}

static inline int
returnable_frag( fd_snapdc_tile_t *  ctx,
                 ulong               in_idx,
                 ulong               seq    FD_PARAM_UNUSED,
                 ulong               sig,
                 ulong               chunk,
                 ulong               sz,
                 ulong               ctl,
                 ulong               tsorig FD_PARAM_UNUSED,
                 ulong               tspub  FD_PARAM_UNUSED,
                 fd_stem_context_t * stem ) {
  FD_TEST( ctx->state!=FD_SNAPSHOT_STATE_SHUTDOWN );

  if( FD_LIKELY( sig==FD_SNAPSHOT_MSG_DATA ) ) {
    if( FD_UNLIKELY( handle_data_frag( ctx, stem, in_idx, chunk, sz ) ) ) return 1;
    /* The reader marks the last frag of each of its stripes, which is
       where the decompressor moves on to the next lane. */
    if( FD_UNLIKELY( fd_frag_meta_ctl_eom( ctl ) ) ) lane_next( ctx );
    return 0;
  }

  if( FD_UNLIKELY( sig==FD_SNAPSHOT_MSG_LOAD_COMPLETE ) ) {
    /* This reader has published its last stripe; drop its lane from the
       rotation. */
    FD_TEST( in_idx<ctx->lane_cnt );
    ctx->lane_done[ in_idx ] = 1U;
    lane_next( ctx );
    return 0;
  }

  handle_control_barrier( ctx, stem, in_idx, sig, chunk, sz );
  return 0;
}

static ulong
populate_allowed_fds( fd_topo_t      const * topo FD_PARAM_UNUSED,
                      fd_topo_tile_t const * tile FD_PARAM_UNUSED,
                      ulong                  out_fds_cnt,
                      int *                  out_fds ) {
  if( FD_UNLIKELY( out_fds_cnt<2UL ) ) FD_LOG_ERR(( "out_fds_cnt %lu", out_fds_cnt ));

  ulong out_cnt = 0;
  out_fds[ out_cnt++ ] = 2UL; /* stderr */
  if( FD_LIKELY( -1!=fd_log_private_logfile_fd() ) ) {
    out_fds[ out_cnt++ ] = fd_log_private_logfile_fd(); /* logfile */
  }

  return out_cnt;
}

static ulong
populate_allowed_seccomp( fd_topo_t const *      topo FD_PARAM_UNUSED,
                          fd_topo_tile_t const * tile FD_PARAM_UNUSED,
                          ulong                  out_cnt,
                          struct sock_filter *   out ) {
  populate_sock_filter_policy_fd_snapdc_tile( out_cnt, out, (uint)fd_log_private_logfile_fd() );
  return sock_filter_policy_fd_snapdc_tile_instr_cnt;
}

static void
unprivileged_init( fd_topo_t const *      topo,
                   fd_topo_tile_t const * tile ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_snapdc_tile_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_snapdc_tile_t), sizeof(fd_snapdc_tile_t) );
  void * _zstd           = FD_SCRATCH_ALLOC_APPEND( l, 32UL,                      ZSTD_estimateDStreamSize( ZSTD_WINDOW_SZ ) );

  ctx->state      = FD_SNAPSHOT_STATE_IDLE;
  ctx->tile_idx   = tile->kind_id;
  ctx->tile_count = fd_topo_tile_name_cnt( topo, NAME );
  FD_TEST( ctx->tile_count );
  FD_TEST( ctx->tile_idx<ctx->tile_count );

  ctx->zstd = ZSTD_initStaticDStream( _zstd, ZSTD_estimateDStreamSize( ZSTD_WINDOW_SZ ) );
  FD_TEST( ctx->zstd );
  FD_TEST( ctx->zstd==_zstd );

  ctx->dirty       = 0;
  ctx->frame_idx   = 0UL;
  ctx->frag_pos = 0UL;
  FD_TEST( fd_zstd_frame_new( ctx->zstd_frame ) );
  fd_memset( &ctx->metrics, 0, sizeof(ctx->metrics) );

  if( FD_UNLIKELY( !tile->in_cnt || tile->in_cnt>FD_SNAPDC_LD_LANE_MAX ) ) FD_LOG_ERR(( "tile `" NAME "` has %lu ins, expected [1,%lu]",  tile->in_cnt, FD_SNAPDC_LD_LANE_MAX ));
  if( FD_UNLIKELY( tile->out_cnt!=1UL ) ) FD_LOG_ERR(( "tile `" NAME "` has %lu outs, expected 1", tile->out_cnt ));

  fd_topo_link_t const * snapin_link = &topo->links[ tile->out_link_id[ 0UL ] ];
  FD_TEST( 0==strcmp( snapin_link->name, "snapdc_in" ) );
  ctx->out.mem    = topo->workspaces[ topo->objs[ snapin_link->dcache_obj_id ].wksp_id ].wksp;
  ctx->out.chunk0 = fd_dcache_compact_chunk0( ctx->out.mem, snapin_link->dcache );
  ctx->out.wmark  = fd_dcache_compact_wmark ( ctx->out.mem, snapin_link->dcache, snapin_link->mtu );
  ctx->out.chunk  = ctx->out.chunk0;
  ctx->out.mtu    = snapin_link->mtu;

  ctx->in_cnt = tile->in_cnt;
  for( ulong i=0UL; i<tile->in_cnt; i++ ) {
    fd_topo_link_t const * in_link = &topo->links[ tile->in_link_id[ i ] ];
    FD_TEST( 0==strcmp( in_link->name, "snapld_dc" ) );
    fd_topo_wksp_t const * in_wksp = &topo->workspaces[ topo->objs[ in_link->dcache_obj_id ].wksp_id ];
    ctx->in[ i ].mem    = in_wksp->wksp;
    ctx->in[ i ].chunk0 = fd_dcache_compact_chunk0( ctx->in[ i ].mem, in_link->dcache );
    ctx->in[ i ].wmark  = fd_dcache_compact_wmark( ctx->in[ i ].mem, in_link->dcache, in_link->mtu );
    ctx->in[ i ].mtu    = in_link->mtu;
  }
  clear_control_barrier( ctx );
  reset_lane_rotation( ctx, ctx->in_cnt );

  ulong scratch_top = FD_SCRATCH_ALLOC_FINI( l, scratch_align() );
  if( FD_UNLIKELY( scratch_top > (ulong)scratch + scratch_footprint( tile ) ) )
    FD_LOG_ERR(( "scratch overflow %lu %lu %lu",
                 scratch_top - (ulong)scratch - scratch_footprint( tile ),
                 scratch_top,
                 (ulong)scratch + scratch_footprint( tile ) ));
}

#define STEM_BURST 1UL

#define STEM_LAZY  (128L*3000L)

#define STEM_CALLBACK_CONTEXT_TYPE  fd_snapdc_tile_t
#define STEM_CALLBACK_CONTEXT_ALIGN alignof(fd_snapdc_tile_t)

#define STEM_CALLBACK_SHOULD_SHUTDOWN should_shutdown
#define STEM_CALLBACK_METRICS_WRITE   metrics_write
#define STEM_CALLBACK_BEFORE_FRAG     before_frag
#define STEM_CALLBACK_RETURNABLE_FRAG returnable_frag

#include "../../disco/stem/fd_stem.c"

fd_topo_run_tile_t fd_tile_snapdc = {
  .name                     = NAME,
  .populate_allowed_fds     = populate_allowed_fds,
  .populate_allowed_seccomp = populate_allowed_seccomp,
  .scratch_align            = scratch_align,
  .scratch_footprint        = scratch_footprint,
  .unprivileged_init        = unprivileged_init,
  .run                      = stem_run,
};

#undef NAME
