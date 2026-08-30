#include "utils/fd_ssctrl.h"
#include "utils/fd_zstd_frame.h"

#include "../../disco/topo/fd_topo.h"
#include "../../disco/metrics/fd_metrics.h"
#include "../../util/archive/fd_tar.h"

#include "generated/fd_snapdc_tile_seccomp.h"

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#define NAME "snapdc"

#define ZSTD_WINDOW_SZ (1UL<<25UL) /* 32MiB */

/* The snapdc tile is a state machine that decompresses the full and
   optionally incremental snapshot byte stream that it receives from the
   snapld tile.  In the event that the snapshot is already uncompressed,
   this tile simply copies the stream to the next tile in the pipeline.

   It runs in one of two output modes, selected by the topology:

   BROADCAST mode (out link `snapdc_in`): the decompressed bytes are
   published verbatim as one lane of the global ordered stream, and
   every downstream snapin tile consumes every lane.  This is the
   original behaviour.

   ROUTING mode (out links `snapdc_rt` + `snapdc_cr`): the tile
   additionally parses the tar entry headers inside its own frames and
   publishes each entry only to the snapin tile that owns it
   (appendvec_ordinal % snapin_tile_cnt; the prologue entries -- version,
   directories, status cache, manifest -- always go to tile 0).  A snapin
   tile therefore receives, and walks, only its ~1/N share of the
   stream.  That matters because `fd_frag_meta_t` stores the frag size in
   16 bits, so a frag can never exceed FD_SNAPSHOT_DATA_MTU and the only
   way to amortize a consumer's per-frag cost is to hand it fewer frags.

   Two things make routing possible without giving up parallel
   decompression:

     - Frame ownership stays `frame_idx % tile_count`, and decompression
       of an owned frame runs into a private whole-frame buffer with no
       cross-tile dependency at all.
     - Only the tar WALK is chained, through the `snapdc_cr` ring
       (fd_ssctrl_route_carry_t): the tile that owns frame k-1 tells the
       tile that owns frame k where the next tar header sits and which
       appendvec ordinal comes next.  Walking is ~one 512 byte header per
       3 MiB of payload, i.e. negligible next to the ~9 ms it takes to
       decompress the frame it is walking.

   Every frame the tile owns ends with one zero-length EOM frag per
   target, so a snapin tile's expected-frame lane rotation still advances
   exactly once per frame even for frames that held nothing for it. */

/* Whole-frame staging buffer for routing mode.  Agave chunks the
   uncompressed stream at a fixed 32 MiB and compresses each chunk as an
   independent frame, so one buffer holds exactly one frame; a larger
   frame is simply routed in 32 MiB pieces (the walk state carries
   across the pieces the same way it carries across frames), and because
   the buffer size is a multiple of 512 a tar header is never split. */

#define FD_SNAPDC_FRAMEBUF_SZ (1UL<<25UL) /* 32 MiB */
#define FD_SNAPDC_OUT_MAX     (FD_TOPO_MAX_TILE_OUT_LINKS)
#define FD_SNAPDC_EMIT_BATCH  (64UL)

FD_STATIC_ASSERT( FD_SNAPDC_FRAMEBUF_SZ%512UL==0UL, framebuf_tar_aligned );

struct fd_snapdc_out {
  fd_wksp_t * mem;
  ulong       chunk0;
  ulong       wmark;
  ulong       chunk;
  ulong       mtu;
};

typedef struct fd_snapdc_out fd_snapdc_out_t;

struct fd_snapdc_tile {
  uint full    : 1;
  uint is_zstd : 1;
  uint dirty   : 1;  /* in the middle of a frame? */
  uint route   : 1;  /* routing mode? */
  int state;

  ulong tile_idx;
  ulong tile_count;
  ulong frame_idx;
  ulong generation; /* attempt counter, bumped at every INIT */

  ZSTD_DCtx *     zstd;
  fd_zstd_frame_t zstd_frame[1];

  struct {
    fd_wksp_t * mem;
    ulong       chunk0;
    ulong       wmark;
    ulong       mtu;
    ulong       frag_pos;
  } in;

  struct {
    fd_wksp_t * mem;
  } cr_in;

  /* Routing mode.  out[ 0, target_cnt ) are the per-snapin routed
     links; cr_out is the walk-state hand-off link (ULONG_MAX when there
     is only one snapdc tile and the walk never leaves this tile). */
  ulong           target_cnt;
  ulong           cr_out;
  fd_snapdc_out_t out[ FD_SNAPDC_OUT_MAX ];

  uchar * framebuf;
  ulong   buf_len;        /* decompressed bytes staged in framebuf */
  uint    buf_frame_end : 1; /* the staged bytes end at a zstd frame boundary */
  uint    emit_pending  : 1; /* framebuf holds bytes that still have to be routed */
  uint    have_carry    : 1; /* the walk state for the staged bytes is known */
  uint    saw_eof       : 1; /* the tar end-of-archive marker was already routed */
  uint    eof_pending   : 1; /* still publishing synthesized end-of-archive blocks */
  uint    scanned       : 1; /* the staged bytes were walked and the hand-off went out */
  fd_ssctrl_route_carry_t carry_out[1]; /* walk state at the end of the staged bytes */
  ulong   emit_pos;       /* framebuf offset of the next byte to route */
  ulong   run_left;       /* bytes left in the tar entry run being routed */
  ulong   run_target;     /* snapin tile owning that run */
  ulong   av_ord;         /* stream ordinal of the next appendvec header */
  ulong   mark_target;    /* next target to receive this frame's EOM marker */
  ulong   eof_target;     /* next target to receive the end-of-archive blocks */

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
  l = FD_LAYOUT_APPEND( l, 4096UL,                    FD_SNAPDC_FRAMEBUF_SZ                     );
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

/* Routing helpers ****************************************************/

static inline void
route_reset_emit( fd_snapdc_tile_t * ctx ) {
  ctx->buf_len       = 0UL;
  ctx->buf_frame_end = 0;
  ctx->emit_pending  = 0;
  ctx->eof_pending   = 0;
  ctx->scanned       = 0;
  ctx->emit_pos      = 0UL;
  ctx->mark_target   = 0UL;
  ctx->eof_target    = 0UL;
}

static inline void
route_reset_attempt( fd_snapdc_tile_t * ctx ) {
  route_reset_emit( ctx );
  ctx->run_left   = 0UL;
  ctx->run_target = 0UL;
  ctx->av_ord     = 0UL;
  ctx->saw_eof    = 0;
  fd_memset( ctx->carry_out, 0, sizeof(ctx->carry_out) );
  /* Frame 0's walk state needs no hand-off, so the tile that owns it
     starts with the carry already in hand. */
  ctx->have_carry = ctx->tile_idx==0UL;
}

/* Copies sz bytes into target's dcache and publishes them.  sz==0
   publishes the frame-end marker, which costs no dcache space. */

static inline void
route_publish( fd_snapdc_tile_t *  ctx,
               fd_stem_context_t * stem,
               ulong               target,
               uchar const *       data,
               ulong               sz,
               int                 eom ) {
  fd_snapdc_out_t * o = &ctx->out[ target ];
  FD_TEST( target<ctx->target_cnt && sz<=o->mtu );
  if( FD_LIKELY( sz ) ) fd_memcpy( fd_chunk_to_laddr( o->mem, o->chunk ), data, sz );
  fd_stem_publish( stem, target, FD_SNAPSHOT_MSG_DATA, o->chunk, sz,
                   fd_frag_meta_ctl( 0UL, 0, eom, 0 ), 0UL, 0UL );
  if( FD_LIKELY( sz ) ) o->chunk = fd_dcache_compact_next( o->chunk, sz, o->chunk0, o->wmark );
}

static inline void
route_publish_carry( fd_snapdc_tile_t *              ctx,
                     fd_stem_context_t *             stem,
                     ulong                           frame_idx,
                     fd_ssctrl_route_carry_t const * state,
                     ulong                           flags ) {
  if( FD_UNLIKELY( ctx->cr_out==ULONG_MAX ) ) return;
  fd_snapdc_out_t * o = &ctx->out[ ctx->cr_out ];
  fd_ssctrl_route_carry_t * carry = fd_chunk_to_laddr( o->mem, o->chunk );
  *carry            = *state;
  carry->generation = ctx->generation;
  carry->frame_idx  = frame_idx;
  carry->flags     |= flags;
  fd_stem_publish( stem, ctx->cr_out, FD_SNAPSHOT_MSG_ROUTE_CARRY, o->chunk, sizeof(fd_ssctrl_route_carry_t), 0UL, 0UL, 0UL );
  o->chunk = fd_dcache_compact_next( o->chunk, sizeof(fd_ssctrl_route_carry_t), o->chunk0, o->wmark );
}

static void
transition_malformed( fd_snapdc_tile_t *  ctx,
                      fd_stem_context_t * stem ) {
  if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR ) ) return;
  ctx->state = FD_SNAPSHOT_STATE_ERROR;
  /* Release the tile that is waiting for our walk state, otherwise it
     sits on its own input forever and never sees the ERROR behind it. */
  if( FD_UNLIKELY( ctx->route ) ) {
    route_reset_emit( ctx );
    fd_ssctrl_route_carry_t stop[1] = {{0}};
    route_publish_carry( ctx, stem, ULONG_MAX, stop, FD_SSCTRL_CARRY_ABORT );
  }
  for( ulong t=0UL; t<ctx->target_cnt; t++ ) {
    fd_stem_publish( stem, t, FD_SNAPSHOT_MSG_CTRL_ERROR, 0UL, 0UL, 0UL, 0UL, 0UL );
  }
}

/* Parses the tar header at `hdr` and reports the entry run it
   introduces.  av_ord is in/out.  Returns 0 on a malformed header. */

static int
route_entry_at( fd_snapdc_tile_t const * ctx,
                uchar const *            hdr,
                ulong *                  av_ord,
                ulong *                  run,
                ulong *                  target,
                int *                    eof ) {
  fd_tar_meta_t const * meta = (fd_tar_meta_t const *)hdr;

  *eof = 0;
  if( FD_UNLIKELY( memcmp( meta->magic, FD_TAR_MAGIC, FD_TAR_MAGIC_SZ ) ) ) {
    int not_zero = 0;
    for( ulong i=0UL; i<512UL; i++ ) not_zero |= hdr[ i ];
    if( FD_UNLIKELY( not_zero ) ) {
      FD_LOG_WARNING(( "invalid tar header magic `%." FD_EXPAND_THEN_STRINGIFY(FD_TAR_MAGIC_SZ) "s` while routing", meta->magic ));
      return 0;
    }
    *eof = 1;
    return 1;
  }

  ulong file_bytes = fd_tar_meta_get_size( meta );
  if( FD_UNLIKELY( file_bytes==ULONG_MAX ) ) {
    FD_LOG_WARNING(( "invalid tar header size for name %." FD_EXPAND_THEN_STRINGIFY(FD_TAR_NAME_SZ) "s while routing", meta->name ));
    return 0;
  }

  /* The routed run is the whole tar record -- header, body and the
     padding up to the next 512 byte boundary -- so the owner's parser
     sees a byte-exact sub-stream of the archive and needs no new entry
     point.  Directory entries carry a zero size and are just the
     header. */
  *run = 512UL + fd_ulong_align_up( file_bytes, 512UL );

  if( FD_LIKELY( !strncmp( meta->name, "accounts/", 9UL ) ) ) {
    *target = (*av_ord)%ctx->target_cnt;
    (*av_ord)++;
  } else {
    /* version, the snapshots/ directories, the status cache and the
       manifest.  They are all in the first handful of frames and only
       tile 0 parses them. */
    *target = 0UL;
  }

  return 1;
}

/* Walks the tar headers across the whole staged buffer WITHOUT
   publishing anything, to compute the walk state at its end.  This is
   what gets handed to the next frame's owner, and handing it over before
   any of this frame's bytes are published is what keeps the chain off
   the critical path: the hand-off then costs one header parse per ~3 MiB
   instead of waiting for a whole frame to be copied out and drained by
   its consumers.  Returns 0 on a malformed header. */

static int
route_scan( fd_snapdc_tile_t * ctx ) {
  ulong pos    = ctx->emit_pos;
  ulong left   = ctx->run_left;
  ulong target = ctx->run_target;
  ulong ord    = ctx->av_ord;
  int   eof    = ctx->saw_eof;

  while( pos<ctx->buf_len && !eof ) {
    if( FD_UNLIKELY( left ) ) {
      ulong n = fd_ulong_min( left, ctx->buf_len-pos );
      pos  += n;
      left -= n;
      continue;
    }
    if( FD_UNLIKELY( ctx->buf_len-pos<512UL ) ) {
      FD_LOG_WARNING(( "snapshot zstd frame content is not a multiple of the 512 byte tar block size "
                       "(%lu trailing bytes); the routing snapdc requires 512 byte aligned frame boundaries",
                       ctx->buf_len-pos ));
      return 0;
    }
    if( FD_UNLIKELY( !route_entry_at( ctx, ctx->framebuf+pos, &ord, &left, &target, &eof ) ) ) return 0;
    if( FD_UNLIKELY( eof ) ) break;
    FD_TEST( left ); /* every tar record is at least its 512 byte header */
  }

  ctx->carry_out->run_left   = left;
  ctx->carry_out->run_target = target;
  ctx->carry_out->av_ord     = ord;
  ctx->carry_out->flags      = fd_ulong_if( eof, FD_SSCTRL_CARRY_EOF, 0UL );
  return 1;
}

/* One publish worth of routing work.  Returns 1 if it published, 0 if
   the staged buffer is fully routed (or the tile errored out). */

static int
route_emit_one( fd_snapdc_tile_t *  ctx,
                fd_stem_context_t * stem ) {
  /* Synthesized end-of-archive blocks, one target per call. */
  if( FD_UNLIKELY( ctx->eof_pending ) ) {
    static uchar const zero[ 1024UL ] = {0};
    route_publish( ctx, stem, ctx->eof_target++, zero, sizeof(zero), 0 );
    if( FD_UNLIKELY( ctx->eof_target>=ctx->target_cnt ) ) ctx->eof_pending = 0;
    return 1;
  }

  /* Route entry bytes. */
  if( FD_LIKELY( ctx->emit_pos<ctx->buf_len && !ctx->saw_eof ) ) {
    if( FD_UNLIKELY( !ctx->run_left ) ) {
      int eof = 0;
      /* route_scan already validated the whole buffer, so this cannot
         fail here. */
      FD_TEST( ctx->buf_len-ctx->emit_pos>=512UL );
      FD_TEST( route_entry_at( ctx, ctx->framebuf+ctx->emit_pos, &ctx->av_ord, &ctx->run_left, &ctx->run_target, &eof ) );
      if( FD_UNLIKELY( eof ) ) {
        /* Every snapin tile needs the two zero blocks to reach its own
           end-of-stream, and they are synthesized rather than forwarded
           so a tile's routed stream terminates identically no matter
           where the real padding fell. */
        ctx->saw_eof     = 1;
        ctx->eof_pending = 1;
        ctx->eof_target  = 0UL;
        return 1; /* nothing published, but state advanced */
      }
    }
    ulong n = fd_ulong_min( ctx->run_left, ctx->buf_len-ctx->emit_pos );
    n       = fd_ulong_min( n, ctx->out[ ctx->run_target ].mtu );
    route_publish( ctx, stem, ctx->run_target, ctx->framebuf+ctx->emit_pos, n, 0 );
    ctx->emit_pos += n;
    ctx->run_left -= n;
    return 1;
  }

  if( FD_LIKELY( ctx->buf_frame_end ) ) {
    /* One zero-length EOM per target, so every snapin tile's lane
       rotation advances past this frame whether or not the frame held
       anything for it. */
    if( FD_LIKELY( ctx->mark_target<ctx->target_cnt ) ) {
      route_publish( ctx, stem, ctx->mark_target++, NULL, 0UL, 1 );
      return 1;
    }
    /* The next frame we own needs a fresh hand-off. */
    ctx->have_carry = 0;
    ctx->dirty      = 0;
    ctx->frame_idx++;
    FD_TEST( fd_zstd_frame_new( ctx->zstd_frame ) );
  }

  route_reset_emit( ctx );
  return 0;
}

static void
after_credit( fd_snapdc_tile_t *  ctx,
              fd_stem_context_t * stem,
              int *               opt_poll_in FD_PARAM_UNUSED,
              int *               charge_busy ) {
  if( FD_LIKELY( !ctx->emit_pending ) ) return;
  if( FD_UNLIKELY( ctx->state!=FD_SNAPSHOT_STATE_PROCESSING ) ) {
    /* An ERROR/FAIL overtook the staged bytes; drop them. */
    route_reset_emit( ctx );
    return;
  }
  /* The tile that owns the previous frame has not told us where our
     first tar header is yet.  Frames are received in order on the
     broadcast input, so in steady state the hand-off already happened
     before we finished decompressing. */
  if( FD_UNLIKELY( !ctx->have_carry ) ) return;

  *charge_busy = 1;

  if( FD_UNLIKELY( !ctx->scanned ) ) {
    if( FD_UNLIKELY( !route_scan( ctx ) ) ) {
      transition_malformed( ctx, stem );
      return;
    }
    ctx->scanned = 1;
    if( FD_LIKELY( ctx->buf_frame_end ) ) {
      route_publish_carry( ctx, stem, ctx->frame_idx+1UL, ctx->carry_out, 0UL );
    }
  }

  for( ulong pub=0UL; pub<FD_SNAPDC_EMIT_BATCH; pub++ ) {
    if( FD_UNLIKELY( !route_emit_one( ctx, stem ) ) ) break;
    if( FD_UNLIKELY( ctx->state!=FD_SNAPSHOT_STATE_PROCESSING ) ) break;
  }
}

static inline void
handle_control_frag( fd_snapdc_tile_t *  ctx,
                     fd_stem_context_t * stem,
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
    for( ulong t=0UL; t<ctx->target_cnt; t++ ) {
      fd_snapdc_out_t * o = &ctx->out[ t ];
      FD_TEST( sz<=o->mtu );
      void * dst = fd_chunk_to_laddr( o->mem, o->chunk );
      fd_memcpy( dst, fd_chunk_to_laddr_const( ctx->in.mem, chunk ), sz );
      fd_stem_publish( stem, t, sig, o->chunk, sz, 0UL, 0UL, 0UL );
      o->chunk = fd_dcache_compact_next( o->chunk, sz, o->chunk0, o->wmark );
    }
    return;
  }

  int forward_msg = 1;

  switch( sig ) {
    case FD_SNAPSHOT_MSG_CTRL_INIT_FULL:
    case FD_SNAPSHOT_MSG_CTRL_INIT_INCR: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
      ctx->state = FD_SNAPSHOT_STATE_PROCESSING;
      FD_TEST( sz==sizeof(fd_ssctrl_init_t) );
      fd_ssctrl_init_t const * msg = fd_chunk_to_laddr_const( ctx->in.mem, chunk );
      ctx->full = sig==FD_SNAPSHOT_MSG_CTRL_INIT_FULL;
      ctx->is_zstd = !!msg->zstd;
      ctx->dirty       = 0;
      ctx->frame_idx   = 0UL;
      ctx->in.frag_pos = 0UL;
      ctx->generation++;
      if( FD_UNLIKELY( ctx->route ) ) route_reset_attempt( ctx );
      FD_TEST( fd_zstd_frame_new( ctx->zstd_frame ) );
      if( ctx->full ) {
        ctx->metrics.full.compressed_bytes_read      = 0UL;
        ctx->metrics.full.decompressed_bytes_written = 0UL;
      } else {
        ctx->metrics.incremental.compressed_bytes_read      = 0UL;
        ctx->metrics.incremental.decompressed_bytes_written = 0UL;
      }
      for( ulong t=0UL; t<ctx->target_cnt; t++ ) {
        fd_snapdc_out_t *  o       = &ctx->out[ t ];
        fd_ssctrl_init_t * msg_out = fd_chunk_to_laddr( o->mem, o->chunk );
        fd_memcpy( msg_out, msg, sz );
        fd_stem_publish( stem, t, sig, o->chunk, sz, 0UL, 0UL, 0UL );
        o->chunk = fd_dcache_compact_next( o->chunk, o->mtu, o->chunk0, o->wmark );
      }
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
      if( FD_UNLIKELY( ctx->route ) ) {
        route_reset_emit( ctx );
        fd_ssctrl_route_carry_t stop[1] = {{0}};
        route_publish_carry( ctx, stem, ULONG_MAX, stop, FD_SSCTRL_CARRY_ABORT );
      }
      break;
    }

    case FD_SNAPSHOT_MSG_CTRL_FAIL: {
      FD_TEST( ctx->state!=FD_SNAPSHOT_STATE_SHUTDOWN );
      ctx->state = FD_SNAPSHOT_STATE_IDLE;
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
    for( ulong t=0UL; t<ctx->target_cnt; t++ ) fd_stem_publish( stem, t, sig, 0UL, 0UL, 0UL, 0UL, 0UL );
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
  FD_TEST( ctx->dirty || ctx->in.frag_pos<sz );
  ctx->dirty = 1;

  ulong skipped = 0UL;
  int scan_result = fd_zstd_frame_advance( ctx->zstd_frame, data+ctx->in.frag_pos, sz-ctx->in.frag_pos, &skipped );
  switch( scan_result ) {
    case FD_ZSTD_FRAME_ERR: {
      transition_malformed( ctx, stem );
      return 0;
    }
    case FD_ZSTD_FRAME_MORE: {
      /* Current frame spans until the end of the frag, so we can move
         onto the next frag. */
      FD_TEST( skipped+ctx->in.frag_pos==sz );
      ctx->in.frag_pos = 0UL;
      return 0;
    }
    case FD_ZSTD_FRAME_END: {
      /* A frame ends within this frag.  If the frame end coincides with
         the frag end, wait for the next frag, otherwise reprocess
         the next frame in this frag. */
      FD_TEST( skipped && skipped<=sz-ctx->in.frag_pos );
      ctx->in.frag_pos += skipped;
      finish_frame( ctx );

      if( FD_UNLIKELY( ctx->in.frag_pos==sz ) ) {
        ctx->in.frag_pos = 0UL;
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
  FD_TEST( ctx->dirty || ctx->in.frag_pos<sz );
  ctx->dirty = 1;

  uchar * out          = fd_chunk_to_laddr( ctx->out[ 0 ].mem, ctx->out[ 0 ].chunk );
  ulong   out_cap      = ctx->out[ 0 ].mtu;
  ulong   in_sz        = sz-ctx->in.frag_pos;
  ulong   in_consumed  = 0UL;
  ulong   out_produced = 0UL;
  ulong frame_res = ZSTD_decompressStream_simpleArgs(
      ctx->zstd,
      out,
      out_cap,
      &out_produced,
      data+ctx->in.frag_pos,
      in_sz,
      &in_consumed );
  if( FD_UNLIKELY( ZSTD_isError( frame_res ) ) ) {
    FD_LOG_WARNING(( "error while decompressing %s snapshot (%u-%s)",
                     ctx->full ? "full" : "incremental",
                     ZSTD_getErrorCode( frame_res ), ZSTD_getErrorName( frame_res ) ));
    transition_malformed( ctx, stem );
    return 0;
  }

  ctx->in.frag_pos += in_consumed;
  FD_TEST( ctx->in.frag_pos<=sz );

  if( FD_LIKELY( ctx->full ) ) {
    ctx->metrics.full.compressed_bytes_read      += in_consumed;
    ctx->metrics.full.decompressed_bytes_written += out_produced;
  } else {
    ctx->metrics.incremental.compressed_bytes_read      += in_consumed;
    ctx->metrics.incremental.decompressed_bytes_written += out_produced;
  }

  if( FD_UNLIKELY( frame_res && !in_consumed && !out_produced ) ) {
    if( FD_UNLIKELY( ctx->in.frag_pos<sz ) ) {
      /* No progress with remaining input would retry forever */
      transition_malformed( ctx, stem );
    } else {
      /* No progress with exhausted input means zstd needs the next frag */
      ctx->in.frag_pos = 0UL;
    }
    return 0;
  }

  if( FD_LIKELY( out_produced || !frame_res ) ) {
    ulong out_ctl = fd_frag_meta_ctl( 0UL, 0, !frame_res, 0 );
    fd_stem_publish( stem, 0UL, FD_SNAPSHOT_MSG_DATA, ctx->out[ 0 ].chunk, out_produced, out_ctl, 0UL, 0UL );
    ctx->out[ 0 ].chunk = fd_dcache_compact_next( ctx->out[ 0 ].chunk, out_produced, ctx->out[ 0 ].chunk0, ctx->out[ 0 ].wmark );
  }

  if( FD_UNLIKELY( !frame_res ) ) finish_frame( ctx );

  /* frame_res==0 means the frame ended exactly at the output boundary;
     re-polling then reports "new frame expected" and would mark the
     stream dirty at a clean EOF. */
  int maybe_more_output = (out_produced==out_cap && frame_res!=0UL) || ctx->in.frag_pos<sz;
  if( FD_LIKELY( !maybe_more_output ) ) ctx->in.frag_pos = 0UL;
  return maybe_more_output;
}

/* Routing mode: decompress an owned frame into the private whole-frame
   buffer.  Nothing is published here; the tar walk in after_credit
   routes the staged bytes once the buffer holds a whole frame (or is
   full).  Returns 1 if the current frag needs to be reprocessed. */

static inline int
stage_owned_frame( fd_snapdc_tile_t *  ctx,
                   fd_stem_context_t * stem,
                   uchar const *       data,
                   ulong               sz ) {
  FD_TEST( ctx->frame_idx%ctx->tile_count==ctx->tile_idx );
  FD_TEST( ctx->dirty || ctx->in.frag_pos<sz );
  FD_TEST( ctx->buf_len<FD_SNAPDC_FRAMEBUF_SZ );
  ctx->dirty = 1;

  ulong in_consumed  = 0UL;
  ulong out_produced = 0UL;
  ulong frame_res = ZSTD_decompressStream_simpleArgs(
      ctx->zstd,
      ctx->framebuf+ctx->buf_len,
      FD_SNAPDC_FRAMEBUF_SZ-ctx->buf_len,
      &out_produced,
      data+ctx->in.frag_pos,
      sz-ctx->in.frag_pos,
      &in_consumed );
  if( FD_UNLIKELY( ZSTD_isError( frame_res ) ) ) {
    FD_LOG_WARNING(( "error while decompressing %s snapshot (%u-%s)",
                     ctx->full ? "full" : "incremental",
                     ZSTD_getErrorCode( frame_res ), ZSTD_getErrorName( frame_res ) ));
    transition_malformed( ctx, stem );
    return 0;
  }

  ctx->in.frag_pos += in_consumed;
  ctx->buf_len     += out_produced;
  FD_TEST( ctx->in.frag_pos<=sz && ctx->buf_len<=FD_SNAPDC_FRAMEBUF_SZ );

  if( FD_LIKELY( ctx->full ) ) {
    ctx->metrics.full.compressed_bytes_read      += in_consumed;
    ctx->metrics.full.decompressed_bytes_written += out_produced;
  } else {
    ctx->metrics.incremental.compressed_bytes_read      += in_consumed;
    ctx->metrics.incremental.decompressed_bytes_written += out_produced;
  }

  if( FD_UNLIKELY( frame_res && !in_consumed && !out_produced ) ) {
    if( FD_UNLIKELY( ctx->in.frag_pos<sz ) ) {
      transition_malformed( ctx, stem );
    } else {
      ctx->in.frag_pos = 0UL;
    }
    return 0;
  }

  if( FD_UNLIKELY( !frame_res ) ) {
    /* Whole frame staged.  frame_idx / dirty / the zstd frame scanner
       are advanced only once the walk has drained the buffer and handed
       the walk state on, because that hand-off is what identifies the
       frame. */
    ctx->buf_frame_end = 1;
    ctx->emit_pending  = 1;
    if( FD_LIKELY( ctx->in.frag_pos>=sz ) ) { ctx->in.frag_pos = 0UL; return 0; }
    return 1;
  }

  if( FD_UNLIKELY( ctx->buf_len==FD_SNAPDC_FRAMEBUF_SZ ) ) {
    /* Frame is larger than the buffer: route what we have and keep
       decompressing the rest of it afterwards. */
    ctx->emit_pending = 1;
    if( FD_LIKELY( ctx->in.frag_pos>=sz ) ) { ctx->in.frag_pos = 0UL; return 0; }
    return 1;
  }

  if( FD_LIKELY( ctx->in.frag_pos>=sz ) ) { ctx->in.frag_pos = 0UL; return 0; }
  return 1;
}

/* Routing mode, uncompressed snapshot: tile 0 stages the raw stream in
   the same buffer and routes it with the same walk.  The stream is one
   never-ending "frame", so no EOM marker is ever emitted and every
   snapin tile keeps admitting lane 0 -- exactly what the broadcast
   passthrough does today. */

static inline int
stage_plain( fd_snapdc_tile_t * ctx,
             uchar const *      data,
             ulong              sz ) {
  FD_TEST( ctx->in.frag_pos<sz && ctx->buf_len<FD_SNAPDC_FRAMEBUF_SZ );
  ulong cpy = fd_ulong_min( sz-ctx->in.frag_pos, FD_SNAPDC_FRAMEBUF_SZ-ctx->buf_len );
  fd_memcpy( ctx->framebuf+ctx->buf_len, data+ctx->in.frag_pos, cpy );
  ctx->buf_len     += cpy;
  ctx->in.frag_pos += cpy;

  if( FD_LIKELY( ctx->full ) ) {
    ctx->metrics.full.compressed_bytes_read      += cpy;
    ctx->metrics.full.decompressed_bytes_written += cpy;
  } else {
    ctx->metrics.incremental.compressed_bytes_read      += cpy;
    ctx->metrics.incremental.decompressed_bytes_written += cpy;
  }

  if( FD_UNLIKELY( ctx->buf_len==FD_SNAPDC_FRAMEBUF_SZ ) ) ctx->emit_pending = 1;
  if( FD_LIKELY( ctx->in.frag_pos>=sz ) ) { ctx->in.frag_pos = 0UL; return 0; }
  return 1;
}

static inline int
handle_data_frag( fd_snapdc_tile_t *  ctx,
                  fd_stem_context_t * stem,
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

  FD_TEST( chunk>=ctx->in.chunk0 && chunk<=ctx->in.wmark && sz<=ctx->in.mtu && sz>=ctx->in.frag_pos );
  uchar const * data = fd_chunk_to_laddr_const( ctx->in.mem, chunk );

  if( FD_UNLIKELY( ctx->route ) ) {
    /* Hold the frag while the staged bytes are still being routed: the
       buffer is single and the walk hand-off identifies the frame in
       it.  Progress is guaranteed -- after_credit still runs on every
       held iteration, and the walk chain terminates at frame 0. */
    if( FD_UNLIKELY( ctx->emit_pending ) ) return 1;
    if( FD_UNLIKELY( !ctx->is_zstd ) ) {
      if( FD_UNLIKELY( ctx->tile_idx!=0UL ) ) return 0;
      return stage_plain( ctx, data, sz );
    }
    if( ctx->frame_idx%ctx->tile_count!=ctx->tile_idx ) return skip_unowned_frame( ctx, stem, data, sz );
    return stage_owned_frame( ctx, stem, data, sz );
  }

  if( FD_UNLIKELY( !ctx->is_zstd ) ) {
    if( FD_UNLIKELY( ctx->tile_idx!=0UL ) ) return 0;
    FD_TEST( ctx->in.frag_pos<sz );
    uchar const * in  = data+ctx->in.frag_pos;
    uchar *       out = fd_chunk_to_laddr( ctx->out[ 0 ].mem, ctx->out[ 0 ].chunk );
    ulong cpy = fd_ulong_min( sz-ctx->in.frag_pos, ctx->out[ 0 ].mtu );
    fd_memcpy( out, in, cpy );
    fd_stem_publish( stem, 0UL, FD_SNAPSHOT_MSG_DATA, ctx->out[ 0 ].chunk, cpy, 0UL, 0UL, 0UL );
    ctx->out[ 0 ].chunk = fd_dcache_compact_next( ctx->out[ 0 ].chunk, cpy, ctx->out[ 0 ].chunk0, ctx->out[ 0 ].wmark );

    if( FD_LIKELY( ctx->full ) ) {
      ctx->metrics.full.compressed_bytes_read      += cpy;
      ctx->metrics.full.decompressed_bytes_written += cpy;
    } else {
      ctx->metrics.incremental.compressed_bytes_read      += cpy;
      ctx->metrics.incremental.decompressed_bytes_written += cpy;
    }

    ctx->in.frag_pos += cpy;
    FD_TEST( ctx->in.frag_pos<=sz );
    if( FD_UNLIKELY( ctx->in.frag_pos<sz ) ) return 1;
    ctx->in.frag_pos = 0UL;
    return 0;
  }

  if( ctx->frame_idx%ctx->tile_count!=ctx->tile_idx ) {
    return skip_unowned_frame( ctx, stem, data, sz );
  }

  return process_owned_frame( ctx, stem, data, sz );
}

static inline int
handle_carry_frag( fd_snapdc_tile_t *  ctx,
                   fd_stem_context_t * stem,
                   ulong               chunk,
                   ulong               sz ) {
  FD_TEST( ctx->route && sz==sizeof(fd_ssctrl_route_carry_t) );
  fd_ssctrl_route_carry_t const * carry = fd_chunk_to_laddr_const( ctx->cr_in.mem, chunk );

  /* A retried attempt leaves the previous attempt's hand-offs in the
     link, and the upstream tile can also reach the next attempt's INIT
     before we do, so the generation decides whether to drop or wait. */
  if( FD_UNLIKELY( carry->generation< ctx->generation ) ) return 0;
  if( FD_UNLIKELY( carry->generation>ctx->generation ) ) return 1;

  if( FD_UNLIKELY( carry->flags & FD_SSCTRL_CARRY_ABORT ) ) {
    transition_malformed( ctx, stem );
    return 0;
  }
  if( FD_UNLIKELY( ctx->state!=FD_SNAPSHOT_STATE_PROCESSING ) ) return 0; /* attempt already over */

  /* One hand-off per frame we own, in order.  It can arrive well before
     we get there -- our previous frame's hand-off went out D frames
     earlier in the stream -- so hold it until we have skipped forward
     to the frame it describes. */
  if( FD_UNLIKELY( ctx->have_carry ) )                  return 1;
  if( FD_UNLIKELY( carry->frame_idx>ctx->frame_idx ) )  return 1;

  if( FD_UNLIKELY( carry->frame_idx!=ctx->frame_idx ) ) {
    FD_LOG_WARNING(( "snapdc %lu received walk hand-off for frame %lu, expected %lu",
                     ctx->tile_idx, carry->frame_idx, ctx->frame_idx ));
    transition_malformed( ctx, stem );
    return 0;
  }

  ctx->run_left   = carry->run_left;
  ctx->run_target = carry->run_target;
  ctx->av_ord     = carry->av_ord;
  ctx->saw_eof    = !!(carry->flags & FD_SSCTRL_CARRY_EOF);
  ctx->have_carry = 1;
  return 0;
}

static inline int
returnable_frag( fd_snapdc_tile_t *  ctx,
                 ulong               in_idx,
                 ulong               seq    FD_PARAM_UNUSED,
                 ulong               sig,
                 ulong               chunk,
                 ulong               sz,
                 ulong               ctl    FD_PARAM_UNUSED,
                 ulong               tsorig FD_PARAM_UNUSED,
                 ulong               tspub  FD_PARAM_UNUSED,
                 fd_stem_context_t * stem ) {
  FD_TEST( ctx->state!=FD_SNAPSHOT_STATE_SHUTDOWN );

  if( FD_UNLIKELY( in_idx!=0UL ) ) {
    FD_TEST( sig==FD_SNAPSHOT_MSG_ROUTE_CARRY );
    return handle_carry_frag( ctx, stem, chunk, sz );
  }

  if( FD_LIKELY( sig==FD_SNAPSHOT_MSG_DATA ) ) return handle_data_frag( ctx, stem, chunk, sz );

  /* Controls are barriers for every snapin tile, so anything still
     staged has to be routed before the control overtakes it.  ERROR and
     FAIL are the exception: they cancel the staged bytes instead.  (The
     uncompressed passthrough has no frame boundary to flush on, so its
     tail is flushed here.) */
  if( FD_UNLIKELY( ctx->route && sig!=FD_SNAPSHOT_MSG_CTRL_ERROR && sig!=FD_SNAPSHOT_MSG_CTRL_FAIL ) ) {
    if( FD_UNLIKELY( ctx->emit_pos<ctx->buf_len ) ) { ctx->emit_pending = 1; return 1; }
    if( FD_UNLIKELY( ctx->emit_pending ) ) return 1;
  }

  handle_control_frag( ctx, stem, sig, chunk, sz );
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
  void * _framebuf       = FD_SCRATCH_ALLOC_APPEND( l, 4096UL,                    FD_SNAPDC_FRAMEBUF_SZ                     );

  ctx->state      = FD_SNAPSHOT_STATE_IDLE;
  ctx->tile_idx   = tile->kind_id;
  ctx->tile_count = fd_topo_tile_name_cnt( topo, NAME );
  FD_TEST( ctx->tile_count );
  FD_TEST( ctx->tile_idx<ctx->tile_count );

  ctx->zstd = ZSTD_initStaticDStream( _zstd, ZSTD_estimateDStreamSize( ZSTD_WINDOW_SZ ) );
  FD_TEST( ctx->zstd );
  FD_TEST( ctx->zstd==_zstd );

  ctx->framebuf    = _framebuf;
  ctx->dirty       = 0;
  ctx->frame_idx   = 0UL;
  ctx->in.frag_pos = 0UL;
  FD_TEST( fd_zstd_frame_new( ctx->zstd_frame ) );
  fd_memset( &ctx->metrics, 0, sizeof(ctx->metrics) );

  if( FD_UNLIKELY( !tile->out_cnt || tile->out_cnt>FD_SNAPDC_OUT_MAX ) ) {
    FD_LOG_ERR(( "tile `" NAME "` has %lu outs, expected 1..%lu", tile->out_cnt, FD_SNAPDC_OUT_MAX ));
  }

  fd_topo_link_t const * out0 = &topo->links[ tile->out_link_id[ 0UL ] ];
  ctx->route  = !strcmp( out0->name, "snapdc_rt" );
  ctx->cr_out = ULONG_MAX;
  if( FD_UNLIKELY( ctx->route ) ) {
    /* [ 0, target_cnt ) routed data links, then the walk hand-off. */
    ctx->target_cnt = tile->out_cnt - fd_ulong_if( ctx->tile_count>1UL, 1UL, 0UL );
    FD_TEST( ctx->target_cnt );
    if( FD_LIKELY( ctx->tile_count>1UL ) ) {
      ctx->cr_out = ctx->target_cnt;
      FD_TEST( 0==strcmp( topo->links[ tile->out_link_id[ ctx->cr_out ] ].name, "snapdc_cr" ) );
    }
    if( FD_UNLIKELY( tile->in_cnt!=1UL+fd_ulong_if( ctx->tile_count>1UL, 1UL, 0UL ) ) ) {
      FD_LOG_ERR(( "tile `" NAME "` has %lu ins, expected %lu", tile->in_cnt, 1UL+fd_ulong_if( ctx->tile_count>1UL, 1UL, 0UL ) ));
    }
  } else {
    FD_TEST( 0==strcmp( out0->name, "snapdc_in" ) );
    ctx->target_cnt = 1UL;
    if( FD_UNLIKELY( tile->in_cnt !=1UL ) ) FD_LOG_ERR(( "tile `" NAME "` has %lu ins, expected 1",  tile->in_cnt  ));
    if( FD_UNLIKELY( tile->out_cnt!=1UL ) ) FD_LOG_ERR(( "tile `" NAME "` has %lu outs, expected 1", tile->out_cnt ));
  }

  for( ulong o=0UL; o<tile->out_cnt; o++ ) {
    fd_topo_link_t const * link = &topo->links[ tile->out_link_id[ o ] ];
    ctx->out[ o ].mem    = topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ].wksp;
    ctx->out[ o ].chunk0 = fd_dcache_compact_chunk0( ctx->out[ o ].mem, link->dcache );
    ctx->out[ o ].wmark  = fd_dcache_compact_wmark ( ctx->out[ o ].mem, link->dcache, link->mtu );
    ctx->out[ o ].chunk  = ctx->out[ o ].chunk0;
    ctx->out[ o ].mtu    = link->mtu;
  }

  fd_topo_link_t const * in_link = &topo->links[ tile->in_link_id[ 0UL ] ];
  fd_topo_wksp_t const * in_wksp = &topo->workspaces[ topo->objs[ in_link->dcache_obj_id ].wksp_id ];
  ctx->in.mem                    = in_wksp->wksp;
  ctx->in.chunk0                 = fd_dcache_compact_chunk0( ctx->in.mem, in_link->dcache );
  ctx->in.wmark                  = fd_dcache_compact_wmark( ctx->in.mem, in_link->dcache, in_link->mtu );
  ctx->in.mtu                    = in_link->mtu;

  if( FD_UNLIKELY( ctx->route ) ) {
    if( FD_LIKELY( ctx->tile_count>1UL ) ) {
      fd_topo_link_t const * cr_link = &topo->links[ tile->in_link_id[ 1UL ] ];
      FD_TEST( 0==strcmp( cr_link->name, "snapdc_cr" ) );
      FD_TEST( cr_link->mtu>=sizeof(fd_ssctrl_route_carry_t) );
      ctx->cr_in.mem = topo->workspaces[ topo->objs[ cr_link->dcache_obj_id ].wksp_id ].wksp;
    }
    route_reset_attempt( ctx );
  }

  ulong scratch_top = FD_SCRATCH_ALLOC_FINI( l, scratch_align() );
  if( FD_UNLIKELY( scratch_top > (ulong)scratch + scratch_footprint( tile ) ) )
    FD_LOG_ERR(( "scratch overflow %lu %lu %lu",
                 scratch_top - (ulong)scratch - scratch_footprint( tile ),
                 scratch_top,
                 (ulong)scratch + scratch_footprint( tile ) ));
}

/* One control frag has to be forwarded to every routed target, plus the
   walk hand-off, in a single callback -- and after_credit publishes up
   to FD_SNAPDC_EMIT_BATCH routed frags per call so the stem loop's
   per-iteration cost is amortized over a run of them instead of paid
   once per 64 KiB. */
#define STEM_BURST (FD_SNAPDC_OUT_MAX+FD_SNAPDC_EMIT_BATCH)

#define STEM_LAZY  (128L*3000L)

#define STEM_CALLBACK_CONTEXT_TYPE  fd_snapdc_tile_t
#define STEM_CALLBACK_CONTEXT_ALIGN alignof(fd_snapdc_tile_t)

#define STEM_CALLBACK_SHOULD_SHUTDOWN should_shutdown
#define STEM_CALLBACK_METRICS_WRITE   metrics_write
#define STEM_CALLBACK_AFTER_CREDIT    after_credit
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
