#define _GNU_SOURCE
#include "../../disco/stem/fd_stem.h"
#include "../../flamenco/accdb/fd_accdb_base.h"
#include "utils/fd_ssparse.h"

#include <sys/mman.h>

static ulong test_pub_sig[ 64UL ];
static ulong test_pub_out_idx[ 64UL ];
static ulong test_pub_cnt;
static ulong test_accdb_reset_cnt;
static ulong test_accdb_attach_cnt;
static ulong test_accdb_purge_cnt;
static ulong test_accdb_revert_cnt;
static ulong test_accdb_worker_call_cnt;
static ulong test_accdb_worker_slot;
static ulong test_accdb_worker_fork;
static ulong test_accdb_worker_cnt_arg;
static uchar test_accdb_worker_pubkeys[ 8 ][ 32 ];
static ulong test_accdb_worker_lamports[ 8 ];
static ulong test_accdb_worker_data_lens[ 8 ];
static int   test_accdb_worker_execs[ 8 ];
static ulong test_accdb_worker_next_off;   /* mock allocation cursor */
static ulong test_accdb_worker_skip_mask;  /* entries reported ignored (no allocation) */
static ulong test_accdb_worker_repl_mask;  /* entries reported replaced */
static ulong test_accdb_worker_close_cnt;
static ulong test_accdb_writer_begin_cnt;
static ulong test_accdb_writer_end_cnt;
static int   test_parser_script;
static ulong test_parser_call_cnt;

static int
test_ssparse_advance( fd_ssparse_t *                 parser,
                      uchar const *                  data,
                      ulong                          data_sz,
                      fd_ssparse_advance_result_t *  result );

static void mock_accdb_snapshot_writer_begin( fd_accdb_t * accdb );
static void mock_accdb_snapshot_writer_end  ( fd_accdb_t * accdb );

static ulong
test_stem_publish( fd_stem_context_t * stem,
                   ulong               out_idx,
                   ulong               sig,
                   ulong               chunk,
                   ulong               sz,
                   ulong               ctl,
                   ulong               tsorig,
                   ulong               tspub ) {
  (void)stem;
  (void)chunk;
  (void)sz;
  (void)ctl;
  (void)tsorig;
  (void)tspub;
  FD_TEST( test_pub_cnt<sizeof(test_pub_sig)/sizeof(test_pub_sig[0]) );
  test_pub_out_idx[ test_pub_cnt ] = out_idx;
  test_pub_sig    [ test_pub_cnt ] = sig;
  test_pub_cnt++;
  return test_pub_cnt-1UL;
}

#define FD_TILE_TEST 1
#define fd_accdb_snapshot_prefetch_batch mock_accdb_snapshot_prefetch_batch
#define fd_accdb_snapshot_writer_begin mock_accdb_snapshot_writer_begin
#define fd_accdb_snapshot_writer_end mock_accdb_snapshot_writer_end
#define fd_accdb_snapshot_write_batch_par_worker mock_accdb_snapshot_write_batch_par_worker
#define fd_accdb_snapshot_flush_par_metrics mock_accdb_snapshot_flush_par_metrics
#define fd_accdb_snapshot_worker_close mock_accdb_snapshot_worker_close
#define fd_accdb_snapshot_verify_readback mock_accdb_snapshot_verify_readback
#define fd_accdb_snapshot_load_begin_with_writers mock_accdb_snapshot_load_begin_with_writers
#define fd_accdb_snapshot_write_batch mock_accdb_snapshot_write_batch
#define fd_accdb_snapshot_write_one   mock_accdb_snapshot_write_one
#define fd_accdb_reset                mock_accdb_reset
#define fd_accdb_attach_child         mock_accdb_attach_child
#define fd_accdb_purge                mock_accdb_purge
#define fd_accdb_snapshot_revert_whead mock_accdb_snapshot_revert_whead
#define fd_txncache_reset             mock_txncache_reset
#define fd_ssmanifest_parser_init     mock_ssmanifest_parser_init
#define fd_slot_delta_parser_init     mock_slot_delta_parser_init
#define fd_stem_publish               test_stem_publish
#define fd_ssparse_advance            test_ssparse_advance
#include "fd_snapin_tile.c"
#undef fd_ssparse_advance
#undef fd_stem_publish
#undef fd_slot_delta_parser_init
#undef fd_ssmanifest_parser_init
#undef fd_txncache_reset
#undef fd_accdb_snapshot_revert_whead
#undef fd_accdb_purge
#undef fd_accdb_attach_child
#undef fd_accdb_reset
#undef fd_accdb_snapshot_write_batch_par_worker
#undef fd_accdb_snapshot_flush_par_metrics
#undef fd_accdb_snapshot_worker_close
#undef fd_accdb_snapshot_verify_readback
#undef fd_accdb_snapshot_writer_end
#undef fd_accdb_snapshot_writer_begin
#undef fd_accdb_snapshot_prefetch_batch

#include <stdlib.h>

static void
mock_accdb_snapshot_writer_begin( fd_accdb_t * accdb ) {
  (void)accdb;
  test_accdb_writer_begin_cnt++;
}

static void
mock_accdb_snapshot_writer_end( fd_accdb_t * accdb ) {
  (void)accdb;
  test_accdb_writer_end_cnt++;
}

void
mock_accdb_snapshot_prefetch_batch( fd_accdb_t *        accdb,
                                    ulong               cnt,
                                    uchar const * const pubkeys[] ) {
  (void)accdb;
  (void)cnt;
  (void)pubkeys;
}

/* Mock striped-lock worker writer: records what the worker parsed
   from the appendvec bytes, allocates sequentially from a cursor for
   accepted entries, and reports skip_mask entries as ignored (no
   allocation) and repl_mask entries as replaced. */
int
mock_accdb_snapshot_write_batch_par_worker( fd_accdb_t *                      accdb,
                                            fd_accdb_fork_id_t                fork_id,
                                            ulong                             cnt,
                                            uchar const * const               pubkeys[],
                                            ulong                             slot,
                                            ulong const                       lamports[],
                                            ulong const                       data_lens[],
                                            int const                         executables[],
                                            fd_accdb_snapshot_whead_t *       whead,
                                            uint *                            stripe_locks,
                                            ulong                             stripe_msk,
                                            fd_accdb_snapshot_par_metrics_t * par_metrics,
                                            ulong                             file_offsets[],
                                            ulong *                           accounts_ignored,
                                            ulong *                           accounts_replaced,
                                            ulong *                           accounts_loaded,
                                            ulong *                           out_replaced_lamports,
                                            ulong *                           out_ignored_lamports ) {
  (void)accdb;
  (void)whead;
  (void)stripe_locks;
  (void)stripe_msk;
  (void)par_metrics;
  FD_TEST( cnt && cnt<=8UL );
  test_accdb_worker_call_cnt++;
  test_accdb_worker_fork    = (ulong)fork_id.val;
  test_accdb_worker_slot    = slot;
  test_accdb_worker_cnt_arg = cnt;
  *accounts_ignored = *accounts_replaced = *accounts_loaded = 0UL;
  *out_replaced_lamports = *out_ignored_lamports = 0UL;
  for( ulong i=0UL; i<cnt; i++ ) {
    fd_memcpy( test_accdb_worker_pubkeys[ i ], pubkeys[ i ], 32UL );
    test_accdb_worker_lamports [ i ] = lamports[ i ];
    test_accdb_worker_data_lens[ i ] = data_lens[ i ];
    test_accdb_worker_execs    [ i ] = executables[ i ];
    if( test_accdb_worker_skip_mask & (1UL<<i) ) {
      file_offsets[ i ] = ULONG_MAX;
      (*accounts_ignored)++;
      *out_ignored_lamports += lamports[ i ];
    } else {
      file_offsets[ i ] = test_accdb_worker_next_off;
      test_accdb_worker_next_off += sizeof(fd_accdb_disk_meta_t)+data_lens[ i ];
      if( test_accdb_worker_repl_mask & (1UL<<i) ) { (*accounts_replaced)++; *out_replaced_lamports += lamports[ i ]; }
      else                                           (*accounts_loaded)++;
    }
  }
  /* masks apply to a single call */
  test_accdb_worker_skip_mask = 0UL;
  test_accdb_worker_repl_mask = 0UL;
  return 0;
}

void
mock_accdb_snapshot_flush_par_metrics( fd_accdb_t *                      accdb,
                                       fd_accdb_snapshot_par_metrics_t * m ) {
  (void)accdb;
  m->disk_used_added      = 0UL;
  m->disk_used_removed    = 0UL;
  m->accounts_total_added = 0UL;
}

void
mock_accdb_snapshot_worker_close( fd_accdb_t *                accdb,
                                  fd_accdb_snapshot_whead_t * whead ) {
  (void)accdb;
  whead->val           = 0UL;
  whead->has_partition = 0;
  test_accdb_worker_close_cnt++;
}

void
mock_accdb_snapshot_load_begin_with_writers( fd_accdb_t * accdb,
                                             ulong        writer_cnt ) {
  (void)accdb;
  (void)writer_cnt;
}

void
mock_accdb_snapshot_verify_readback( fd_accdb_t * accdb,
                                     ulong        sample_max ) {
  (void)accdb;
  (void)sample_max;
}

int
mock_accdb_snapshot_write_batch( fd_accdb_t *        accdb,
                                 fd_accdb_fork_id_t  fork_id,
                                 ulong               cnt,
                                 uchar const * const pubkeys[],
                                 ulong               slot,
                                 ulong  const        lamports[],
                                 ulong  const        data_lens[],
                                 int    const        executables[],
                                 ulong *             accounts_ignored,
                                 ulong *             accounts_replaced,
                                 ulong *             accounts_loaded,
                                 ulong *             out_replaced_lamports,
                                 ulong *             out_ignored_lamports ) {
  (void)accdb;
  (void)fork_id;
  (void)pubkeys;
  (void)slot;
  (void)lamports;
  (void)data_lens;
  (void)executables;
  *accounts_ignored       = 0UL;
  *accounts_replaced      = 0UL;
  *accounts_loaded        = cnt;
  *out_replaced_lamports  = 0UL;
  *out_ignored_lamports   = 0UL;
  return 0;
}

int
mock_accdb_snapshot_write_one( fd_accdb_t *       accdb,
                               fd_accdb_fork_id_t fork_id,
                               uchar const *      pubkey,
                               ulong              slot,
                               ulong              lamports,
                               ulong              data_len,
                               int                executable,
                               ulong *            out_replaced_lamports ) {
  (void)accdb;
  (void)fork_id;
  (void)pubkey;
  (void)slot;
  (void)lamports;
  (void)data_len;
  (void)executable;
  *out_replaced_lamports = 0UL;
  return 1;
}

void
mock_accdb_reset( fd_accdb_t * accdb ) {
  (void)accdb;
  test_accdb_reset_cnt++;
}

fd_accdb_fork_id_t
mock_accdb_attach_child( fd_accdb_t *       accdb,
                         fd_accdb_fork_id_t parent_fork_id ) {
  (void)accdb;
  (void)parent_fork_id;
  test_accdb_attach_cnt++;
  return (fd_accdb_fork_id_t){ .val = 7U };
}

void
mock_accdb_purge( fd_accdb_t *       accdb,
                  fd_accdb_fork_id_t fork_id ) {
  (void)accdb;
  (void)fork_id;
  test_accdb_purge_cnt++;
}

void
mock_accdb_snapshot_revert_whead( fd_accdb_t *                         accdb,
                                  fd_accdb_snapshot_recovery_t const * recover ) {
  (void)accdb;
  (void)recover;
  test_accdb_revert_cnt++;
}

void
mock_txncache_reset( fd_txncache_t * txncache ) {
  (void)txncache;
}

void
mock_ssmanifest_parser_init( fd_ssmanifest_parser_t * parser,
                             fd_snapshot_manifest_t * manifest ) {
  (void)parser;
  (void)manifest;
}

void
mock_slot_delta_parser_init( fd_slot_delta_parser_t * parser ) {
  (void)parser;
}

/* Static account entries + fragmented account body for the worker batch
   staging script (5). */
static uchar test_script5_ent[ 3 ][ 160UL ];
static uchar test_script5_hdr_pk   [ 32UL ];
static uchar test_script5_hdr_owner[ 32UL ];
static uchar test_script5_hdr_data [ 8UL ];

static int
test_ssparse_advance( fd_ssparse_t *                parser,
                      uchar const *                 data,
                      ulong                         data_sz,
                      fd_ssparse_advance_result_t * result ) {
  (void)parser;
  FD_TEST( test_parser_script>=1 && test_parser_script<=6 );
  if( test_parser_script==6 ) {
    /* coordinator passthrough: appendvec header, region header, DONE */
    fd_memset( result, 0, sizeof(*result) );
    result->bytes_consumed = data_sz;
    test_parser_call_cnt++;
    if( test_parser_call_cnt==1UL ) {
      result->appendvec.slot    = 42UL;
      result->appendvec.data_sz = 1000UL;
      return FD_SSPARSE_ADVANCE_APPENDVEC;
    }
    if( test_parser_call_cnt==2UL ) {
      result->region.data_sz = 600UL;
      return FD_SSPARSE_ADVANCE_REGION;
    }
    return FD_SSPARSE_ADVANCE_DONE;
  }
  if( test_parser_script==5 ) {
    /* worker appendvec body: one 3-account batch, then a fragmented
       account (header + one data run), then garbage to the end */
    fd_memset( result, 0, sizeof(*result) );
    test_parser_call_cnt++;
    if( test_parser_call_cnt==1UL ) {
      result->bytes_consumed = 40UL;
      result->account_batch.batch[ 0 ] = test_script5_ent[ 0 ];
      result->account_batch.batch[ 1 ] = test_script5_ent[ 1 ];
      result->account_batch.batch[ 2 ] = test_script5_ent[ 2 ];
      result->account_batch.batch_cnt  = 3UL;
      result->account_batch.slot       = 440123518UL;
      return FD_SSPARSE_ADVANCE_ACCOUNT_BATCH;
    }
    if( test_parser_call_cnt==2UL ) {
      result->bytes_consumed = 20UL;
      result->account_header.pubkey     = test_script5_hdr_pk;
      result->account_header.slot       = 440123518UL;
      result->account_header.lamports   = 40UL;
      result->account_header.data_len   = 8UL;
      result->account_header.owner      = test_script5_hdr_owner;
      result->account_header.executable = 0;
      return FD_SSPARSE_ADVANCE_ACCOUNT_HEADER;
    }
    if( test_parser_call_cnt==3UL ) {
      result->bytes_consumed        = 8UL;
      result->account_data.data     = test_script5_hdr_data;
      result->account_data.data_sz  = 8UL;
      return FD_SSPARSE_ADVANCE_ACCOUNT_DATA;
    }
    result->bytes_consumed = data_sz;
    return FD_SSPARSE_ADVANCE_AGAIN;
  }
  if( test_parser_script==4 ) {
    /* consume everything, no events (worker appendvec body) */
    fd_memset( result, 0, sizeof(*result) );
    result->bytes_consumed = data_sz;
    test_parser_call_cnt++;
    return FD_SSPARSE_ADVANCE_AGAIN;
  }
  if( test_parser_script==3 ) {
    FD_TEST( data_sz==1UL );
    fd_memset( result, 0, sizeof(*result) );
    result->bytes_consumed = 1UL;
    test_parser_call_cnt++;
    return test_parser_call_cnt==2UL ? FD_SSPARSE_ADVANCE_DONE : FD_SSPARSE_ADVANCE_AGAIN;
  }
  if( test_parser_script==2 ) {
    FD_TEST( data_sz==1UL );
    fd_memset( result, 0, sizeof(*result) );
    result->bytes_consumed = 1UL;
    test_parser_call_cnt++;
    return FD_SSPARSE_ADVANCE_DONE;
  }

  FD_TEST( data_sz==(test_parser_call_cnt ? 2UL : 4UL) );
  fd_memset( result, 0, sizeof(*result) );
  result->bytes_consumed = 2UL;
  if( !test_parser_call_cnt++ ) {
    result->account_data.data    = data;
    result->account_data.data_sz = 2UL;
    return FD_SSPARSE_ADVANCE_ACCOUNT_DATA;
  }
  return FD_SSPARSE_ADVANCE_AGAIN;
}

static void
sync_ctx_init( fd_snapin_tile_t * ctx,
               ulong              lane_cnt,
               int                state ) {
  fd_memset( ctx, 0, sizeof(*ctx) );
  ctx->state           = state;
  ctx->full            = 1;
  ctx->lane_cnt        = lane_cnt;
  ctx->pending_control = ULONG_MAX;
  ctx->ct_out.idx      = 0UL;
}

static void
send_control( fd_snapin_tile_t * ctx,
              ulong              lane,
              ulong              sig ) {
  FD_TEST( !returnable_frag( ctx, lane, 0UL, sig, 0UL, 0UL, 0UL, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
}

static void
test_control_barriers( void ) {
  ulong const lane_cnts[] = { 1UL, 2UL, 4UL };
  for( ulong n_idx=0UL; n_idx<sizeof(lane_cnts)/sizeof(lane_cnts[0]); n_idx++ ) {
    ulong lane_cnt = lane_cnts[ n_idx ];
    fd_snapin_tile_t ctx[1];
    sync_ctx_init( ctx, lane_cnt, FD_SNAPSHOT_STATE_FINISHING );
    test_pub_cnt = 0UL;

    for( ulong lane=lane_cnt; lane; lane-- ) {
      send_control( ctx, lane-1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
      FD_TEST( test_pub_cnt==(lane==1UL) );
    }
    FD_TEST( test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_FINI );
    FD_TEST( ctx->pending_control==ULONG_MAX );
    for( ulong lane=0UL; lane<lane_cnt; lane++ ) FD_TEST( !ctx->control_seen[ lane ] );
  }
}

static void
test_all_control_barriers_and_final_payload( void ) {
  ulong const controls[] = {
    FD_SNAPSHOT_MSG_META,
    FD_SNAPSHOT_MSG_CTRL_INIT_FULL,
    FD_SNAPSHOT_MSG_CTRL_INIT_INCR,
    FD_SNAPSHOT_MSG_CTRL_FAIL,
    FD_SNAPSHOT_MSG_CTRL_NEXT,
    FD_SNAPSHOT_MSG_CTRL_DONE,
    FD_SNAPSHOT_MSG_CTRL_SHUTDOWN,
    FD_SNAPSHOT_MSG_CTRL_FINI,
  };
  for( ulong i=0UL; i<sizeof(controls)/sizeof(controls[0]); i++ ) {
    fd_snapin_tile_t ctx[1];
    sync_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_IDLE );
    test_pub_cnt = 0UL;
    send_control( ctx, 0UL, controls[i] );
    FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
    FD_TEST( ctx->pending_control==controls[i] );
    FD_TEST( ctx->control_seen[0] );
    FD_TEST( !ctx->control_seen[1] );
    FD_TEST( !test_pub_cnt );
  }

  fd_snapin_tile_t ctx[1];
  sync_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_PROCESSING );
  fd_ssctrl_meta_t meta[2];
  uchar meta_mem[2][ sizeof(fd_ssctrl_meta_t) ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( meta, 0, sizeof(meta) );
  meta[0].resolved_slot = 11UL;
  meta[1].resolved_slot = 22UL;
  fd_memset( meta[0].resolved_hash, 0x11, FD_HASH_FOOTPRINT );
  fd_memset( meta[1].resolved_hash, 0x22, FD_HASH_FOOTPRINT );
  fd_memcpy( meta_mem[0], &meta[0], sizeof(fd_ssctrl_meta_t) );
  fd_memcpy( meta_mem[1], &meta[1], sizeof(fd_ssctrl_meta_t) );
  ctx->in[0].wksp = (fd_wksp_t *)meta_mem[0];
  ctx->in[1].wksp = (fd_wksp_t *)meta_mem[1];
  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_META, 0UL, sizeof(fd_ssctrl_meta_t),
                             0UL, 0UL, 0UL, (fd_stem_context_t *)1UL ) );
  FD_TEST( !ctx->advertised_slot );
  FD_TEST( !returnable_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_META, 0UL, sizeof(fd_ssctrl_meta_t),
                             0UL, 0UL, 0UL, (fd_stem_context_t *)1UL ) );
  FD_TEST( ctx->advertised_slot==22UL );
  FD_TEST( !memcmp( ctx->advertised_hash, meta[1].resolved_hash, FD_HASH_FOOTPRINT ) );
  FD_TEST( !test_pub_cnt );

  sync_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_PROCESSING );
  ctx->advertised_slot = 33UL;
  fd_memset( ctx->advertised_hash, 0x33, FD_HASH_FOOTPRINT );
  for( ulong i=0UL; i<2UL; i++ ) {
    meta[i].resolved_slot = ULONG_MAX;
    fd_memcpy( meta_mem[i], &meta[i], sizeof(fd_ssctrl_meta_t) );
    ctx->in[i].wksp = (fd_wksp_t *)meta_mem[i];
    FD_TEST( !returnable_frag( ctx, i, 0UL, FD_SNAPSHOT_MSG_META, 0UL, sizeof(fd_ssctrl_meta_t),
                               0UL, 0UL, 0UL, (fd_stem_context_t *)1UL ) );
  }
  FD_TEST( ctx->advertised_slot==33UL );
  uchar expected_hash[ FD_HASH_FOOTPRINT ];
  fd_memset( expected_hash, 0x33, sizeof(expected_hash) );
  FD_TEST( !memcmp( ctx->advertised_hash, expected_hash, sizeof(expected_hash) ) );
  FD_TEST( !test_pub_cnt );
}

static void
test_fast_lane_control_pipeline( void ) {
  fd_snapin_tile_t ctx[1];
  sync_ctx_init( ctx, 4UL, FD_SNAPSHOT_STATE_FINISHING );
  test_pub_cnt = 0UL;

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( before_frag( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_NEXT )<0 );
  send_control( ctx, 2UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  send_control( ctx, 3UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( !before_frag( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_NEXT ) );
}

static void
data_ctx_init( fd_snapin_tile_t * ctx,
               ulong              lane_cnt,
               uchar              lane_data[ FD_TOPO_MAX_TILE_IN_LINKS ][ 64UL ] );

static void
test_pending_control_allows_lagging_data( void ) {
  uchar lane_data[ FD_TOPO_MAX_TILE_IN_LINKS ][ 64UL ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_snapin_tile_t ctx[1];
  data_ctx_init( ctx, 2UL, lane_data );
  ctx->expected_frame = 1UL;
  lane_data[1][0]     = 0U;
  test_pub_cnt         = 0UL;
  test_parser_script   = 2;
  test_parser_call_cnt = 0UL;

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );
  FD_TEST( ctx->pending_control==FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->control_seen[0] );
  FD_TEST( !ctx->control_seen[1] );
  FD_TEST( before_frag( ctx, 0UL, 1UL,
                        FD_SNAPSHOT_MSG_DATA )<0 );

  ulong sig = FD_SNAPSHOT_MSG_DATA;
  ulong ctl = fd_frag_meta_ctl( 0UL, 0, 1, 0 );
  FD_TEST( !before_frag( ctx, 1UL, 0UL, sig ) );
  FD_TEST( !returnable_frag( ctx, 1UL, 0UL, sig, 0UL, 1UL, ctl, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
  FD_TEST( test_parser_call_cnt==1UL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
  FD_TEST( ctx->expected_frame==2UL );
  FD_TEST( ctx->pending_control==FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->control_seen[0] );
  FD_TEST( !ctx->control_seen[1] );

  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_FINI );
}

static void
test_pending_control_keeps_frame_order( void ) {
  uchar lane_data[ FD_TOPO_MAX_TILE_IN_LINKS ][ 64UL ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_snapin_tile_t ctx[1];
  data_ctx_init( ctx, 3UL, lane_data );
  ctx->expected_frame = 1UL;
  lane_data[1][0]     = 0U;
  lane_data[2][0]     = 0U;
  test_pub_cnt         = 0UL;
  test_parser_script   = 3;
  test_parser_call_cnt = 0UL;

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  ulong sig1 = FD_SNAPSHOT_MSG_DATA;
  ulong sig2 = FD_SNAPSHOT_MSG_DATA;
  ulong ctl  = fd_frag_meta_ctl( 0UL, 0, 1, 0 );
  FD_TEST( !before_frag( ctx, 1UL, 0UL, sig1 ) );
  FD_TEST( before_frag( ctx, 2UL, 0UL, sig2 )<0 );

  FD_TEST( !returnable_frag( ctx, 1UL, 0UL, sig1, 0UL, 1UL, ctl, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );
  FD_TEST( ctx->expected_frame==2UL );
  FD_TEST( !before_frag( ctx, 2UL, 0UL, sig2 ) );
  FD_TEST( !returnable_frag( ctx, 2UL, 0UL, sig2, 0UL, 1UL, ctl, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
  FD_TEST( test_parser_call_cnt==2UL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
  FD_TEST( ctx->expected_frame==3UL );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  send_control( ctx, 2UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_FINI );

}

static void
test_error_interrupts_incremental_init( void ) {
  fd_snapin_tile_t ctx[1];
  uchar init_mem[ 2UL ][ FD_CHUNK_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( init_mem, 0, sizeof(init_mem) );
  sync_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_IDLE );
  ctx->in[0].wksp          = (fd_wksp_t *)init_mem[0];
  ctx->in[1].wksp          = (fd_wksp_t *)init_mem[1];
  ctx->accdb_root_fork_id  = (fd_accdb_fork_id_t){ .val = 3U };
  test_pub_cnt              = 0UL;
  test_accdb_reset_cnt      = 0UL;
  test_accdb_attach_cnt     = 0UL;
  test_accdb_purge_cnt      = 0UL;
  test_accdb_revert_cnt     = 0UL;

  FD_TEST( !before_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR ) );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( ctx->pending_control==FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( ctx->control_seen[0] );
  FD_TEST( !ctx->control_seen[1] );
  FD_TEST( ctx->full );
  FD_TEST( !ctx->init_completed );

  FD_TEST( !before_frag( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_ERROR ) );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( !ctx->init_completed );
  FD_TEST( ctx->pending_control==FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( ctx->control_seen[0] );
  FD_TEST( !ctx->control_seen[1] );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( before_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR )>0 );

  FD_TEST( !before_frag( ctx, 0UL, 2UL, FD_SNAPSHOT_MSG_CTRL_FAIL ) );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( !ctx->init_completed );
  FD_TEST( !test_accdb_reset_cnt );
  FD_TEST( !before_frag( ctx, 1UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL ) );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( !ctx->init_completed );
  FD_TEST( !test_accdb_reset_cnt );
  FD_TEST( !test_accdb_attach_cnt );
  FD_TEST( !test_accdb_purge_cnt );
  FD_TEST( !test_accdb_revert_cnt );
}

static void
test_partial_fail_survives_error( void ) {
  fd_snapin_tile_t ctx[1];
  sync_ctx_init( ctx, 4UL, FD_SNAPSHOT_STATE_PROCESSING );
  test_pub_cnt = 0UL;

  FD_TEST( !before_frag( ctx, 2UL, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL ) );
  send_control( ctx, 2UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->pending_control==FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->control_seen[2] );

  FD_TEST( !before_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR ) );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( ctx->pending_control==FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->control_seen[2] );

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  send_control( ctx, 3UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( ctx->pending_control==ULONG_MAX );
  FD_TEST( test_pub_cnt==2UL );
  FD_TEST( test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( test_pub_sig[1]==FD_SNAPSHOT_MSG_CTRL_FAIL );

  sync_ctx_init( ctx, 4UL, FD_SNAPSHOT_STATE_PROCESSING );
  test_pub_cnt = 0UL;
  send_control( ctx, 2UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  transition_malformed( ctx, (fd_stem_context_t *)1UL );
  FD_TEST( ctx->pending_control==FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->control_seen[2] );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  send_control( ctx, 3UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( ctx->pending_control==ULONG_MAX );
}

static void
test_fail_supersedes_pending_controls( void ) {
  struct {
    ulong sig;
    int   state;
  } const cases[] = {
    { FD_SNAPSHOT_MSG_META,           FD_SNAPSHOT_STATE_PROCESSING },
    { FD_SNAPSHOT_MSG_CTRL_INIT_FULL, FD_SNAPSHOT_STATE_IDLE       },
    { FD_SNAPSHOT_MSG_CTRL_INIT_INCR, FD_SNAPSHOT_STATE_IDLE       },
    { FD_SNAPSHOT_MSG_CTRL_FINI,      FD_SNAPSHOT_STATE_PROCESSING },
    { FD_SNAPSHOT_MSG_CTRL_NEXT,      FD_SNAPSHOT_STATE_FINISHING  },
    { FD_SNAPSHOT_MSG_CTRL_DONE,      FD_SNAPSHOT_STATE_FINISHING  },
  };

  for( ulong i=0UL; i<sizeof(cases)/sizeof(cases[0]); i++ ) {
    fd_snapin_tile_t ctx[1];
    sync_ctx_init( ctx, 4UL, cases[i].state );
    test_pub_cnt = 0UL;

    FD_TEST( !before_frag( ctx, 0UL, 0UL, cases[i].sig ) );
    send_control( ctx, 0UL, cases[i].sig );
    FD_TEST( ctx->pending_control==cases[i].sig );
    FD_TEST( ctx->control_seen[0] );

    FD_TEST( !before_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL ) );
    send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
    FD_TEST( ctx->pending_control==FD_SNAPSHOT_MSG_CTRL_FAIL );
    FD_TEST( !ctx->control_seen[0] );
    FD_TEST( ctx->control_seen[1] );

    send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
    send_control( ctx, 2UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
    send_control( ctx, 3UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
    FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
    FD_TEST( ctx->pending_control==ULONG_MAX );
    FD_TEST( test_pub_cnt==1UL );
    FD_TEST( test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_FAIL );
  }
}

static void
test_initialized_incremental_fail_rolls_back( void ) {
  fd_snapin_tile_t ctx[1];
  uchar init_mem[ 2UL ][ FD_CHUNK_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( init_mem, 0, sizeof(init_mem) );
  sync_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_IDLE );
  ctx->in[0].wksp          = (fd_wksp_t *)init_mem[0];
  ctx->in[1].wksp          = (fd_wksp_t *)init_mem[1];
  ctx->accdb_root_fork_id  = (fd_accdb_fork_id_t){ .val = 3U };
  test_pub_cnt              = 0UL;
  test_accdb_reset_cnt      = 0UL;
  test_accdb_attach_cnt     = 0UL;
  test_accdb_purge_cnt      = 0UL;
  test_accdb_revert_cnt     = 0UL;

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );
  FD_TEST( ctx->init_completed );
  FD_TEST( !ctx->full );
  FD_TEST( test_accdb_attach_cnt==1UL );
  FD_TEST( ctx->accdb_incr_fork_id.val==7U );

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( !ctx->init_completed );
  FD_TEST( !test_accdb_reset_cnt );
  FD_TEST( test_accdb_purge_cnt==1UL );
  FD_TEST( test_accdb_revert_cnt==1UL );
}

static void
test_error_fail_and_retry( void ) {
  fd_snapin_tile_t ctx[1];
  sync_ctx_init( ctx, 4UL, FD_SNAPSHOT_STATE_FINISHING );
  ctx->init_completed  = 1;
  test_pub_cnt         = 0UL;
  test_accdb_reset_cnt = 0UL;

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( before_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_DATA )>0 );
  FD_TEST( before_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI )>0 );

  send_control( ctx, 3UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( !test_accdb_reset_cnt );
  send_control( ctx, 2UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( test_accdb_reset_cnt==1UL );
  FD_TEST( test_pub_cnt==2UL );
  FD_TEST( test_pub_sig[1]==FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( !before_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL ) );
}

static void
data_ctx_init( fd_snapin_tile_t * ctx,
               ulong              lane_cnt,
               uchar              lane_data[ FD_TOPO_MAX_TILE_IN_LINKS ][ 64UL ] ) {
  sync_ctx_init( ctx, lane_cnt, FD_SNAPSHOT_STATE_PROCESSING );
  fd_ssparse_init( ctx->ssparse );
  for( ulong lane=0UL; lane<lane_cnt; lane++ ) {
    ctx->in[ lane ].wksp   = (fd_wksp_t *)lane_data[ lane ];
    ctx->in[ lane ].chunk0 = 0UL;
    ctx->in[ lane ].wmark  = 0UL;
    ctx->in[ lane ].mtu    = 64UL;
  }
}

static void
send_data( fd_snapin_tile_t * ctx,
           ulong              lane,
           ulong              sz,
           int                eom ) {
  ulong sig = FD_SNAPSHOT_MSG_DATA;
  ulong ctl = fd_frag_meta_ctl( 0UL, 0, eom, 0 );
  FD_TEST( !before_frag( ctx, lane, 0UL, sig ) );
  FD_TEST( !returnable_frag( ctx, lane, 0UL, sig, 0UL, sz, ctl, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
}

static void
test_frame_ordering( void ) {
  ulong const lane_cnts[] = { 1UL, 2UL, 4UL };
  uchar lane_data[ FD_TOPO_MAX_TILE_IN_LINKS ][ 64UL ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  for( ulong n_idx=0UL; n_idx<sizeof(lane_cnts)/sizeof(lane_cnts[0]); n_idx++ ) {
    ulong lane_cnt = lane_cnts[ n_idx ];
    fd_snapin_tile_t ctx[1];
    data_ctx_init( ctx, lane_cnt, lane_data );

    for( ulong frame=0UL; frame<2UL*lane_cnt; frame++ ) {
      if( lane_cnt>1UL && frame+1UL<2UL*lane_cnt ) {
        ulong future = frame+1UL;
        FD_TEST( before_frag( ctx, future%lane_cnt, 0UL, FD_SNAPSHOT_MSG_DATA )<0 );
      }
      send_data( ctx, frame%lane_cnt, 0UL, 1 );
      FD_TEST( ctx->expected_frame==frame+1UL );
    }
  }
}

static void
test_frame_owner_and_raw_lane( void ) {
  uchar lane_data[ FD_TOPO_MAX_TILE_IN_LINKS ][ 64UL ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_snapin_tile_t ctx[1];

  data_ctx_init( ctx, 4UL, lane_data );
  test_pub_cnt = 0UL;
  FD_TEST( before_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_DATA )<0 );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );
  FD_TEST( !test_pub_cnt );

  send_data( ctx, 0UL, 0UL, 0 );
  FD_TEST( !ctx->expected_frame );
}

static void
test_partial_and_zero_byte_eom( void ) {
  uchar lane_data[ FD_TOPO_MAX_TILE_IN_LINKS ][ 64UL ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_snapin_tile_t ctx[1];
  data_ctx_init( ctx, 2UL, lane_data );
  fd_memcpy( lane_data[0], "abcd", 4UL );
  lane_data[0][0] = 2U;
  uchar gui_data[ 64UL ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  ctx->gui_out.idx       = 0UL;
  ctx->gui_out.mem       = (fd_wksp_t *)gui_data;
  ctx->gui_out.chunk0    = 0UL;
  ctx->gui_out.wmark     = 0UL;
  ctx->gui_out.chunk     = 0UL;
  ctx->gui_config_acct_sz  = 2UL;
  ctx->gui_config_acct_off = 0UL;

  test_parser_script   = 1;
  test_parser_call_cnt = 0UL;
  ulong sig = FD_SNAPSHOT_MSG_DATA;
  ulong ctl = fd_frag_meta_ctl( 0UL, 0, 1, 0 );
  FD_TEST( returnable_frag( ctx, 0UL, 0UL, sig, 0UL, 4UL, ctl, 0UL, 0UL,
                            (fd_stem_context_t *)1UL ) );
  FD_TEST( !ctx->expected_frame );
  FD_TEST( ctx->in[0].pos==2UL );
  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, sig, 0UL, 4UL, ctl, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
  FD_TEST( test_parser_call_cnt==2UL );
  FD_TEST( ctx->expected_frame==1UL );

  ctx->state          = FD_SNAPSHOT_STATE_FINISHING;
  ctx->expected_frame = 1UL;
  send_data( ctx, 1UL, 0UL, 1 );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
  FD_TEST( ctx->expected_frame==2UL );
}

static void
test_malformed_stream_endings( void ) {
  fd_snapin_tile_t ctx[1];
  sync_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_PROCESSING );
  test_pub_cnt = 0UL;
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_ERROR );

  sync_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_FINISHING );
  test_pub_cnt = 0UL;
  ulong ctl = fd_frag_meta_ctl( 0UL, 0, 1, 0 );
  FD_TEST( !before_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_DATA ) );
  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_DATA,
                             0UL, 1UL, ctl, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_ERROR );
}

static void
test_init_resets_lane_state( void ) {
  fd_snapin_tile_t ctx[1];
  uchar init_mem[ 2UL ][ FD_CHUNK_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( init_mem, 0, sizeof(init_mem) );
  sync_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_IDLE );
  ctx->in[0].wksp      = (fd_wksp_t *)init_mem[0];
  ctx->in[1].wksp      = (fd_wksp_t *)init_mem[1];
  ctx->in[0].pos       = 5UL;
  ctx->in[1].pos       = 6UL;
  ctx->expected_frame  = 7UL;
  test_pub_cnt          = 0UL;

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( ctx->in[0].pos==5UL );
  FD_TEST( ctx->in[1].pos==6UL );
  FD_TEST( ctx->expected_frame==7UL );

  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( !ctx->in[0].pos );
  FD_TEST( !ctx->in[1].pos );
  FD_TEST( !ctx->expected_frame );
  FD_TEST( ctx->init_completed );
  FD_TEST( !ctx->full );
}

static void
test_nonempty_raw_data( void ) {
  uchar lane_data[ FD_TOPO_MAX_TILE_IN_LINKS ][ 64UL ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_snapin_tile_t ctx[1];
  data_ctx_init( ctx, 2UL, lane_data );
  lane_data[0][0]     = 0U;
  test_parser_script   = 2;
  test_parser_call_cnt = 0UL;

  FD_TEST( before_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_DATA )<0 );
  send_data( ctx, 0UL, 1UL, 0 );
  FD_TEST( test_parser_call_cnt==1UL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
  FD_TEST( !ctx->expected_frame );
}

static fd_banks_t *
new_banks( void ** mem_out ) {
  ulong footprint = fd_banks_footprint( 16UL, 4UL, 16UL, 64UL, 16UL );
  void * mem = aligned_alloc( fd_banks_align(), fd_ulong_align_up( footprint, fd_banks_align() ) );
  FD_TEST( mem );
  fd_banks_t * banks = fd_banks_join( fd_banks_new( mem, 16UL, 4UL, 16UL, 64UL, 16UL, 0, 42UL ) );
  FD_TEST( banks );
  *mem_out = mem;
  return banks;
}

static void
make_stake_state( fd_stake_state_t * state,
                  fd_pubkey_t const * vote_account ) {
  fd_memset( state, 0, sizeof(*state) );
  state->stake_type                                = FD_STAKE_STATE_STAKE;
  state->stake.stake.delegation.voter_pubkey       = *vote_account;
  state->stake.stake.delegation.stake              = 1234UL;
  state->stake.stake.delegation.activation_epoch   = 7UL;
  state->stake.stake.delegation.deactivation_epoch = ULONG_MAX;
  state->stake.stake.credits_observed               = 99UL;
}

static void
assert_stake_delegation( fd_stake_delegations_t const * stake_delegations,
                         fd_pubkey_t const *            stake_account,
                         fd_pubkey_t const *            vote_account ) {
  fd_stake_delegation_t const * delegation =
      fd_stake_delegation_root_query( stake_delegations, stake_account );
  FD_TEST( delegation );
  FD_TEST( fd_pubkey_eq( &delegation->vote_account, vote_account ) );
  FD_TEST( delegation->stake==1234UL );
  FD_TEST( delegation->activation_epoch==7UL );
  FD_TEST( delegation->deactivation_epoch==USHORT_MAX );
  FD_TEST( delegation->credits_observed==99UL );
  FD_TEST( delegation->lamports==5000UL );
  FD_TEST( delegation->acc_dlen==sizeof(fd_stake_state_t) );
}

static void
test_batch_stake_delegation( void ) {
  void * banks_mem;
  fd_banks_t * banks = new_banks( &banks_mem );
  fd_stake_delegations_t * stake_delegations = fd_banks_stake_delegations_root_query( banks );

  fd_pubkey_t stake_account = { .ul = { 1UL, 2UL, 3UL, 4UL } };
  fd_pubkey_t vote_account  = { .ul = { 5UL, 6UL, 7UL, 8UL } };
  fd_stake_state_t state[1];
  make_stake_state( state, &vote_account );

  uchar entry[ 136UL + sizeof(fd_stake_state_t) ] __attribute__((aligned(8)));
  fd_memset( entry, 0, sizeof(entry) );
  FD_STORE( ulong, entry+8UL,  sizeof(fd_stake_state_t) );
  fd_memcpy( entry+16UL,  &stake_account,               sizeof(fd_pubkey_t)      );
  FD_STORE( ulong, entry+48UL, 5000UL );
  fd_memcpy( entry+64UL,  &fd_solana_stake_program_id,  sizeof(fd_pubkey_t)      );
  fd_memcpy( entry+136UL, state,                        sizeof(fd_stake_state_t) );

  fd_snapin_tile_t ctx = { .full = 1, .banks = banks };
  fd_ssparse_advance_result_t result = {
    .account_batch = {
      .batch     = { entry },
      .batch_cnt = 1UL,
      .slot      = 10UL,
    },
  };

  FD_TEST( !process_account_batch( &ctx, &result, (fd_stem_context_t *)1UL ) );
  assert_stake_delegation( stake_delegations, &stake_account, &vote_account );

  free( banks_mem );
}

static void
test_streaming_stake_delegation( void ) {
  void * banks_mem;
  fd_banks_t * banks = new_banks( &banks_mem );
  fd_stake_delegations_t * stake_delegations = fd_banks_stake_delegations_root_query( banks );

  fd_pubkey_t stake_account = { .ul = { 11UL, 12UL, 13UL, 14UL } };
  fd_pubkey_t vote_account  = { .ul = { 15UL, 16UL, 17UL, 18UL } };
  fd_stake_state_t state[1];
  make_stake_state( state, &vote_account );

  fd_snapin_tile_t ctx = { .full = 1, .banks = banks };
  fd_ssparse_advance_result_t header = {
    .account_header = {
      .pubkey     = stake_account.uc,
      .slot       = 10UL,
      .lamports   = 5000UL,
      .data_len   = sizeof(fd_stake_state_t),
      .owner      = fd_solana_stake_program_id.uc,
      .executable = 0,
    },
  };
  FD_TEST( !process_account_header( &ctx, &header, (fd_stem_context_t *)1UL ) );

  ulong split = sizeof(fd_stake_state_t)/2UL;
  fd_ssparse_advance_result_t data = {
    .account_data = {
      .data    = (uchar const *)state,
      .data_sz = split,
    },
  };
  process_account_data( &ctx, &data );
  FD_TEST( !fd_stake_delegation_root_query( stake_delegations, &stake_account ) );

  data.account_data.data    = (uchar const *)state + split;
  data.account_data.data_sz = sizeof(fd_stake_state_t) - split;
  process_account_data( &ctx, &data );
  assert_stake_delegation( stake_delegations, &stake_account, &vote_account );

  free( banks_mem );
}

static void
test_txncache_staging_entry_size( void ) {
  fd_snapin_tile_t ctx[ 1 ];
  FD_TEST( sizeof(ctx->txncache_entries[ 0 ])==20UL );
}

static ulong
test_txncache_staging_slot_prepare( fd_snapin_tile_t * ctx,
                                    ulong               slot ) {
  ulong candidate_idx;
  if( ctx->txncache_slots_len<FD_TXNCACHE_MAX_SLOT_DELTAS ) {
    candidate_idx = ctx->txncache_slots_len++;
  } else {
    candidate_idx = 0UL;
    for( ulong i=1UL; i<FD_TXNCACHE_MAX_SLOT_DELTAS; i++ ) {
      if( ctx->txncache_slots[ i ].slot<ctx->txncache_slots[ candidate_idx ].slot ) candidate_idx = i;
    }
    if( slot<ctx->txncache_slots[ candidate_idx ].slot ) return ULONG_MAX;
  }

  ctx->txncache_slots[ candidate_idx ].slot      = slot;
  ctx->txncache_slots[ candidate_idx ].entry_cnt = 0UL;
  return candidate_idx;
}

static void
test_txncache_staging_slot_begin( fd_snapin_tile_t * ctx,
                                  ulong               slot ) {
  ctx->txncache_current_slot_idx       = test_txncache_staging_slot_prepare( ctx, slot );
  ctx->txncache_current_slot_entry_cnt = 0UL;
}

static void
test_txncache_staging_evicts_oldest_slot( void ) {
  fd_snapin_tile_t ctx[ 1 ] = {0};
  ctx->txncache_current_slot_idx       = ULONG_MAX;
  ctx->txncache_current_slot_entry_cnt = 0UL;
  ctx->txncache_slots_len              = 0UL;

  ulong oldest_idx = ULONG_MAX;
  for( ulong i=0UL; i<FD_TXNCACHE_MAX_SLOT_DELTAS; i++ ) {
    ulong slot_idx = test_txncache_staging_slot_prepare( ctx, 1000UL+i );
    FD_TEST( slot_idx!=ULONG_MAX );
    if( FD_UNLIKELY( !i ) ) oldest_idx = slot_idx;
  }

  FD_TEST( oldest_idx!=ULONG_MAX );
  ctx->txncache_slots[ oldest_idx ].entry_cnt = 7UL;
  fd_sstxncache_hash_t oldest_entries[ 7UL ];
  ctx->txncache_entries = oldest_entries;

  blockhash_group_t oldest_group = {
    .slot               = 1000UL,
    .txncache_entry_idx = oldest_idx*FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT,
    .txncache_entry_cnt = 7UL
  };
  ulong group_slot_idx = oldest_group.txncache_entry_idx/FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT;
  FD_TEST( group_slot_idx<ctx->txncache_slots_len );
  FD_TEST( ctx->txncache_slots[ group_slot_idx ].slot==oldest_group.slot );

  FD_TEST( test_txncache_staging_slot_prepare( ctx, 999UL )==ULONG_MAX );
  FD_TEST( ctx->txncache_slots[ oldest_idx ].slot==1000UL );

  ulong replacement_idx = test_txncache_staging_slot_prepare( ctx, 1200UL );
  FD_TEST( replacement_idx==oldest_idx );
  FD_TEST( ctx->txncache_slots[ group_slot_idx ].slot!=oldest_group.slot );
  FD_TEST( ctx->txncache_slots[ replacement_idx ].slot==1200UL );
  FD_TEST( ctx->txncache_slots[ replacement_idx ].entry_cnt==0UL );
}

static void
test_txncache_staging_fits_one_gigantic_page( void ) {
  fd_topo_tile_t tile = {0};
  tile.snapin.max_live_slots = 2048UL;
  FD_TEST( scratch_footprint( &tile )<(1UL<<30) );
}

static void
test_txncache_staging_validates_stale_group_offsets( void ) {
  fd_snapin_tile_t ctx[ 1 ] = {0};
  ctx->txncache_current_slot_idx       = ULONG_MAX;
  ctx->txncache_current_slot_entry_cnt = 0UL;
  ctx->txncache_slots_len              = 0UL;
  ctx->seed = 1UL;

  static uchar const blockhash[ 32UL ] = {1U};
  blockhash_group_t groups[ 2UL ] = {
    {
      .slot               = 1000UL,
      .txnhash_offset     = 1UL,
      .txncache_entry_idx = 0UL,
      .txncache_entry_cnt = 0UL
    },
    {
      .slot               = 1200UL,
      .txnhash_offset     = 2UL,
      .txncache_entry_idx = 0UL,
      .txncache_entry_cnt = 0UL
    }
  };
  fd_memcpy( groups[ 0UL ].blockhash, blockhash, sizeof(blockhash) );
  fd_memcpy( groups[ 1UL ].blockhash, blockhash, sizeof(blockhash) );
  ctx->blockhash_groups     = groups;
  ctx->blockhash_groups_len = 2UL;

  for( ulong i=0UL; i<FD_TXNCACHE_MAX_SLOT_DELTAS; i++ ) {
    test_txncache_staging_slot_begin( ctx, 1000UL+i );
  }
  test_txncache_staging_slot_begin( ctx, 1200UL );

  fd_sstxncache_hash_t entries[ 1UL ];
  ctx->txncache_entries = entries;

  ulong shmem_sz = fd_txncache_shmem_footprint( 1UL, 1UL, 0 );
  shmem_sz = fd_ulong_align_up( shmem_sz, fd_txncache_shmem_align() );
  void * shmem = aligned_alloc( fd_txncache_shmem_align(), shmem_sz );
  FD_TEST( shmem );
  fd_txncache_shmem_t * txncache_shmem = fd_txncache_shmem_join( fd_txncache_shmem_new( shmem, 1UL, 1UL, 0, 0UL ) );
  FD_TEST( txncache_shmem );

  ulong local_sz = fd_ulong_align_up( fd_txncache_footprint( 1UL ), fd_txncache_align() );
  void * local = aligned_alloc( fd_txncache_align(), local_sz );
  FD_TEST( local );
  ctx->txncache = fd_txncache_join( fd_txncache_new( local, txncache_shmem ) );
  FD_TEST( ctx->txncache );

  fd_snapshot_manifest_blockhash_t blockhashes[ FD_BLOCKHASHES_MAX ] = {{ .hash_index = 0UL }};
  fd_memcpy( blockhashes[ 0UL ].hash, blockhash, sizeof(blockhash) );
  FD_TEST( populate_txncache( ctx, blockhashes, 1UL )==1 );

  free( local );
  free( shmem );
}

/* Parallel loader (D9) role tests ***********************************/

static uchar test_worker_snoop_mem[ sizeof(fd_snapio_worker_snoop_t) + 64UL*sizeof(fd_snapio_stake_ent_t) ] __attribute__((aligned(64)));
static fd_snapin_extent_t test_worker_fifo[ FD_SNAPIN_FIFO_CNT ];
static uchar test_worker_write_buf[ FD_SNAPIN_WRITE_BUF_SZ ]    __attribute__((aligned(4096)));
static uchar test_job_mem [ FD_SNAPIN_IO_JOB_SLOT_SZ ]          __attribute__((aligned(FD_CHUNK_ALIGN)));
static uchar test_ack_mem [ FD_SNAPIN_IO_ACK_SLOT_SZ ]          __attribute__((aligned(FD_CHUNK_ALIGN)));
static uchar test_wlane_mem[ 4 ][ 4096UL ]                      __attribute__((aligned(FD_CHUNK_ALIGN)));
static uchar test_ring_mem[ 8 ][ FD_SNAPIN_IO_JOB_SLOT_SZ ]     __attribute__((aligned(FD_CHUNK_ALIGN)));
static uchar test_wack_mem[ 8 ][ FD_SNAPIN_IO_ACK_SLOT_SZ ]     __attribute__((aligned(FD_CHUNK_ALIGN)));

/* Worker in link layout mirrors the real topology: the snapin_io ring
   at in_idx 0, the snapdc lanes at in_idx 1..lane_cnt. */

static void
worker_ctx_init( fd_snapin_tile_t * ctx,
                 ulong              lane_cnt ) {
  fd_memset( ctx, 0, sizeof(*ctx) );
  ctx->role            = FD_SNAPIN_ROLE_ACCDB_WORKER;
  ctx->worker_idx      = 0UL;
  ctx->full            = 1;
  ctx->state           = FD_SNAPSHOT_STATE_IDLE;
  ctx->lane_cnt        = lane_cnt;
  ctx->pending_control = ULONG_MAX;
  ctx->job_in_idx      = 0UL;
  ctx->in_lane[ 0 ]    = ULONG_MAX;
  ctx->io_in[ 0 ].wksp   = (fd_wksp_t *)test_job_mem;
  ctx->io_in[ 0 ].chunk0 = 0UL;
  ctx->io_in[ 0 ].wmark  = 0UL;
  ctx->io_in[ 0 ].mtu    = FD_SNAPIN_IO_JOB_SLOT_SZ;
  for( ulong lane=0UL; lane<lane_cnt; lane++ ) {
    ctx->in_lane[ 1UL+lane ] = lane;
    ctx->in[ lane ].wksp   = (fd_wksp_t *)test_wlane_mem[ lane ];
    ctx->in[ lane ].chunk0 = 0UL;
    ctx->in[ lane ].wmark  = 0UL;
    ctx->in[ lane ].mtu    = 4096UL;
  }
  ctx->fifo          = test_worker_fifo;
  ctx->write_buf     = test_worker_write_buf;
  ctx->my_snoop      = (fd_snapio_worker_snoop_t *)test_worker_snoop_mem;
  ctx->stake_log_max = 64UL;
  ctx->ack_out = (fd_snapin_out_link_t){ .idx = 7UL, .mem = (fd_wksp_t *)test_ack_mem, .chunk0 = 0UL, .wmark = 0UL, .chunk = 0UL, .mtu = FD_SNAPIN_IO_ACK_SLOT_SZ };
}

static int
send_job( fd_snapin_tile_t * ctx,
          ulong kind, ulong gen, ulong av_idx,
          ulong slot, ulong body_off, ulong body_sz, ulong covered_until,
          ulong fork_id ) {
  fd_snapin_io_job_t * job = (fd_snapin_io_job_t *)test_job_mem;
  fd_memset( job, 0, sizeof(*job) );
  job->kind          = kind;
  job->generation    = gen;
  job->appendvec_idx = av_idx;
  job->slot          = slot;
  job->body_off      = body_off;
  job->body_sz       = body_sz;
  job->covered_until = covered_until;
  job->fork_id       = fork_id;
  FD_TEST( !before_frag( ctx, ctx->job_in_idx, 0UL, kind ) );
  return returnable_frag( ctx, ctx->job_in_idx, 0UL, kind, 0UL, sizeof(fd_snapin_io_job_t),
                          0UL, 0UL, 0UL, (fd_stem_context_t *)1UL );
}

static void
worker_send_control( fd_snapin_tile_t * ctx,
                     ulong              lane,
                     ulong              sig ) {
  FD_TEST( !returnable_frag( ctx, 1UL+lane, 0UL, sig, 0UL, 0UL, 0UL, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
}

static int
send_worker_data( fd_snapin_tile_t * ctx,
                  ulong              lane,
                  ulong              sz ) {
  ulong sig = FD_SNAPSHOT_MSG_DATA;
  ulong ctl = fd_frag_meta_ctl( 0UL, 0, 0, 0 );
  FD_TEST( !before_frag( ctx, 1UL+lane, 0UL, sig ) );
  return returnable_frag( ctx, 1UL+lane, 0UL, sig, 0UL, sz, ctl, 0UL, 0UL, (fd_stem_context_t *)1UL );
}

static fd_snapin_io_ack_t const *
last_worker_ack( void ) {
  return (fd_snapin_io_ack_t const *)test_ack_mem;
}

static void
worker_send_init( fd_snapin_tile_t * ctx ) {
  ulong pub0 = test_pub_cnt;
  for( ulong lane=0UL; lane<ctx->lane_cnt; lane++ ) worker_send_control( ctx, lane, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );
  FD_TEST( test_pub_cnt==pub0+1UL );
  FD_TEST( test_pub_sig[ test_pub_cnt-1UL ]==fd_snapin_io_ack_sig( ctx->generation, FD_SNAPSHOT_MSG_CTRL_INIT_FULL ) );
}

static void
test_worker_coverage_hold( void ) {
  fd_snapin_tile_t ctx[1];
  worker_ctx_init( ctx, 2UL );
  test_pub_cnt = 0UL;
  test_accdb_writer_begin_cnt = 0UL;
  worker_send_init( ctx );
  FD_TEST( ctx->generation==1UL );
  FD_TEST( test_accdb_writer_begin_cnt==1UL );

  /* No coverage yet: data frag held (and counted) */
  FD_TEST( send_worker_data( ctx, 0UL, 100UL )==1 );
  FD_TEST( ctx->cursor==0UL );
  FD_TEST( ctx->cov_stats.hold_reprocess==1UL );

  /* WATERMARK covers the first 512-aligned entry */
  FD_TEST( !send_job( ctx, FD_SNAPIN_IO_KIND_WATERMARK, 1UL, 0UL, 0UL, 0UL, 0UL, 512UL, ULONG_MAX ) );
  FD_TEST( ctx->covered_until==512UL );

  /* Reprocess: fully consumed (skipped, not ours) */
  FD_TEST( send_worker_data( ctx, 0UL, 100UL )==0 );
  FD_TEST( ctx->cursor==100UL );

  /* Partially covered frag: consume up to the watermark, hold the rest */
  FD_TEST( send_worker_data( ctx, 0UL, 512UL )==1 );
  FD_TEST( ctx->cursor==512UL );
  FD_TEST( ctx->in[ 0 ].pos==412UL );
  FD_TEST( ctx->cov_stats.hold_reprocess==2UL );

  /* EOS extends coverage to infinity: reprocess drains */
  FD_TEST( !send_job( ctx, FD_SNAPIN_IO_KIND_EOS, 1UL, 0UL, 0UL, 0UL, 0UL, 0UL, ULONG_MAX ) );
  FD_TEST( ctx->eos_seen );
  FD_TEST( send_worker_data( ctx, 0UL, 512UL )==0 );
  FD_TEST( ctx->cursor==612UL );
  FD_TEST( ctx->in[ 0 ].pos==0UL );
}

static void
test_worker_owned_appendvec( void ) {
  fd_snapin_tile_t ctx[1];
  worker_ctx_init( ctx, 2UL );
  test_pub_cnt = 0UL;
  worker_send_init( ctx );

  /* Appendvec for this worker at body [512, 612); ASSIGN carries the
     coverage through its own (padded) end inline. */
  FD_TEST( !send_job( ctx, FD_SNAPIN_IO_KIND_ASSIGN, 1UL, 5UL, 42UL, 512UL, 100UL, 1024UL, (ulong)USHORT_MAX ) );
  FD_TEST( ctx->fifo_tail==1UL && ctx->fifo_head==0UL );
  FD_TEST( ctx->covered_until==1024UL );
  FD_TEST( ctx->cov_stats.lag_samples==1UL && ctx->cov_stats.lag_max==1024UL );

  /* Another worker's coverage arrives on the periodic watermark */
  FD_TEST( !send_job( ctx, FD_SNAPIN_IO_KIND_WATERMARK, 1UL, 0UL, 0UL, 0UL, 0UL, 2048UL, ULONG_MAX ) );
  FD_TEST( ctx->fifo_tail==1UL );
  FD_TEST( ctx->covered_until==2048UL );

  test_parser_script   = 4;
  test_parser_call_cnt = 0UL;

  /* One frag spans skip(512) + owned body(100) + skip(88) */
  FD_TEST( send_worker_data( ctx, 0UL, 700UL )==0 );
  FD_TEST( ctx->cursor==700UL );
  FD_TEST( ctx->fifo_head==1UL );
  FD_TEST( !ctx->av_active );
  FD_TEST( test_parser_call_cnt==1UL );
}

/* End-to-end worker staging: an owned appendvec body parsed into an
   account batch (one ignored, one replaced, one loaded) plus a
   fragmented account, the accepted records staged at the mock
   allocator's explicit offsets, flushed at FINI, and readable from the
   memfd backing FD_ACCDB_FD_RW. */
static void
test_worker_batch_staging( void ) {
  fd_snapin_tile_t ctx[1];
  worker_ctx_init( ctx, 1UL );
  test_pub_cnt = 0UL;
  worker_send_init( ctx );

  /* Batch entries: (pubkey byte, lamports, data_len, exec, data byte) */
  struct { uchar pk; ulong lamports; ulong len; int exec; uchar db; } spec[3] = {
    { 0xA1, 10UL,  5UL, 0, 0x51 },
    { 0xB2, 20UL,  0UL, 1, 0x00 },
    { 0xC3, 30UL, 12UL, 0, 0x52 },
  };
  for( ulong i=0UL; i<3UL; i++ ) {
    uchar * e = test_script5_ent[ i ];
    fd_memset( e, 0, 160UL );
    FD_STORE( ulong, e+8UL, spec[ i ].len );
    fd_memset( e+16UL, spec[ i ].pk, 32UL );
    FD_STORE( ulong, e+48UL, spec[ i ].lamports );
    fd_memset( e+64UL, (uchar)(spec[ i ].pk+1), 32UL );
    e[ 96UL ] = (uchar)spec[ i ].exec;
    fd_memset( e+136UL, spec[ i ].db, spec[ i ].len );
  }
  fd_memset( test_script5_hdr_pk,    0xD4, 32UL );
  fd_memset( test_script5_hdr_owner, 0xD5, 32UL );
  fd_memset( test_script5_hdr_data,  0x53, 8UL  );

  /* Owned appendvec spanning the whole 100-byte frag at offset 0 */
  FD_TEST( !send_job( ctx, FD_SNAPIN_IO_KIND_ASSIGN, 1UL, 0UL, 440123518UL, 0UL, 100UL, 512UL, (ulong)USHORT_MAX ) );

  test_parser_script   = 5;
  test_parser_call_cnt = 0UL;
  test_accdb_worker_call_cnt  = 0UL;
  test_accdb_worker_next_off  = 4096UL;
  test_accdb_worker_skip_mask = 1UL; /* entry 0 ignored */
  test_accdb_worker_repl_mask = 2UL; /* entry 1 replaced */
  FD_TEST( send_worker_data( ctx, 0UL, 100UL )==0 );

  FD_TEST( test_accdb_worker_call_cnt==2UL ); /* batch + header account */
  FD_TEST( test_accdb_worker_slot==440123518UL );
  FD_TEST( ctx->metrics.accounts_ignored==1UL );
  FD_TEST( ctx->metrics.accounts_replaced==1UL );
  FD_TEST( ctx->metrics.accounts_loaded==2UL );
  FD_TEST( ctx->metrics.total_accounts_processed==4UL );
  FD_TEST( ctx->worker.input_lamports==100UL );
  FD_TEST( ctx->worker.replaced_lamports==20UL );
  FD_TEST( ctx->worker.ignored_lamports==10UL );
  FD_TEST( ctx->rec_idx==4UL );
  FD_TEST( !ctx->av_active && ctx->cursor==100UL );

  /* FINI (after EOS) flushes the staging and acks with the counters. */
  test_accdb_writer_end_cnt   = 0UL;
  test_accdb_worker_close_cnt = 0UL;
  FD_TEST( !send_job( ctx, FD_SNAPIN_IO_KIND_EOS, 1UL, 0UL, 0UL, 0UL, 0UL, 0UL, ULONG_MAX ) );
  test_pub_cnt = 0UL;
  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
  FD_TEST( !ctx->pending_fini );
  FD_TEST( test_accdb_writer_end_cnt==1UL );
  FD_TEST( test_accdb_worker_close_cnt==1UL );
  FD_TEST( !ctx->write_buf_used );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[0]==fd_snapin_io_ack_sig( 1UL, FD_SNAPSHOT_MSG_CTRL_FINI ) );
  fd_snapin_io_ack_t const * ack = last_worker_ack();
  FD_TEST( ack->worker_idx==0UL );
  FD_TEST( ack->accounts_ignored==1UL );
  FD_TEST( ack->accounts_replaced==1UL );
  FD_TEST( ack->accounts_loaded==2UL );
  FD_TEST( ack->input_lamports==100UL );
  FD_TEST( ack->replaced_lamports==20UL );
  FD_TEST( ack->ignored_lamports==10UL );

  /* Ignored entry 0 burned no space; the accepted records are packed at
     the allocator's explicit offsets: entry1@4096 (72+0), entry2@4168
     (72+12), header account@4252 (72+8). */
  uchar disk[ 128 ];
  FD_TEST( pread( FD_ACCDB_FD_RW, disk, sizeof(fd_accdb_disk_meta_t), 4096L )==(long)sizeof(fd_accdb_disk_meta_t) );
  fd_accdb_disk_meta_t const * m1 = (fd_accdb_disk_meta_t const *)disk;
  FD_TEST( m1->pubkey[ 0 ]==0xB2 && m1->size==0U && m1->owner[ 0 ]==0xB3 );
  FD_TEST( pread( FD_ACCDB_FD_RW, disk, sizeof(fd_accdb_disk_meta_t)+12UL, 4168L )==(long)(sizeof(fd_accdb_disk_meta_t)+12UL) );
  fd_accdb_disk_meta_t const * m2 = (fd_accdb_disk_meta_t const *)disk;
  FD_TEST( m2->pubkey[ 0 ]==0xC3 && m2->size==12U );
  for( ulong i=0UL; i<12UL; i++ ) FD_TEST( disk[ sizeof(fd_accdb_disk_meta_t)+i ]==0x52 );
  FD_TEST( pread( FD_ACCDB_FD_RW, disk, sizeof(fd_accdb_disk_meta_t)+8UL, 4252L )==(long)(sizeof(fd_accdb_disk_meta_t)+8UL) );
  fd_accdb_disk_meta_t const * m3 = (fd_accdb_disk_meta_t const *)disk;
  FD_TEST( m3->pubkey[ 0 ]==0xD4 && m3->size==8U && m3->owner[ 0 ]==0xD5 );
  for( ulong i=0UL; i<8UL; i++ ) FD_TEST( disk[ sizeof(fd_accdb_disk_meta_t)+i ]==0x53 );

  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_DONE );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_SHUTDOWN );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_SHUTDOWN );
  FD_TEST( should_shutdown( ctx ) );
}

static void
test_worker_stale_generation( void ) {
  fd_snapin_tile_t ctx[1];
  worker_ctx_init( ctx, 1UL );
  test_pub_cnt = 0UL;
  worker_send_init( ctx );
  FD_TEST( ctx->generation==1UL );

  /* Stale job (previous attempt): drained, no effect */
  FD_TEST( !send_job( ctx, FD_SNAPIN_IO_KIND_WATERMARK, 0UL, 0UL, 0UL, 0UL, 0UL, 512UL, ULONG_MAX ) );
  FD_TEST( ctx->covered_until==0UL );

  /* Future job (coordinator's INIT raced ahead): held */
  FD_TEST( send_job( ctx, FD_SNAPIN_IO_KIND_WATERMARK, 2UL, 0UL, 0UL, 0UL, 0UL, 512UL, ULONG_MAX )==1 );
  FD_TEST( ctx->covered_until==0UL );

  /* Current generation: applied */
  FD_TEST( !send_job( ctx, FD_SNAPIN_IO_KIND_WATERMARK, 1UL, 0UL, 0UL, 0UL, 0UL, 512UL, ULONG_MAX ) );
  FD_TEST( ctx->covered_until==512UL );

  /* A full FIFO holds ASSIGNs (reprocess later) */
  ctx->fifo_head = 0UL;
  ctx->fifo_tail = FD_SNAPIN_FIFO_CNT;
  FD_TEST( send_job( ctx, FD_SNAPIN_IO_KIND_ASSIGN, 1UL, 5UL, 42UL, 512UL, 100UL, 1024UL, (ulong)USHORT_MAX )==1 );
  ctx->fifo_tail = 0UL;

  /* Stale ASSIGN after FAIL (state IDLE, same generation): drained */
  for( ulong lane=0UL; lane<ctx->lane_cnt; lane++ ) worker_send_control( ctx, lane, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( test_pub_sig[ test_pub_cnt-1UL ]==fd_snapin_io_ack_sig( 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL ) );
  FD_TEST( !send_job( ctx, FD_SNAPIN_IO_KIND_ASSIGN, 1UL, 5UL, 512UL, 512UL, 100UL, 1024UL, (ulong)USHORT_MAX ) );
  FD_TEST( ctx->fifo_tail==0UL );
}

static void
test_worker_fini_deferred_ack( void ) {
  fd_snapin_tile_t ctx[1];
  worker_ctx_init( ctx, 2UL );
  test_pub_cnt = 0UL;
  worker_send_init( ctx );

  /* FINI barrier completes but EOS has not arrived on the job ring:
     the ack is deferred (and the lanes are NOT held, so a FAIL queued
     behind could still flow). */
  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  worker_send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->pending_fini );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );
  FD_TEST( test_pub_cnt==1UL ); /* just the INIT ack */

  int poll_in = 1; int charge_busy = 0;
  after_credit( ctx, (fd_stem_context_t *)1UL, &poll_in, &charge_busy );
  FD_TEST( ctx->pending_fini ); /* still waiting for EOS */

  FD_TEST( !send_job( ctx, FD_SNAPIN_IO_KIND_EOS, 1UL, 0UL, 0UL, 0UL, 0UL, 0UL, ULONG_MAX ) );
  after_credit( ctx, (fd_stem_context_t *)1UL, &poll_in, &charge_busy );
  FD_TEST( !ctx->pending_fini );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
  FD_TEST( test_pub_cnt==2UL );
  FD_TEST( test_pub_sig[ 1 ]==fd_snapin_io_ack_sig( 1UL, FD_SNAPSHOT_MSG_CTRL_FINI ) );
  FD_TEST( last_worker_ack()->control==FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( last_worker_ack()->worker_idx==0UL );

  /* NEXT/DONE acked immediately */
  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_DONE );
  worker_send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_DONE );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( test_pub_sig[ 2 ]==fd_snapin_io_ack_sig( 1UL, FD_SNAPSHOT_MSG_CTRL_DONE ) );

  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_SHUTDOWN );
  worker_send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_SHUTDOWN );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_SHUTDOWN );
  FD_TEST( should_shutdown( ctx ) );
}

static void
test_worker_fail_cancels_pending_fini( void ) {
  fd_snapin_tile_t ctx[1];
  worker_ctx_init( ctx, 2UL );
  test_pub_cnt = 0UL;
  worker_send_init( ctx );

  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  worker_send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->pending_fini );

  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  worker_send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( !ctx->pending_fini );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( test_pub_sig[ test_pub_cnt-1UL ]==fd_snapin_io_ack_sig( 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL ) );

  /* Retry: INIT bumps the generation */
  worker_send_init( ctx );
  FD_TEST( ctx->generation==2UL );
}

/* A coverage-held worker cannot reach a FAIL queued behind the held
   DATA frag: the ABORT ring job bypasses the lane and flips the worker
   to ERROR-drain so FAIL flows. */
static void
test_worker_abort_ring_bypass( void ) {
  fd_snapin_tile_t ctx[1];
  worker_ctx_init( ctx, 1UL );
  test_pub_cnt = 0UL;
  worker_send_init( ctx );

  /* Held on coverage */
  FD_TEST( send_worker_data( ctx, 0UL, 100UL )==1 );

  /* ABORT: silent (no ERROR ack; the coordinator already knows) */
  ulong pub0 = test_pub_cnt;
  FD_TEST( !send_job( ctx, FD_SNAPIN_IO_KIND_ABORT, 1UL, 0UL, 0UL, 0UL, 0UL, 0UL, ULONG_MAX ) );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( test_pub_cnt==pub0 );

  /* The held frag is now dropped by the ERROR-state filter */
  FD_TEST( before_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_DATA )==1 );
  FD_TEST( before_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI )==1 );
  FD_TEST( before_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL )==0 );

  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( test_pub_sig[ test_pub_cnt-1UL ]==fd_snapin_io_ack_sig( 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL ) );
}

static void
test_worker_error_mid_init_barrier( void ) {
  /* Regression: an ERROR processed while the INIT barrier is only
     partially complete aborts the barrier.  The generation must have
     been bumped at the FIRST INIT frag (not at completion), or the
     worker's subsequent ERROR/FAIL acks would be dropped by the
     coordinator as generation-stale, hanging the FAIL ack mask. */
  fd_snapin_tile_t ctx[1];
  worker_ctx_init( ctx, 2UL );
  test_pub_cnt = 0UL;

  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE ); /* barrier incomplete */
  FD_TEST( ctx->generation==1UL );               /* but generation already bumped */
  FD_TEST( !test_pub_cnt );

  /* Immediate ERROR (queued behind INIT on lane 0) aborts the barrier */
  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[ 0 ]==fd_snapin_io_ack_sig( 1UL, FD_SNAPSHOT_MSG_CTRL_ERROR ) );

  /* Lane 1's INIT would be drained by the ERROR-state filter */
  FD_TEST( before_frag( ctx, 2UL, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL )>0 );

  /* FAIL supersedes the aborted INIT barrier and is acked at the
     CURRENT generation */
  worker_send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  worker_send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( test_pub_sig[ 1 ]==fd_snapin_io_ack_sig( 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL ) );

  /* Retry: the next attempt's generation is 2 on both sides */
  worker_send_init( ctx );
  FD_TEST( ctx->generation==2UL );
}

/* Coordinator (io) side *********************************************/

static void
coordinator_io_ctx_init_n( fd_snapin_tile_t * ctx,
                           ulong              lane_cnt,
                           int                state,
                           ulong              worker_cnt ) {
  sync_ctx_init( ctx, lane_cnt, state );
  ctx->io_enabled = 1;
  ctx->worker_cnt = worker_cnt;
  ctx->generation = 1UL;
  ctx->pending_worker_control = ULONG_MAX;
  for( ulong w=0UL; w<FD_SNAPIN_WORKER_MAX; w++ ) ctx->ack_in_idx[ w ] = ULONG_MAX;
  for( ulong w=0UL; w<worker_cnt; w++ ) ctx->ack_in_idx[ w ] = lane_cnt+w;
  for( ulong w=0UL; w<worker_cnt; w++ ) {
    ctx->io_in[ w ].wksp   = (fd_wksp_t *)test_wack_mem[ w ];
    ctx->io_in[ w ].chunk0 = 0UL;
    ctx->io_in[ w ].wmark  = 0UL;
    ctx->io_in[ w ].mtu    = FD_SNAPIN_IO_ACK_SLOT_SZ;
    ctx->io_out[ w ].idx    = 10UL+w;
    ctx->io_out[ w ].mem    = (fd_wksp_t *)test_ring_mem[ w ];
    ctx->io_out[ w ].chunk0 = 0UL;
    ctx->io_out[ w ].wmark  = 0UL;
    ctx->io_out[ w ].chunk  = 0UL;
    ctx->io_out[ w ].mtu    = FD_SNAPIN_IO_JOB_SLOT_SZ;
  }
}

static void
coordinator_io_ctx_init( fd_snapin_tile_t * ctx,
                         ulong              lane_cnt,
                         int                state ) {
  coordinator_io_ctx_init_n( ctx, lane_cnt, state, 2UL );
}

static int
coordinator_send_ack_ex( fd_snapin_tile_t * ctx,
                         ulong              worker,
                         ulong              gen,
                         ulong              control,
                         ulong              loaded,
                         ulong              eq_slot_dups,
                         int                err ) {
  fd_snapin_io_ack_t * ack = (fd_snapin_io_ack_t *)test_wack_mem[ worker ];
  fd_memset( ack, 0, sizeof(*ack) );
  ack->worker_idx      = worker;
  ack->generation      = gen;
  ack->control         = control;
  ack->accounts_loaded = loaded;
  ack->input_lamports  = 10UL*loaded;
  ack->eq_slot_dups    = eq_slot_dups;
  ack->err             = err;
  ulong in_idx = ctx->ack_in_idx[ worker ];
  FD_TEST( !before_frag( ctx, in_idx, 0UL, fd_snapin_io_ack_sig( gen, control ) ) );
  return returnable_frag( ctx, in_idx, 0UL, fd_snapin_io_ack_sig( gen, control ), 0UL,
                          sizeof(fd_snapin_io_ack_t), 0UL, 0UL, 0UL, (fd_stem_context_t *)1UL );
}

static void
coordinator_send_ack( fd_snapin_tile_t * ctx,
                      ulong              worker,
                      ulong              gen,
                      ulong              control,
                      ulong              loaded,
                      ulong              eq_slot_dups,
                      int                err ) {
  FD_TEST( !coordinator_send_ack_ex( ctx, worker, gen, control, loaded, eq_slot_dups, err ) );
}

static void
test_coordinator_ack_gating( void ) {
  fd_snapin_tile_t ctx[1];
  coordinator_io_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_FINISHING );
  test_pub_cnt = 0UL;

  /* FINI barrier completes: local processing, forward to snapct is
     deferred until both workers ack; lanes are held meanwhile. */
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->pending_worker_control==FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( test_pub_cnt==0UL );
  FD_TEST( before_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_DATA )<0 );
  FD_TEST( !before_frag( ctx, ctx->ack_in_idx[ 0 ], 0UL, 0UL ) );

  /* Stale ack (wrong control) is dropped */
  coordinator_send_ack( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_NEXT, 0UL, 0UL, 0 );
  FD_TEST( ctx->pending_worker_ack_mask==0UL );

  coordinator_send_ack( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 7UL, 0UL, 0 );
  FD_TEST( ctx->pending_worker_ack_mask==1UL );
  FD_TEST( test_pub_cnt==0UL );

  coordinator_send_ack( ctx, 1UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 5UL, 0UL, 0 );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[ 0 ]==FD_SNAPSHOT_MSG_CTRL_FINI );
  /* Counters folded from the FINI acks */
  FD_TEST( ctx->metrics.accounts_loaded==12UL );
  FD_TEST( ctx->capitalization==120UL );
  FD_TEST( !before_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_DATA ) );
}

static void
test_coordinator_worker_error_ack( void ) {
  fd_snapin_tile_t ctx[1];
  coordinator_io_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_FINISHING );
  test_pub_cnt = 0UL;

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->pending_worker_control==FD_SNAPSHOT_MSG_CTRL_FINI );

  /* Worker 1 reports an error: the coordinator abandons the ack wait,
     publishes ABORT on every worker ring and ERROR to snapct. */
  coordinator_send_ack( ctx, 1UL, 1UL, FD_SNAPSHOT_MSG_CTRL_ERROR, 0UL, 0UL, 5 );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( test_pub_cnt==3UL );
  FD_TEST( test_pub_out_idx[ 0 ]==10UL && test_pub_sig[ 0 ]==FD_SNAPIN_IO_KIND_ABORT );
  FD_TEST( test_pub_out_idx[ 1 ]==11UL && test_pub_sig[ 1 ]==FD_SNAPIN_IO_KIND_ABORT );
  FD_TEST( test_pub_sig[ 2 ]==FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( ((fd_snapin_io_job_t const *)test_ring_mem[ 0 ])->kind==FD_SNAPIN_IO_KIND_ABORT );
  FD_TEST( ((fd_snapin_io_job_t const *)test_ring_mem[ 1 ])->kind==FD_SNAPIN_IO_KIND_ABORT );

  /* Straggler FINI ack from worker 0 after the error: dropped */
  coordinator_send_ack( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 7UL, 0UL, 0 );
  FD_TEST( ctx->pending_worker_ack_mask==0UL );
  FD_TEST( test_pub_cnt==3UL );

  /* FAIL: forwarded only after both workers acked it */
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->pending_worker_control==FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( test_pub_cnt==3UL );
  coordinator_send_ack( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL, 0UL, 0UL, 0 );
  coordinator_send_ack( ctx, 1UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL, 0UL, 0UL, 0 );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( test_pub_cnt==4UL );
  FD_TEST( test_pub_sig[ 3 ]==FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
}

/* Equal-slot cross-appendvec duplicates cannot be tiebroken with
   worker-local offsets: any nonzero fold at FINI must flag the snapshot
   malformed instead of forwarding FINI. */
static void
test_coordinator_eq_slot_malform( void ) {
  fd_snapin_tile_t ctx[1];
  coordinator_io_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_FINISHING );
  test_pub_cnt = 0UL;

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->pending_worker_control==FD_SNAPSHOT_MSG_CTRL_FINI );

  coordinator_send_ack( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 7UL, 1UL, 0 );
  coordinator_send_ack( ctx, 1UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 5UL, 0UL, 0 );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( ctx->worker_fold.eq_slot_dups==1UL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_ERROR );
  /* ABORT to both rings + ERROR to snapct; FINI never forwarded */
  FD_TEST( test_pub_cnt==3UL );
  FD_TEST( test_pub_sig[ 0 ]==FD_SNAPIN_IO_KIND_ABORT );
  FD_TEST( test_pub_sig[ 1 ]==FD_SNAPIN_IO_KIND_ABORT );
  FD_TEST( test_pub_sig[ 2 ]==FD_SNAPSHOT_MSG_CTRL_ERROR );
}

/* Early worker acks: a worker's lane barrier (or a whole new attempt's
   INIT bump) can complete before the coordinator has consumed its own
   copies of the control frags — the coordinator must HOLD such acks,
   not drop them (dropping wedges the ack mask forever; the race hit
   the 8-worker bench at DONE). */
static void
test_coordinator_early_ack_hold( void ) {
  fd_snapin_tile_t ctx[1];
  coordinator_io_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_FINISHING );
  test_pub_cnt = 0UL;

  /* Ack arrives before the coordinator's own FINI barrier: held. */
  FD_TEST( coordinator_send_ack_ex( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 7UL, 0UL, 0 )==1 );
  FD_TEST( ctx->pending_worker_ack_mask==0UL );

  /* Partial barrier: still held. */
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( coordinator_send_ack_ex( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 7UL, 0UL, 0 )==1 );

  /* Barrier completes: the redelivered ack folds. */
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->pending_worker_control==FD_SNAPSHOT_MSG_CTRL_FINI );
  coordinator_send_ack( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 7UL, 0UL, 0 );
  FD_TEST( ctx->pending_worker_ack_mask==1UL );
  coordinator_send_ack( ctx, 1UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 5UL, 0UL, 0 );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( test_pub_cnt==1UL && test_pub_sig[ 0 ]==FD_SNAPSHOT_MSG_CTRL_FINI );

  /* Future-generation ack (worker's INIT barrier for the next attempt
     raced ahead of ours): held; same-generation-after-bump folds. */
  FD_TEST( coordinator_send_ack_ex( ctx, 0UL, 2UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL, 0UL, 0UL, 0 )==1 );
  /* Stale ack from a previous attempt: dropped. */
  FD_TEST( coordinator_send_ack_ex( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL, 0UL, 0UL, 0 )==0 );

  /* While failing (state ERROR, pending abandoned): acks for abandoned
     controls are dropped so the FAIL ack queued behind them can flow,
     but an EARLY FAIL ack (worker's FAIL barrier completed first) is
     held. */
  ctx->state                  = FD_SNAPSHOT_STATE_ERROR;
  ctx->abort_published        = 1;
  ctx->pending_worker_control = ULONG_MAX;
  FD_TEST( coordinator_send_ack_ex( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 7UL, 0UL, 0 )==0 );
  FD_TEST( coordinator_send_ack_ex( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL, 0UL, 0UL, 0 )==1 );

  /* FAIL barrier completes: the held FAIL ack folds and FAIL flows. */
  test_pub_cnt = 0UL;
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->pending_worker_control==FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  /* A straggling stale FINI ack ahead of worker 1's FAIL ack: dropped
     on the mismatch path. */
  FD_TEST( coordinator_send_ack_ex( ctx, 1UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 7UL, 0UL, 0 )==0 );
  coordinator_send_ack( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL, 0UL, 0UL, 0 );
  coordinator_send_ack( ctx, 1UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL, 0UL, 0UL, 0 );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( test_pub_cnt==1UL && test_pub_sig[ 0 ]==FD_SNAPSHOT_MSG_CTRL_FAIL );
}

/* Full control lifecycle at 8 workers (regression for the n9 post-DONE
   wedge): INIT through SHUTDOWN with worker acks racing ahead of the
   coordinator's own lane barriers at every step. */
static void
test_coordinator_full_lifecycle_8_workers( void ) {
  static uchar init_mem[ 2 ][ FD_CHUNK_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( init_mem, 0, sizeof(init_mem) );

  void * banks_mem;
  fd_banks_t * banks = new_banks( &banks_mem );

  fd_snapin_tile_t ctx[1];
  coordinator_io_ctx_init_n( ctx, 2UL, FD_SNAPSHOT_STATE_IDLE, 8UL );
  ctx->generation = 0UL; /* INIT bumps it */
  ctx->banks      = banks;
  ctx->in[ 0 ].wksp = (fd_wksp_t *)init_mem[ 0 ];
  ctx->in[ 1 ].wksp = (fd_wksp_t *)init_mem[ 1 ];
  test_pub_cnt = 0UL;
  test_accdb_writer_begin_cnt = 0UL;

  /* INIT: one ack before any INIT frag (future generation), one after
     the first frag (same generation, barrier incomplete): both held. */
  FD_TEST( coordinator_send_ack_ex( ctx, 3UL, 1UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL, 0UL, 0UL, 0 )==1 );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( ctx->generation==1UL );
  FD_TEST( coordinator_send_ack_ex( ctx, 5UL, 1UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL, 0UL, 0UL, 0 )==1 );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );
  FD_TEST( ctx->pending_worker_control==FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  for( ulong w=0UL; w<8UL; w++ ) coordinator_send_ack( ctx, w, 1UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL, 0UL, 0UL, 0 );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( test_pub_cnt==1UL && test_pub_sig[ 0 ]==FD_SNAPSHOT_MSG_CTRL_INIT_FULL );

  /* FINI: ALL EIGHT workers ack before the coordinator's own barrier
     (the n9 wedge shape at DONE): all held, then folded. */
  ctx->state   = FD_SNAPSHOT_STATE_FINISHING;
  test_pub_cnt = 0UL;
  for( ulong w=0UL; w<8UL; w++ ) {
    FD_TEST( coordinator_send_ack_ex( ctx, w, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 100UL+w, 0UL, 0 )==1 );
  }
  FD_TEST( ctx->pending_worker_ack_mask==0UL );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->pending_worker_control==FD_SNAPSHOT_MSG_CTRL_FINI );
  for( ulong w=0UL; w<8UL; w++ ) coordinator_send_ack( ctx, w, 1UL, FD_SNAPSHOT_MSG_CTRL_FINI, 100UL+w, 0UL, 0 );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( test_pub_cnt==1UL && test_pub_sig[ 0 ]==FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->metrics.accounts_loaded==8UL*100UL+28UL );

  /* SHUTDOWN: half the acks early, half late. */
  ctx->state   = FD_SNAPSHOT_STATE_IDLE; /* as if DONE completed */
  test_pub_cnt = 0UL;
  for( ulong w=0UL; w<4UL; w++ ) {
    FD_TEST( coordinator_send_ack_ex( ctx, w, 1UL, FD_SNAPSHOT_MSG_CTRL_SHUTDOWN, 0UL, 0UL, 0 )==1 );
  }
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_SHUTDOWN );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_SHUTDOWN );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_SHUTDOWN );
  FD_TEST( ctx->pending_worker_control==FD_SNAPSHOT_MSG_CTRL_SHUTDOWN );
  FD_TEST( !should_shutdown( ctx ) ); /* acks outstanding */
  for( ulong w=0UL; w<8UL; w++ ) coordinator_send_ack( ctx, w, 1UL, FD_SNAPSHOT_MSG_CTRL_SHUTDOWN, 0UL, 0UL, 0 );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( test_pub_cnt==1UL && test_pub_sig[ 0 ]==FD_SNAPSHOT_MSG_CTRL_SHUTDOWN );
  FD_TEST( should_shutdown( ctx ) );

  free( banks_mem );
}

/* Worker write-behind: async writeback kicked per contiguous run,
   partition-rotation discontinuities kick the partial run, and the
   in-flight window applies the smooth wait backstop. */
static void
test_worker_write_behind( void ) {
  static uchar data[ 4096UL ];
  fd_memset( data, 0x66, sizeof(data) );

  fd_snapin_tile_t ctx[1];
  worker_ctx_init( ctx, 1UL );
  ctx->wb_kick_sz = 4096UL;
  ctx->wb_window  = 8192UL;

  worker_buffer_write( ctx, 0UL, data, 4096UL );
  worker_buffer_flush( ctx );
  FD_TEST( ctx->wb_kick_cnt==1UL && ctx->wb_pending==4096UL && ctx->wb_wait_cnt==0UL );

  worker_buffer_write( ctx, 4096UL, data, 4096UL );
  worker_buffer_flush( ctx );
  FD_TEST( ctx->wb_kick_cnt==2UL && ctx->wb_pending==8192UL && ctx->wb_wait_cnt==0UL );

  /* Window exceeded: backstop waits on the oldest range. */
  worker_buffer_write( ctx, 8192UL, data, 4096UL );
  worker_buffer_flush( ctx );
  FD_TEST( ctx->wb_kick_cnt==3UL && ctx->wb_wait_cnt==1UL && ctx->wb_pending==8192UL );

  /* Sub-kick run accumulates without kicking... */
  worker_buffer_write( ctx, 1UL<<20, data, 100UL );
  worker_buffer_flush( ctx );
  FD_TEST( ctx->wb_kick_cnt==3UL && ctx->wb_run_off==(1UL<<20) && ctx->wb_run_sz==100UL );

  /* ...and a discontinuity (partition rotation) kicks the partial run. */
  worker_buffer_write( ctx, 2UL<<20, data, 100UL );
  worker_buffer_flush( ctx );
  FD_TEST( ctx->wb_kick_cnt==4UL && ctx->wb_run_off==(2UL<<20) && ctx->wb_run_sz==100UL );
  FD_TEST( ctx->wb_pending<=ctx->wb_window );

  worker_reset_write_engine( ctx );
  FD_TEST( !ctx->wb_pending && ctx->wb_head==ctx->wb_tail && !ctx->wb_run_sz );

  /* Disabled window: no tracking. */
  ctx->wb_window = 0UL;
  worker_buffer_write( ctx, 0UL, data, 4096UL );
  worker_buffer_flush( ctx );
  FD_TEST( !ctx->wb_kick_cnt && !ctx->wb_pending );
}

/* Coordinator passthrough: an appendvec tar header becomes an ASSIGN on
   the least-loaded worker's ring carrying the inline coverage; a region
   header broadcasts the watermark immediately; tar EOF broadcasts EOS. */
static void
test_coordinator_assign_flow( void ) {
  static uchar lane_mem[ 4096UL ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( lane_mem, 0, sizeof(lane_mem) );

  fd_snapin_tile_t ctx[1];
  coordinator_io_ctx_init( ctx, 1UL, FD_SNAPSHOT_STATE_PROCESSING );
  fd_ssparse_init( ctx->ssparse );
  ctx->in[ 0 ].wksp   = (fd_wksp_t *)lane_mem;
  ctx->in[ 0 ].chunk0 = 0UL;
  ctx->in[ 0 ].wmark  = 0UL;
  ctx->in[ 0 ].mtu    = 4096UL;
  ctx->worker_assigned_bytes[ 1 ] = 5000UL; /* worker 0 is least loaded */

  test_parser_script   = 6;
  test_parser_call_cnt = 0UL;
  test_pub_cnt         = 0UL;

  /* Frag 1: appendvec tar header (script consumes the whole frag). */
  ulong ctl = fd_frag_meta_ctl( 0UL, 0, 0, 0 );
  FD_TEST( !returnable_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_DATA, 0UL, 512UL, ctl, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_out_idx[ 0 ]==10UL && test_pub_sig[ 0 ]==FD_SNAPIN_IO_KIND_ASSIGN );
  fd_snapin_io_job_t const * assign = (fd_snapin_io_job_t const *)test_ring_mem[ 0 ];
  FD_TEST( assign->kind==FD_SNAPIN_IO_KIND_ASSIGN );
  FD_TEST( assign->generation==1UL );
  FD_TEST( assign->appendvec_idx==0UL );
  FD_TEST( assign->slot==42UL );
  FD_TEST( assign->body_off==512UL );
  FD_TEST( assign->body_sz==1000UL );
  FD_TEST( assign->covered_until==512UL+1024UL ); /* align512(1000) */
  FD_TEST( ctx->worker_assigned_bytes[ 0 ]==1000UL );
  FD_TEST( ctx->appendvec_seq==1UL );
  FD_TEST( ctx->covered_until==1536UL );
  FD_TEST( ctx->io_watermark_dirty );
  FD_TEST( ctx->av_stats.cnt==1UL && ctx->av_stats.max_sz==1000UL );

  /* Frag 2: region tar header -> immediate watermark broadcast through
     the region end (workers skip the manifest at line rate). */
  FD_TEST( !returnable_frag( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_DATA, 0UL, 512UL, ctl, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
  FD_TEST( test_pub_cnt==3UL );
  FD_TEST( test_pub_out_idx[ 1 ]==10UL && test_pub_sig[ 1 ]==FD_SNAPIN_IO_KIND_WATERMARK );
  FD_TEST( test_pub_out_idx[ 2 ]==11UL && test_pub_sig[ 2 ]==FD_SNAPIN_IO_KIND_WATERMARK );
  FD_TEST( ctx->covered_until==1024UL+1024UL ); /* region body at 1024, align512(600) */
  FD_TEST( !ctx->io_watermark_dirty );
  fd_snapin_io_job_t const * wm = (fd_snapin_io_job_t const *)test_ring_mem[ 1 ];
  FD_TEST( wm->kind==FD_SNAPIN_IO_KIND_WATERMARK && wm->covered_until==2048UL );

  /* Frag 3: tar EOF -> EOS broadcast, state FINISHING. */
  FD_TEST( !returnable_frag( ctx, 0UL, 2UL, FD_SNAPSHOT_MSG_DATA, 0UL, 1UL, ctl, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
  FD_TEST( test_pub_cnt==5UL );
  FD_TEST( test_pub_sig[ 3 ]==FD_SNAPIN_IO_KIND_EOS && test_pub_sig[ 4 ]==FD_SNAPIN_IO_KIND_EOS );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
  FD_TEST( ctx->stream_cursor==1025UL );
}

/* Watermark broadcast cadence: the periodic returnable-frag tail path,
   the idle after_credit path, and their gating on pending controls and
   the ERROR state. */
static void
test_coordinator_watermark_protocol( void ) {
  fd_snapin_tile_t ctx[1];
  coordinator_io_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_PROCESSING );
  ctx->generation = 21UL;

  ctx->covered_until      = 4096UL;
  ctx->io_watermark_dirty = 1;
  test_pub_cnt = 0UL;
  publish_watermarks( ctx, (fd_stem_context_t *)1UL );
  FD_TEST( test_pub_cnt==2UL );
  FD_TEST( test_pub_out_idx[0]==10UL && test_pub_out_idx[1]==11UL );
  FD_TEST( !ctx->io_watermark_dirty && !ctx->io_frags_since_watermark );
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_snapin_io_job_t const * job = (fd_snapin_io_job_t const *)test_ring_mem[ i ];
    FD_TEST( job->kind==FD_SNAPIN_IO_KIND_WATERMARK );
    FD_TEST( job->generation==21UL );
    FD_TEST( job->covered_until==4096UL );
  }

  /* after_credit only emits when the watermark advanced AND no ring
     publish happened since the last check (idle deadlock fix). */
  int poll_in = 1; int charge_busy = 0;
  ctx->covered_until        = 8192UL;
  ctx->io_watermark_dirty   = 1;
  ctx->io_jobs_since_credit = 1UL; /* publishes flowed: workers see coverage soon enough */
  test_pub_cnt = 0UL;
  after_credit( ctx, (fd_stem_context_t *)1UL, &poll_in, &charge_busy );
  FD_TEST( !test_pub_cnt && !charge_busy );
  FD_TEST( !ctx->io_jobs_since_credit );
  after_credit( ctx, (fd_stem_context_t *)1UL, &poll_in, &charge_busy );
  FD_TEST( test_pub_cnt==2UL && charge_busy );
  FD_TEST( ((fd_snapin_io_job_t const *)test_ring_mem[ 0 ])->covered_until==8192UL );

  /* While a control awaits worker acks, after_credit must not emit. */
  ctx->io_watermark_dirty     = 1;
  ctx->io_jobs_since_credit   = 0UL;
  ctx->pending_worker_control = FD_SNAPSHOT_MSG_CTRL_FINI;
  test_pub_cnt = 0UL;
  after_credit( ctx, (fd_stem_context_t *)1UL, &poll_in, &charge_busy );
  FD_TEST( !test_pub_cnt );

  /* Nor in the ERROR state (workers drain regardless after ABORT). */
  ctx->pending_worker_control = ULONG_MAX;
  ctx->state                  = FD_SNAPSHOT_STATE_ERROR;
  after_credit( ctx, (fd_stem_context_t *)1UL, &poll_in, &charge_busy );
  FD_TEST( !test_pub_cnt );
}

/* The scratch footprint is computed from a zero base, so it only bounds
   the runtime layout if no FD_LAYOUT_APPEND alignment exceeds
   scratch_align() (the alignment the topology places the tile object
   at).  A larger member alignment overflows the object into the next
   workspace object at runtime — with >=2 workers the worker write
   buffer clobbered the adjacent worker tile's ctx (role/lane_cnt),
   crashing before_frag with a division by zero.  Replay both roles'
   layouts from a worst-case base: aligned to scratch_align() but to
   nothing larger.  Keep the appends in sync with scratch_footprint /
   unprivileged_init. */
static void
test_snapin_scratch_layout_fits( void ) {
  fd_topo_tile_t tile = {0};
  tile.snapin.max_live_slots = 1024UL;

  ulong align = scratch_align();
  ulong base  = 3UL*align; /* aligned to scratch_align() only */

  /* Worker role */
  tile.kind_id = 1UL;
  {
    FD_SCRATCH_ALLOC_INIT( l, (void *)base );
    FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_snapin_tile_t),     sizeof(fd_snapin_tile_t)                          );
    FD_SCRATCH_ALLOC_APPEND( l, fd_accdb_align(),              fd_accdb_footprint( tile.snapin.max_live_slots )  );
    FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_snapin_extent_t),   FD_SNAPIN_FIFO_CNT*sizeof(fd_snapin_extent_t)     );
    FD_SCRATCH_ALLOC_APPEND( l, 4096UL,                        FD_SNAPIN_WRITE_BUF_SZ                            );
    ulong end = FD_SCRATCH_ALLOC_FINI( l, 1UL );
    FD_TEST( end-base<=scratch_footprint( &tile ) );
  }

  /* Coordinator role */
  tile.kind_id = 0UL;
  {
    FD_SCRATCH_ALLOC_INIT( l, (void *)base );
    FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_snapin_tile_t),     sizeof(fd_snapin_tile_t)                                    );
    FD_SCRATCH_ALLOC_APPEND( l, fd_txncache_align(),           fd_txncache_footprint( tile.snapin.max_live_slots )         );
    FD_SCRATCH_ALLOC_APPEND( l, fd_accdb_align(),              fd_accdb_footprint( tile.snapin.max_live_slots )            );
    FD_SCRATCH_ALLOC_APPEND( l, fd_ssmanifest_parser_align(),  fd_ssmanifest_parser_footprint()                            );
    FD_SCRATCH_ALLOC_APPEND( l, fd_slot_delta_parser_align(),  fd_slot_delta_parser_footprint()                            );
    FD_SCRATCH_ALLOC_APPEND( l, alignof(blockhash_group_t),    sizeof(blockhash_group_t)*FD_SNAPIN_MAX_SLOT_DELTA_GROUPS   );
    FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_sstxncache_hash_t), sizeof(fd_sstxncache_hash_t)*FD_SNAPIN_TXNCACHE_MAX_ENTRIES );
    ulong end = FD_SCRATCH_ALLOC_FINI( l, 1UL );
    FD_TEST( end-base<=scratch_footprint( &tile ) );
  }
}

/* Worker before_frag dispatch across every in_idx/sig class, in the
   2-worker topology shape (snapin_io ring at in_idx 0, snapdc lanes at
   in_idx 1..8): the job ring is always processed; lanes follow the same
   expected-frame rotation and control-barrier discipline as the
   coordinator; in the ERROR state everything but FAIL is dropped
   (drained) so the retry can flow. */
static void
test_snapin_worker_before_frag_dispatch( void ) {
  fd_snapin_tile_t ctx[ 1 ];
  fd_memset( ctx, 0, sizeof(*ctx) );
  ctx->role            = FD_SNAPIN_ROLE_ACCDB_WORKER;
  ctx->worker_idx      = 1UL;
  ctx->state           = FD_SNAPSHOT_STATE_PROCESSING;
  ctx->lane_cnt        = 8UL;
  ctx->job_in_idx      = 0UL;
  ctx->pending_control = ULONG_MAX;
  ctx->in_lane[ 0 ]    = ULONG_MAX;
  for( ulong lane=0UL; lane<8UL; lane++ ) ctx->in_lane[ 1UL+lane ] = lane;
  ctx->expected_frame  = 3UL;

  static const ulong sigs[ 6 ] = {
    FD_SNAPSHOT_MSG_DATA,           FD_SNAPSHOT_MSG_CTRL_INIT_FULL, FD_SNAPSHOT_MSG_CTRL_FINI,
    FD_SNAPSHOT_MSG_CTRL_ERROR,     FD_SNAPSHOT_MSG_CTRL_FAIL,      FD_SNAPSHOT_MSG_CTRL_SHUTDOWN,
  };
  for( ulong s=0UL; s<6UL; s++ ) {
    /* Job ring: always processed, whatever the sig or seq. */
    FD_TEST( before_frag( ctx, 0UL, 0UL,       sigs[ s ] )==0 );
    FD_TEST( before_frag( ctx, 0UL, ULONG_MAX, sigs[ s ] )==0 );
  }

  /* Data frags only from the expected lane */
  FD_TEST( before_frag( ctx, 1UL+3UL, 0UL, FD_SNAPSHOT_MSG_DATA )== 0 );
  FD_TEST( before_frag( ctx, 1UL+4UL, 0UL, FD_SNAPSHOT_MSG_DATA )==-1 );

  /* Controls pass when no barrier is pending */
  FD_TEST( before_frag( ctx, 1UL+5UL, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI )==0 );

  /* Once a lane contributed to a pending barrier, its later frags hold */
  ctx->pending_control   = FD_SNAPSHOT_MSG_CTRL_FINI;
  ctx->control_seen[ 2 ] = 1U;
  FD_TEST( before_frag( ctx, 1UL+2UL, 0UL, FD_SNAPSHOT_MSG_DATA )==-1 );
  FD_TEST( before_frag( ctx, 1UL+3UL, 0UL, FD_SNAPSHOT_MSG_DATA )== 0 );
  ctx->pending_control   = ULONG_MAX;
  ctx->control_seen[ 2 ] = 0U;

  /* ERROR state: drop (drain) everything except FAIL; job ring still
     polled so ABORT/stale jobs keep draining. */
  ctx->state = FD_SNAPSHOT_STATE_ERROR;
  FD_TEST( before_frag( ctx, 1UL+3UL, 0UL, FD_SNAPSHOT_MSG_DATA      )==1 );
  FD_TEST( before_frag( ctx, 1UL+3UL, 0UL, FD_SNAPSHOT_MSG_CTRL_FINI )==1 );
  FD_TEST( before_frag( ctx, 1UL+3UL, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL )==0 );
  FD_TEST( before_frag( ctx, 0UL,     0UL, FD_SNAPIN_IO_KIND_ABORT   )==0 );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  /* The worker write engine pwrites to the fixed FD_ACCDB_FD_RW
     descriptor; back it with a memfd for the staging tests. */
  int mfd = memfd_create( "snapin_test_accdb", 0U );
  FD_TEST( mfd>=0 );
  FD_TEST( dup2( mfd, FD_ACCDB_FD_RW )==FD_ACCDB_FD_RW );

  test_control_barriers();
  test_all_control_barriers_and_final_payload();
  test_fast_lane_control_pipeline();
  test_pending_control_allows_lagging_data();
  test_pending_control_keeps_frame_order();
  test_error_interrupts_incremental_init();
  test_partial_fail_survives_error();
  test_fail_supersedes_pending_controls();
  test_initialized_incremental_fail_rolls_back();
  test_error_fail_and_retry();
  test_frame_ordering();
  test_frame_owner_and_raw_lane();
  test_partial_and_zero_byte_eom();
  test_malformed_stream_endings();
  test_init_resets_lane_state();
  test_nonempty_raw_data();
  test_batch_stake_delegation();
  test_streaming_stake_delegation();
  test_txncache_staging_entry_size();
  test_txncache_staging_evicts_oldest_slot();
  test_txncache_staging_fits_one_gigantic_page();
  test_txncache_staging_validates_stale_group_offsets();
  test_snapin_scratch_layout_fits();
  test_snapin_worker_before_frag_dispatch();
  test_worker_coverage_hold();
  test_worker_owned_appendvec();
  test_worker_batch_staging();
  test_worker_stale_generation();
  test_worker_fini_deferred_ack();
  test_worker_fail_cancels_pending_fini();
  test_worker_abort_ring_bypass();
  test_worker_error_mid_init_barrier();
  test_coordinator_ack_gating();
  test_coordinator_worker_error_ack();
  test_coordinator_eq_slot_malform();
  test_coordinator_early_ack_hold();
  test_coordinator_full_lifecycle_8_workers();
  test_coordinator_assign_flow();
  test_coordinator_watermark_protocol();
  test_worker_write_behind();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
