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
static ulong test_accdb_worker_cnt_arg;
static ulong test_accdb_worker_chain_idxs[ 8 ];
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

static void
mock_accdb_snapshot_route_batch( fd_accdb_t const * accdb,
                                 ulong              cnt,
                                 uchar const * const pubkeys[],
                                 ulong              worker_cnt,
                                 ulong              worker_idxs[],
                                 ulong              chain_idxs[] );

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
#define fd_accdb_snapshot_prefetch_chain_batch mock_accdb_snapshot_prefetch_chain_batch
#define fd_accdb_snapshot_route_batch mock_accdb_snapshot_route_batch
#define fd_accdb_snapshot_writer_begin mock_accdb_snapshot_writer_begin
#define fd_accdb_snapshot_writer_end mock_accdb_snapshot_writer_end
#define fd_accdb_snapshot_write_batch_worker mock_accdb_snapshot_write_batch_worker
#define fd_accdb_snapshot_worker_close mock_accdb_snapshot_worker_close
#define fd_accdb_snapshot_verify_readback mock_accdb_snapshot_verify_readback
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
#undef fd_accdb_snapshot_write_batch_worker
#undef fd_accdb_snapshot_worker_close
#undef fd_accdb_snapshot_verify_readback
#undef fd_accdb_snapshot_writer_end
#undef fd_accdb_snapshot_writer_begin
#undef fd_accdb_snapshot_route_batch
#undef fd_accdb_snapshot_prefetch_chain_batch
#undef fd_accdb_snapshot_prefetch_batch

#include <stdlib.h>

static void
mock_accdb_snapshot_route_batch( fd_accdb_t const * accdb,
                                 ulong              cnt,
                                 uchar const * const pubkeys[],
                                 ulong              worker_cnt,
                                 ulong              worker_idxs[],
                                 ulong              chain_idxs[] ) {
  (void)accdb;
  FD_TEST( cnt && cnt<=8UL );
  FD_TEST( fd_ulong_is_pow2( worker_cnt ) );
  for( ulong i=0UL; i<cnt; i++ ) {
    chain_idxs [ i ] = (ulong)pubkeys[ i ][ 0 ];
    worker_idxs[ i ] = chain_idxs[ i ] & (worker_cnt-1UL);
  }
}

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

void
mock_accdb_snapshot_prefetch_chain_batch( fd_accdb_t * accdb,
                                          ulong        cnt,
                                          ulong const  chain_idxs[] ) {
  (void)accdb;
  (void)cnt;
  (void)chain_idxs;
}

/* Mock explicit-offset worker writer: records what the worker read
   from the held frag bytes, allocates sequentially from a cursor for
   accepted entries, and reports skip_mask entries as ignored (no
   allocation) and repl_mask entries as replaced. */
int
mock_accdb_snapshot_write_batch_worker( fd_accdb_t *                accdb,
                                        ulong                       cnt,
                                        uchar const * const         pubkeys[],
                                        ulong const                 chain_idxs[],
                                        ulong                       slot,
                                        ulong const                 lamports[],
                                        ulong const                 data_lens[],
                                        int const                   executables[],
                                        fd_accdb_snapshot_whead_t * whead,
                                        ulong                       file_offsets[],
                                        ulong *                     accounts_ignored,
                                        ulong *                     accounts_replaced,
                                        ulong *                     accounts_loaded,
                                        ulong *                     out_replaced_lamports,
                                        ulong *                     out_ignored_lamports ) {
  (void)accdb;
  (void)whead;
  FD_TEST( cnt && cnt<=8UL );
  test_accdb_worker_call_cnt++;
  test_accdb_worker_slot    = slot;
  test_accdb_worker_cnt_arg = cnt;
  *accounts_ignored = *accounts_replaced = *accounts_loaded = 0UL;
  *out_replaced_lamports = *out_ignored_lamports = 0UL;
  for( ulong i=0UL; i<cnt; i++ ) {
    test_accdb_worker_chain_idxs[ i ] = chain_idxs[ i ];
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
  return 0;
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

static int
test_ssparse_advance( fd_ssparse_t *                parser,
                      uchar const *                 data,
                      ulong                         data_sz,
                      fd_ssparse_advance_result_t * result ) {
  (void)parser;
  FD_TEST( test_parser_script>=1 && test_parser_script<=3 );
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

/* Write one fd_ssparse batch entry (136-byte header + data) at off in
   lane_mem.  Returns the offset one past the (unpadded) entry. */
static ulong
test_lane_entry( uchar * lane_mem,
                 ulong   off,
                 uchar   pubkey_b,
                 ulong   lamports,
                 ulong   data_len,
                 int     executable,
                 uchar   data_b ) {
  uchar * e = lane_mem+off;
  FD_STORE( ulong, e+FD_SNAPIN_IO_ENT_DATA_LEN_OFF, data_len );
  fd_memset( e+FD_SNAPIN_IO_ENT_PUBKEY_OFF, pubkey_b, 32UL );
  FD_STORE( ulong, e+FD_SNAPIN_IO_ENT_LAMPORTS_OFF, lamports );
  fd_memset( e+FD_SNAPIN_IO_ENT_OWNER_OFF, (uchar)(pubkey_b+1), 32UL );
  e[ FD_SNAPIN_IO_ENT_EXEC_OFF ] = (uchar)executable;
  fd_memset( e+FD_SNAPIN_IO_ENT_DATA_OFF, data_b, data_len );
  return off+FD_SNAPIN_IO_ENT_DATA_OFF+data_len;
}

static void
test_worker_setup( fd_snapin_tile_t * ctx,
                   uchar *            job_mem,
                   uchar *            ack_mem,
                   uchar *            lane_mem,
                   ulong              lane_mtu,
                   uchar *            write_buf ) {
  fd_memset( ctx, 0, sizeof(*ctx) );
  ctx->role         = FD_SNAPIN_ROLE_ACCDB_WORKER;
  ctx->worker_idx   = 0UL;
  ctx->state        = FD_SNAPSHOT_STATE_IDLE;
  ctx->job_in_idx   = 0UL;
  ctx->in_lane[ 0 ] = ULONG_MAX;
  ctx->in_lane[ 1 ] = 0UL;
  ctx->io_in[ 0 ].wksp   = (fd_wksp_t *)job_mem;
  ctx->io_in[ 0 ].chunk0 = 0UL;
  ctx->io_in[ 0 ].wmark  = 0UL;
  ctx->io_in[ 0 ].mtu    = FD_SNAPIN_IO_JOB_SLOT_SZ;
  ctx->lane_cnt        = 1UL;
  ctx->in[ 0 ].wksp   = (fd_wksp_t *)lane_mem;
  ctx->in[ 0 ].chunk0 = 0UL;
  ctx->in[ 0 ].wmark  = 0UL;
  ctx->in[ 0 ].mtu    = lane_mtu;
  ctx->release[ 0 ]   = ULONG_MAX;
  ctx->write_buf      = write_buf;
  ctx->ack_out.idx    = 3UL;
  ctx->ack_out.mem    = (fd_wksp_t *)ack_mem;
  ctx->ack_out.chunk0 = 0UL;
  ctx->ack_out.wmark  = 0UL;
  ctx->ack_out.chunk  = 0UL;
  ctx->ack_out.mtu    = FD_SNAPIN_IO_ACK_SLOT_SZ;
}

static uchar test_worker_write_buf[ FD_SNAPIN_WRITE_BUF_SZ ] __attribute__((aligned(4096)));

static void
test_snapin_worker_protocol( void ) {
  static uchar job_mem [ FD_SNAPIN_IO_JOB_SLOT_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  static uchar ack_mem [ FD_SNAPIN_IO_ACK_SLOT_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  static uchar lane_mem[ 4096UL ]                   __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( job_mem,  0, sizeof(job_mem)  );
  fd_memset( ack_mem,  0, sizeof(ack_mem)  );
  fd_memset( lane_mem, 0, sizeof(lane_mem) );

  fd_snapin_tile_t ctx[ 1 ];
  test_worker_setup( ctx, job_mem, ack_mem, lane_mem, sizeof(lane_mem), test_worker_write_buf );

  /* Three batch entries in the held lane frag: 5-byte data, 0-byte
     data, 12-byte data (offsets 8-byte aligned like the parser). */
  ulong e0 = 0UL;
  ulong e1 = fd_ulong_align_up( test_lane_entry( lane_mem, e0, 0xA1, 10UL, 5UL, 0, 0x51 ), 8UL );
  ulong e2 = fd_ulong_align_up( test_lane_entry( lane_mem, e1, 0xB2, 20UL, 0UL, 1, 0x00 ), 8UL );
  ulong e3 = fd_ulong_align_up( test_lane_entry( lane_mem, e2, 0xC3, 30UL, 12UL, 0, 0x52 ), 8UL );
  /* Slow-path data run for the HEADER account. */
  ulong dr = e3;
  fd_memset( lane_mem+dr, 0x53, 8UL );

  fd_snapin_io_job_t * job = (fd_snapin_io_job_t *)job_mem;
  job->kind       = FD_SNAPIN_IO_KIND_CTRL;
  job->worker_idx = 0UL;
  job->generation = 9UL;
  job->control    = FD_SNAPSHOT_MSG_CTRL_INIT_FULL;
  for( ulong i=0UL; i<FD_SNAPIN_IO_LANE_MAX; i++ ) job->frontier[ i ] = ULONG_MAX;
  test_pub_cnt = 0UL;
  test_accdb_writer_begin_cnt = 0UL;
  test_accdb_writer_end_cnt   = 0UL;
  test_accdb_worker_close_cnt = 0UL;
  worker_handle_frag( ctx, 0UL, sizeof(*job), (fd_stem_context_t *)1UL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );
  FD_TEST( ctx->generation==9UL );
  FD_TEST( test_accdb_writer_begin_cnt==1UL );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[0]==fd_snapin_io_ack_sig( 9UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL ) );

  /* BATCH: worker reads pubkey/lamports/exec/data from the held frag,
     entry 0 ignored (no allocation), entry 1 replaced, entry 2 loaded. */
  fd_memset( job, 0, sizeof(*job) );
  job->kind       = FD_SNAPIN_IO_KIND_BATCH;
  job->worker_idx = 0UL;
  job->generation = 9UL;
  job->cnt        = 3UL;
  job->slot       = 440123518UL;
  job->fork_id    = USHORT_MAX;
  job->lane       = 0UL;
  job->seq        = 5UL;
  job->chunk      = 0UL;
  job->ent_off[0] = (uint)e0; job->data_len[0] =  5U;
  job->ent_off[1] = (uint)e1; job->data_len[1] =  0U;
  job->ent_off[2] = (uint)e2; job->data_len[2] = 12U;
  fd_snapin_io_job_set_chain_idx( job, 0UL, 10UL );
  fd_snapin_io_job_set_chain_idx( job, 1UL, 20UL );
  fd_snapin_io_job_set_chain_idx( job, 2UL, 30UL );
  test_accdb_worker_call_cnt  = 0UL;
  test_accdb_worker_next_off  = 4096UL;
  test_accdb_worker_skip_mask = 1UL; /* entry 0 ignored */
  test_accdb_worker_repl_mask = 2UL; /* entry 1 replaced */
  worker_handle_frag( ctx, 0UL, sizeof(*job), (fd_stem_context_t *)1UL );
  FD_TEST( test_accdb_worker_call_cnt==1UL );
  FD_TEST( test_accdb_worker_cnt_arg==3UL );
  FD_TEST( test_accdb_worker_slot==440123518UL );
  FD_TEST( test_accdb_worker_chain_idxs[0]==10UL && test_accdb_worker_chain_idxs[1]==20UL && test_accdb_worker_chain_idxs[2]==30UL );
  FD_TEST( test_accdb_worker_pubkeys[0][0]==0xA1 && test_accdb_worker_pubkeys[1][0]==0xB2 && test_accdb_worker_pubkeys[2][0]==0xC3 );
  FD_TEST( test_accdb_worker_lamports[0]==10UL && test_accdb_worker_lamports[1]==20UL && test_accdb_worker_lamports[2]==30UL );
  FD_TEST( test_accdb_worker_data_lens[0]==5UL && test_accdb_worker_data_lens[1]==0UL && test_accdb_worker_data_lens[2]==12UL );
  FD_TEST( !test_accdb_worker_execs[0] && test_accdb_worker_execs[1] && !test_accdb_worker_execs[2] );
  FD_TEST( ctx->metrics.accounts_ignored==1UL );
  FD_TEST( ctx->metrics.accounts_replaced==1UL );
  FD_TEST( ctx->metrics.accounts_loaded==1UL );
  FD_TEST( ctx->metrics.total_accounts_processed==3UL );
  FD_TEST( ctx->worker.input_lamports==60UL );
  FD_TEST( ctx->worker.replaced_lamports==20UL );
  FD_TEST( ctx->worker.ignored_lamports==10UL );

  /* Slow path: HEADER by value, then a DATA frag ref.  Loaded at the
     mock's cursor (4096+72+0 + 72+12 = 4252). */
  fd_memset( job, 0, sizeof(*job) );
  job->kind       = FD_SNAPIN_IO_KIND_HEADER;
  job->worker_idx = 0UL;
  job->generation = 9UL;
  job->cnt        = 1UL;
  job->slot       = 440123518UL;
  job->fork_id    = USHORT_MAX;
  fd_memset( job->hdr.pubkey, 0xD4, 32UL );
  fd_memset( job->hdr.owner,  0xD5, 32UL );
  job->hdr.lamports   = 40UL;
  job->hdr.data_len   = 8UL;
  job->hdr.chain_idx  = 40UL;
  job->hdr.executable = 0;
  test_accdb_worker_skip_mask = 0UL;
  test_accdb_worker_repl_mask = 0UL;
  worker_handle_frag( ctx, 0UL, sizeof(*job), (fd_stem_context_t *)1UL );
  FD_TEST( ctx->open_acc.active && ctx->open_acc.accepted );
  FD_TEST( ctx->open_acc.data_len==8UL && !ctx->open_acc.received );
  FD_TEST( ctx->metrics.accounts_loaded==2UL );
  FD_TEST( ctx->worker.input_lamports==100UL );

  fd_memset( job, 0, sizeof(*job) );
  job->kind       = FD_SNAPIN_IO_KIND_DATA;
  job->worker_idx = 0UL;
  job->generation = 9UL;
  job->lane       = 0UL;
  job->seq        = 6UL;
  job->chunk      = 0UL;
  job->off        = (uint)dr;
  job->sz         = 8U;
  worker_handle_frag( ctx, 0UL, sizeof(*job), (fd_stem_context_t *)1UL );
  FD_TEST( !ctx->open_acc.active );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );

  /* FRONTIER releases the held lanes up to seq 6: before_frag consumes
     (filters) released frags and defers newer ones. */
  FD_TEST( before_frag( ctx, 1UL, 6UL, FD_SNAPSHOT_MSG_DATA )==-1 );
  fd_memset( job, 0, sizeof(*job) );
  job->kind       = FD_SNAPIN_IO_KIND_FRONTIER;
  job->worker_idx = 0UL;
  job->generation = 9UL;
  for( ulong i=0UL; i<FD_SNAPIN_IO_LANE_MAX; i++ ) job->frontier[ i ] = ULONG_MAX;
  job->frontier[ 0 ] = 6UL;
  worker_handle_frag( ctx, 0UL, sizeof(*job), (fd_stem_context_t *)1UL );
  FD_TEST( ctx->release[ 0 ]==6UL );
  FD_TEST( before_frag( ctx, 1UL, 6UL, FD_SNAPSHOT_MSG_DATA )==1  );
  FD_TEST( before_frag( ctx, 1UL, 7UL, FD_SNAPSHOT_MSG_DATA )==-1 );
  FD_TEST( before_frag( ctx, 0UL, 7UL, FD_SNAPSHOT_MSG_DATA )==0  ); /* job ring always processed */
  /* Stale frontiers never move release backwards. */
  job->frontier[ 0 ] = 4UL;
  worker_handle_frag( ctx, 0UL, sizeof(*job), (fd_stem_context_t *)1UL );
  FD_TEST( ctx->release[ 0 ]==6UL );

  /* FINI: flush staging, close the private partition, writer_end, ack
     with the folded counters.  The staged bytes must be readable at the
     explicit offsets. */
  fd_memset( job, 0, sizeof(*job) );
  job->kind       = FD_SNAPIN_IO_KIND_CTRL;
  job->worker_idx = 0UL;
  job->generation = 9UL;
  job->control    = FD_SNAPSHOT_MSG_CTRL_FINI;
  for( ulong i=0UL; i<FD_SNAPIN_IO_LANE_MAX; i++ ) job->frontier[ i ] = ULONG_MAX;
  job->frontier[ 0 ] = 6UL;
  test_pub_cnt = 0UL;
  worker_handle_frag( ctx, 0UL, sizeof(*job), (fd_stem_context_t *)1UL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
  FD_TEST( test_accdb_writer_end_cnt==1UL );
  FD_TEST( test_accdb_worker_close_cnt==1UL );
  FD_TEST( !ctx->write_buf_used );
  fd_snapin_io_ack_t const * ack = (fd_snapin_io_ack_t const *)ack_mem;
  FD_TEST( ack->worker_idx==0UL );
  FD_TEST( ack->accounts_ignored==1UL );
  FD_TEST( ack->accounts_replaced==1UL );
  FD_TEST( ack->accounts_loaded==2UL );
  FD_TEST( ack->input_lamports==100UL );
  FD_TEST( ack->replaced_lamports==20UL );
  FD_TEST( ack->ignored_lamports==10UL );

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

  job->control = FD_SNAPSHOT_MSG_CTRL_DONE;
  worker_handle_frag( ctx, 0UL, sizeof(*job), (fd_stem_context_t *)1UL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  job->control = FD_SNAPSHOT_MSG_CTRL_SHUTDOWN;
  worker_handle_frag( ctx, 0UL, sizeof(*job), (fd_stem_context_t *)1UL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_SHUTDOWN );
}

/* A job referencing a frag at or below the released watermark violates
   the frag-lifetime guarantee (nothing holds those bytes): the worker
   must fail the attempt with EPROTO instead of reading them. */
static void
test_snapin_worker_ref_guard( void ) {
  static uchar job_mem [ FD_SNAPIN_IO_JOB_SLOT_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  static uchar ack_mem [ FD_SNAPIN_IO_ACK_SLOT_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  static uchar lane_mem[ 4096UL ]                   __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( job_mem,  0, sizeof(job_mem)  );
  fd_memset( ack_mem,  0, sizeof(ack_mem)  );
  fd_memset( lane_mem, 0, sizeof(lane_mem) );

  fd_snapin_tile_t ctx[ 1 ];
  test_worker_setup( ctx, job_mem, ack_mem, lane_mem, sizeof(lane_mem), test_worker_write_buf );
  ctx->state      = FD_SNAPSHOT_STATE_PROCESSING;
  ctx->generation = 3UL;
  ctx->release[ 0 ] = 6UL;
  test_lane_entry( lane_mem, 0UL, 0xA1, 10UL, 5UL, 0, 0x51 );

  fd_snapin_io_job_t * job = (fd_snapin_io_job_t *)job_mem;
  job->kind       = FD_SNAPIN_IO_KIND_BATCH;
  job->worker_idx = 0UL;
  job->generation = 3UL;
  job->cnt        = 1UL;
  job->slot       = 1UL;
  job->fork_id    = USHORT_MAX;
  job->lane       = 0UL;
  job->seq        = 6UL; /* <= release: bytes are no longer held */
  job->chunk      = 0UL;
  job->ent_off[0] = 0U;
  job->data_len[0] = 5U;
  test_pub_cnt = 0UL;
  test_accdb_worker_call_cnt = 0UL;
  worker_handle_frag( ctx, 0UL, sizeof(*job), (fd_stem_context_t *)1UL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( !test_accdb_worker_call_cnt ); /* never touched the index */
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[0]==fd_snapin_io_ack_sig( 3UL, FD_SNAPSHOT_MSG_CTRL_ERROR ) );
  fd_snapin_io_ack_t const * ack = (fd_snapin_io_ack_t const *)ack_mem;
  FD_TEST( ack->err==EPROTO );
}

static void
test_snapin_coordinator_worker_handoff( void ) {
  static uchar job_mem [ FD_SNAPIN_IO_JOB_SLOT_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  static uchar ack_mem [ FD_SNAPIN_IO_ACK_SLOT_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  static uchar frag_mem[ 1024UL ]                   __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( job_mem,  0, sizeof(job_mem)  );
  fd_memset( ack_mem,  0, sizeof(ack_mem)  );
  fd_memset( frag_mem, 0, sizeof(frag_mem) );

  fd_snapin_tile_t ctx[ 1 ];
  fd_memset( ctx, 0, sizeof(*ctx) );
  ctx->role       = FD_SNAPIN_ROLE_COORDINATOR;
  ctx->io_enabled = 1;
  ctx->worker_cnt = 1UL;
  ctx->generation = 12UL;
  ctx->full       = 1;
  ctx->io_out[ 0 ].idx    = 2UL;
  ctx->io_out[ 0 ].mem    = (fd_wksp_t *)job_mem;
  ctx->io_out[ 0 ].chunk0 = 0UL;
  ctx->io_out[ 0 ].wmark  = 0UL;
  ctx->io_out[ 0 ].chunk  = 0UL;
  ctx->io_out[ 0 ].mtu    = FD_SNAPIN_IO_JOB_SLOT_SZ;

  /* Jobs pin (lane,seq,chunk) refs into the frag being parsed. */
  ctx->cur_frag.lane  = 0UL;
  ctx->cur_frag.seq   = 7UL;
  ctx->cur_frag.chunk = 5UL;
  ctx->cur_frag.base  = frag_mem;

  uchar pubkey[ 3 ][ 32 ] = { { 1U }, { 2U }, { 3U } };
  uchar const * entries[ 3 ] = { frag_mem+0UL, frag_mem+200UL, frag_mem+400UL };
  uchar const * pubkeys[ 3 ] = { pubkey[0], pubkey[1], pubkey[2] };
  ulong const data_lens[ 3 ] = { 100UL, 200UL, 300UL };
  test_pub_cnt = 0UL;
  FD_TEST( dispatch_account_batch( ctx, entries, 3UL, pubkeys, 440123518UL,
                                   data_lens, (fd_stem_context_t *)1UL )==0 );
  FD_TEST( test_pub_cnt==0UL );
  publish_all_pending_jobs( ctx, (fd_stem_context_t *)1UL );
  FD_TEST( test_pub_cnt==1UL && test_pub_sig[0]==FD_SNAPSHOT_MSG_DATA );
  fd_snapin_io_job_t const * job = (fd_snapin_io_job_t const *)job_mem;
  FD_TEST( job->kind==FD_SNAPIN_IO_KIND_BATCH );
  FD_TEST( job->worker_idx==0UL );
  FD_TEST( job->generation==12UL );
  FD_TEST( job->cnt==3UL );
  FD_TEST( job->slot==440123518UL );
  FD_TEST( job->fork_id==USHORT_MAX );
  FD_TEST( job->lane==0UL && job->seq==7UL && job->chunk==5UL );
  FD_TEST( job->ent_off[0]==0U && job->ent_off[1]==200U && job->ent_off[2]==400U );
  FD_TEST( job->data_len[0]==100U && job->data_len[1]==200U && job->data_len[2]==300U );
  FD_TEST( fd_snapin_io_job_chain_idx( job, 0UL )==1UL &&
           fd_snapin_io_job_chain_idx( job, 1UL )==2UL &&
           fd_snapin_io_job_chain_idx( job, 2UL )==3UL );

  ctx->io_in[ 0 ].wksp   = (fd_wksp_t *)ack_mem;
  ctx->io_in[ 0 ].chunk0 = 0UL;
  ctx->io_in[ 0 ].wmark  = 0UL;
  ctx->io_in[ 0 ].mtu    = FD_SNAPIN_IO_ACK_SLOT_SZ;
  ctx->ct_out.idx   = 4UL;
  ctx->pending_worker_control = FD_SNAPSHOT_MSG_CTRL_FINI;
  ctx->capitalization         = 100UL;
  ctx->dup_capitalization     = 5UL;
  fd_snapin_io_ack_t * ack = (fd_snapin_io_ack_t *)ack_mem;
  ack->worker_idx         = 0UL;
  ack->generation        = 12UL;
  ack->control           = FD_SNAPSHOT_MSG_CTRL_FINI;
  ack->accounts_ignored  = 1UL;
  ack->accounts_replaced = 1UL;
  ack->accounts_loaded   = 1UL;
  ack->input_lamports    = 60UL;
  ack->replaced_lamports = 11UL;
  ack->ignored_lamports  = 7UL;
  test_pub_cnt = 0UL;
  coordinator_handle_ack( ctx, (fd_stem_context_t *)1UL,
                          0UL,
                          fd_snapin_io_ack_sig( 12UL, FD_SNAPSHOT_MSG_CTRL_FINI ),
                          0UL, sizeof(*ack) );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( ctx->metrics.accounts_ignored==1UL );
  FD_TEST( ctx->metrics.accounts_replaced==1UL );
  FD_TEST( ctx->metrics.accounts_loaded==1UL );
  FD_TEST( ctx->capitalization==153UL );
  FD_TEST( ctx->dup_capitalization==16UL );
  FD_TEST( test_pub_cnt==1UL && test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_FINI );
}

static void
test_snapin_multiworker_routing_and_barrier( void ) {
  static uchar job_mem[ 2 ][ FD_SNAPIN_IO_JOB_SLOT_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  static uchar ack_mem[ 2 ][ FD_SNAPIN_IO_ACK_SLOT_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  static uchar frag_mem[ 1024UL ]                       __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( job_mem,  0, sizeof(job_mem)  );
  fd_memset( ack_mem,  0, sizeof(ack_mem)  );
  fd_memset( frag_mem, 0, sizeof(frag_mem) );

  fd_snapin_tile_t ctx[ 1 ];
  fd_memset( ctx, 0, sizeof(*ctx) );
  ctx->role       = FD_SNAPIN_ROLE_COORDINATOR;
  ctx->io_enabled = 1;
  ctx->worker_cnt = 2UL;
  ctx->generation = 21UL;
  ctx->full       = 1;
  for( ulong i=0UL; i<2UL; i++ ) {
    ctx->io_out[ i ].idx    = 10UL+i;
    ctx->io_out[ i ].mem    = (fd_wksp_t *)job_mem[ i ];
    ctx->io_out[ i ].chunk0 = 0UL;
    ctx->io_out[ i ].wmark  = 0UL;
    ctx->io_out[ i ].chunk  = 0UL;
    ctx->io_out[ i ].mtu    = FD_SNAPIN_IO_JOB_SLOT_SZ;
  }

  ctx->cur_frag.lane  = 1UL;
  ctx->cur_frag.seq   = 9UL;
  ctx->cur_frag.chunk = 2UL;
  ctx->cur_frag.base  = frag_mem;

  uchar pubkey[ 4 ][ 32 ] = { { 0U }, { 1U }, { 2U }, { 3U } };
  uchar const * entries[ 4 ] = { frag_mem+0UL, frag_mem+200UL, frag_mem+400UL, frag_mem+600UL };
  uchar const * pubkeys[ 4 ] = { pubkey[0], pubkey[1], pubkey[2], pubkey[3] };
  ulong const data_lens[ 4 ] = { 100UL, 200UL, 300UL, 400UL };
  test_pub_cnt = 0UL;
  FD_TEST( dispatch_account_batch( ctx, entries, 4UL, pubkeys, 440123518UL,
                                   data_lens, (fd_stem_context_t *)1UL )==0 );
  FD_TEST( test_pub_cnt==0UL );
  publish_all_pending_jobs( ctx, (fd_stem_context_t *)1UL );
  FD_TEST( test_pub_cnt==2UL );
  FD_TEST( test_pub_out_idx[0]==10UL && test_pub_out_idx[1]==11UL );

  fd_snapin_io_job_t const * even = (fd_snapin_io_job_t const *)job_mem[ 0 ];
  fd_snapin_io_job_t const * odd  = (fd_snapin_io_job_t const *)job_mem[ 1 ];
  FD_TEST( even->worker_idx==0UL && even->cnt==2UL );
  FD_TEST( odd ->worker_idx==1UL && odd ->cnt==2UL );
  FD_TEST( even->lane==1UL && even->seq==9UL && even->chunk==2UL );
  FD_TEST( odd ->lane==1UL && odd ->seq==9UL && odd ->chunk==2UL );
  FD_TEST( even->ent_off[0]==0U   && even->ent_off[1]==400U );
  FD_TEST( odd ->ent_off[0]==200U && odd ->ent_off[1]==600U );
  FD_TEST( even->data_len[0]==100U && even->data_len[1]==300U );
  FD_TEST( odd ->data_len[0]==200U && odd ->data_len[1]==400U );
  FD_TEST( fd_snapin_io_job_chain_idx( even, 0UL )==0UL && fd_snapin_io_job_chain_idx( even, 1UL )==2UL );
  FD_TEST( fd_snapin_io_job_chain_idx( odd,  0UL )==1UL && fd_snapin_io_job_chain_idx( odd,  1UL )==3UL );

  for( ulong i=0UL; i<2UL; i++ ) {
    ctx->io_in[ i ].wksp   = (fd_wksp_t *)ack_mem[ i ];
    ctx->io_in[ i ].chunk0 = 0UL;
    ctx->io_in[ i ].wmark  = 0UL;
    ctx->io_in[ i ].mtu    = FD_SNAPIN_IO_ACK_SLOT_SZ;
  }
  ctx->ct_out.idx               = 17UL;
  ctx->pending_worker_control   = FD_SNAPSHOT_MSG_CTRL_FINI;
  ctx->pending_worker_ack_mask  = 0UL;
  ctx->capitalization           = 100UL;
  ctx->dup_capitalization       = 5UL;

  fd_snapin_io_ack_t * ack0 = (fd_snapin_io_ack_t *)ack_mem[ 0 ];
  ack0->worker_idx         = 0UL;
  ack0->generation         = 21UL;
  ack0->control            = FD_SNAPSHOT_MSG_CTRL_FINI;
  ack0->accounts_ignored   = 1UL;
  ack0->accounts_loaded    = 1UL;
  ack0->input_lamports     = 10UL;
  ack0->ignored_lamports   = 2UL;

  fd_snapin_io_ack_t * ack1 = (fd_snapin_io_ack_t *)ack_mem[ 1 ];
  ack1->worker_idx          = 1UL;
  ack1->generation          = 21UL;
  ack1->control             = FD_SNAPSHOT_MSG_CTRL_FINI;
  ack1->accounts_replaced   = 1UL;
  ack1->accounts_loaded     = 1UL;
  ack1->input_lamports      = 20UL;
  ack1->replaced_lamports   = 3UL;
  ack1->ignored_lamports    = 4UL;

  ulong sig = fd_snapin_io_ack_sig( 21UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  test_pub_cnt = 0UL;
  coordinator_handle_ack( ctx, (fd_stem_context_t *)1UL, 0UL, sig, 0UL, sizeof(*ack0) );
  FD_TEST( test_pub_cnt==0UL );
  FD_TEST( ctx->pending_worker_control==FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->pending_worker_ack_mask==1UL );
  coordinator_handle_ack( ctx, (fd_stem_context_t *)1UL, 1UL, sig, 0UL, sizeof(*ack1) );
  FD_TEST( test_pub_cnt==1UL && test_pub_out_idx[0]==17UL && test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctx->pending_worker_control==ULONG_MAX );
  FD_TEST( ctx->pending_worker_ack_mask==0UL );
  FD_TEST( ctx->metrics.accounts_ignored==1UL );
  FD_TEST( ctx->metrics.accounts_replaced==1UL );
  FD_TEST( ctx->metrics.accounts_loaded==2UL );
  FD_TEST( ctx->capitalization==124UL );
  FD_TEST( ctx->dup_capitalization==8UL );

  /* A failed full attempt must not reset shared accdb state until all
     workers have drained preceding jobs and acknowledged FAIL. */
  ctx->pending_worker_control  = FD_SNAPSHOT_MSG_CTRL_FAIL;
  ctx->pending_worker_ack_mask = 0UL;
  ctx->init_completed          = 1;
  ctx->full                    = 1;
  ack0->control                = FD_SNAPSHOT_MSG_CTRL_FAIL;
  ack1->control                = FD_SNAPSHOT_MSG_CTRL_FAIL;
  test_accdb_reset_cnt         = 0UL;
  test_pub_cnt                 = 0UL;
  sig = fd_snapin_io_ack_sig( 21UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  coordinator_handle_ack( ctx, (fd_stem_context_t *)1UL, 0UL, sig, 0UL, sizeof(*ack0) );
  FD_TEST( test_accdb_reset_cnt==0UL && ctx->init_completed );
  coordinator_handle_ack( ctx, (fd_stem_context_t *)1UL, 1UL, sig, 0UL, sizeof(*ack1) );
  FD_TEST( test_accdb_reset_cnt==1UL && !ctx->init_completed );
  FD_TEST( test_pub_cnt==1UL && test_pub_out_idx[0]==17UL && test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_FAIL );
}

/* Coordinator side of the frontier protocol: watermarks only advance at
   frag boundaries, FRONTIER jobs go to every worker ring on the
   consumed-frag interval and from the idle after_credit path, and every
   CTRL job carries the frontier. */
static void
test_snapin_frontier_protocol( void ) {
  static uchar job_mem[ 2 ][ FD_SNAPIN_IO_JOB_SLOT_SZ ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  fd_memset( job_mem, 0, sizeof(job_mem) );

  fd_snapin_tile_t ctx[ 1 ];
  fd_memset( ctx, 0, sizeof(*ctx) );
  ctx->role       = FD_SNAPIN_ROLE_COORDINATOR;
  ctx->io_enabled = 1;
  ctx->worker_cnt = 2UL;
  ctx->generation = 21UL;
  ctx->lane_cnt   = 2UL;
  ctx->pending_worker_control = ULONG_MAX;
  for( ulong i=0UL; i<FD_SNAPIN_IO_LANE_MAX; i++ ) ctx->lane_consumed_seq[ i ] = ULONG_MAX;
  for( ulong i=0UL; i<2UL; i++ ) {
    ctx->io_out[ i ].idx    = 10UL+i;
    ctx->io_out[ i ].mem    = (fd_wksp_t *)job_mem[ i ];
    ctx->io_out[ i ].chunk0 = 0UL;
    ctx->io_out[ i ].wmark  = 0UL;
    ctx->io_out[ i ].chunk  = 0UL;
    ctx->io_out[ i ].mtu    = FD_SNAPIN_IO_JOB_SLOT_SZ;
  }

  coordinator_mark_lane_consumed( ctx, 0UL, 41UL );
  coordinator_mark_lane_consumed( ctx, 1UL, 7UL );
  FD_TEST( ctx->io_frontier_dirty && ctx->io_frags_since_frontier==2UL );

  test_pub_cnt = 0UL;
  publish_frontier_jobs( ctx, (fd_stem_context_t *)1UL );
  FD_TEST( test_pub_cnt==2UL );
  FD_TEST( test_pub_out_idx[0]==10UL && test_pub_out_idx[1]==11UL );
  FD_TEST( !ctx->io_frontier_dirty && !ctx->io_frags_since_frontier );
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_snapin_io_job_t const * job = (fd_snapin_io_job_t const *)job_mem[ i ];
    FD_TEST( job->kind==FD_SNAPIN_IO_KIND_FRONTIER );
    FD_TEST( job->worker_idx==i );
    FD_TEST( job->generation==21UL );
    FD_TEST( job->frontier[ 0 ]==41UL && job->frontier[ 1 ]==7UL );
    for( ulong l=2UL; l<FD_SNAPIN_IO_LANE_MAX; l++ ) FD_TEST( job->frontier[ l ]==ULONG_MAX );
  }

  /* after_credit only emits when the frontier advanced AND no account
     job was published since the last check (idle deadlock fix). */
  int poll_in = 1; int charge_busy = 0;
  coordinator_mark_lane_consumed( ctx, 1UL, 8UL );
  ctx->io_jobs_since_credit = 1UL; /* jobs flowed: workers will see a ring frontier soon enough */
  test_pub_cnt = 0UL;
  after_credit( ctx, (fd_stem_context_t *)1UL, &poll_in, &charge_busy );
  FD_TEST( !test_pub_cnt && !charge_busy );
  FD_TEST( !ctx->io_jobs_since_credit );
  after_credit( ctx, (fd_stem_context_t *)1UL, &poll_in, &charge_busy );
  FD_TEST( test_pub_cnt==2UL && charge_busy );
  fd_snapin_io_job_t const * job0 = (fd_snapin_io_job_t const *)job_mem[ 0 ];
  FD_TEST( job0->kind==FD_SNAPIN_IO_KIND_FRONTIER && job0->frontier[ 1 ]==8UL );

  /* Every CTRL job carries the frontier too. */
  coordinator_mark_lane_consumed( ctx, 0UL, 42UL );
  test_pub_cnt = 0UL;
  publish_worker_control( ctx, (fd_stem_context_t *)1UL, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( test_pub_cnt==2UL );
  FD_TEST( ctx->pending_worker_control==FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( !ctx->io_frontier_dirty && !ctx->io_frags_since_frontier );
  fd_snapin_io_job_t const * ctrl = (fd_snapin_io_job_t const *)job_mem[ 1 ];
  FD_TEST( ctrl->kind==FD_SNAPIN_IO_KIND_CTRL );
  FD_TEST( ctrl->control==FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( ctrl->frontier[ 0 ]==42UL && ctrl->frontier[ 1 ]==8UL );

  /* While a control awaits worker acks, after_credit must not emit. */
  coordinator_mark_lane_consumed( ctx, 0UL, 43UL );
  test_pub_cnt = 0UL;
  after_credit( ctx, (fd_stem_context_t *)1UL, &poll_in, &charge_busy );
  FD_TEST( !test_pub_cnt );
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
  test_snapin_worker_protocol();
  test_snapin_worker_ref_guard();
  test_snapin_coordinator_worker_handoff();
  test_snapin_multiworker_routing_and_barrier();
  test_snapin_frontier_protocol();
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
