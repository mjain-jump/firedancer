/* Unit tests for the N symmetric fused snapin tiles of the parallel
   (tar-boundary-sharded) snapshot loader.

   There is no coordinator and no message-passing protocol between the
   tiles, so the harness models a cluster directly: N real
   fd_snapin_tile_t contexts sharing one real snapin_shmem object, each
   driven frag by frag through returnable_frag/before_frag.  The accdb,
   ssparse and stem entry points the tile calls out to are mocked, so
   what these tests pin is the tile's own protocol: the attempt-slot
   gate, the eager-claim counter, the per-attempt resets, the FINI
   malform gates and the FINI->totals fold. */

#define _GNU_SOURCE
#include "../../disco/stem/fd_stem.h"
#include "../../flamenco/accdb/fd_accdb_base.h"
#include "../../flamenco/runtime/fd_txncache.h"
#include "utils/fd_ssparse.h"
#include "utils/fd_slot_delta_parser.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

struct test_io_step {
  long result;
  int  err;
};

typedef struct test_io_step test_io_step_t;

static test_io_step_t test_pwrite_steps[ 16UL ];
static ulong test_pwrite_step_cnt;
static ulong test_pwrite_step_idx;
static ulong test_pwrite_call_cnt;

static void
test_io_reset( void ) {
  test_pwrite_step_cnt = 0UL;
  test_pwrite_step_idx = 0UL;
  test_pwrite_call_cnt = 0UL;
}

static void
test_pwrite_push( long result,
                  int  err ) {
  FD_TEST( test_pwrite_step_cnt<16UL );
  test_pwrite_steps[ test_pwrite_step_cnt++ ] = (test_io_step_t){ result, err };
}

static long
test_pwrite( int          fd,
             void const * buf,
             ulong        sz,
             long         off ) {
  (void)fd;
  (void)buf;
  (void)off;
  test_pwrite_call_cnt++;
  if( test_pwrite_step_idx>=test_pwrite_step_cnt ) return (long)sz;
  test_io_step_t step = test_pwrite_steps[ test_pwrite_step_idx++ ];
  if( step.result<0L ) errno = step.err;
  return step.result;
}

/* Recorded stem publishes. */
static ulong test_pub_sig[ 64UL ];
static ulong test_pub_out_idx[ 64UL ];
static ulong test_pub_cnt;

/* Mock accdb call counters. */
static ulong test_accdb_reset_cnt;
static ulong test_accdb_attach_cnt;
static ulong test_accdb_purge_cnt;
static int   test_parser_script;
static ulong test_parser_call_cnt;
static ulong test_accdb_advance_root_cnt;
static ulong test_accdb_writer_begin_cnt;
static ulong test_accdb_writer_end_cnt;
static ulong test_accdb_worker_close_cnt;
static ulong test_accdb_load_begin_cnt;
static ulong test_accdb_load_end_cnt;
static ulong test_accdb_readback_cnt;
static ulong test_accdb_recover_delta_cnt;
static ulong test_accdb_release_cnt;      /* release_partitions calls */
static ulong test_accdb_release_total;    /* partitions released */
static ulong test_feature_finalize_cnt;
static ulong test_appendvec_parse_cnt;

/* Mock ssparse stream: test_av_cnt appendvec entries, then DONE.  Every
   tile walks the same stream independently, so the position is
   per-tile and the driver stamps test_cur_tile before each frag. */
#define TEST_TILE_MAX (9UL)
#define TEST_AV_MAX   (32UL)

static ulong test_av_cnt;
static ulong test_av_sz[ TEST_AV_MAX ];
static ulong test_stream_pos[ TEST_TILE_MAX ];
static ulong test_cur_tile;

static int
test_ssparse_advance( fd_ssparse_t *                parser,
                      uchar const *                 data,
                      ulong                         data_sz,
                      fd_ssparse_advance_result_t *  result );

static void
test_ssparse_appendvec_parse( fd_ssparse_t * parser );

void
mock_txncache_reset( fd_txncache_t * txncache );

void *
mock_txncache_snapin_scratch( fd_txncache_t * txncache,
                              ulong *         out_sz );

void
mock_slot_delta_parser_init( fd_slot_delta_parser_t * parser );

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

#define fd_accdb_reset                               mock_accdb_reset
#define fd_accdb_attach_child                        mock_accdb_attach_child
#define fd_accdb_purge                               mock_accdb_purge
#define fd_accdb_advance_root                        mock_accdb_advance_root
#define fd_accdb_snapshot_writer_begin               mock_accdb_snapshot_writer_begin
#define fd_accdb_snapshot_writer_end                 mock_accdb_snapshot_writer_end
#define fd_accdb_snapshot_load_begin                 mock_accdb_snapshot_load_begin
#define fd_accdb_snapshot_load_end                   mock_accdb_snapshot_load_end
#define fd_accdb_snapshot_worker_close               mock_accdb_snapshot_worker_close
#define fd_accdb_snapshot_flush_worker_metrics       mock_accdb_snapshot_flush_worker_metrics
#define fd_accdb_snapshot_verify_readback            mock_accdb_snapshot_verify_readback
#define fd_accdb_snapshot_recover_delta              mock_accdb_snapshot_recover_delta
#define fd_accdb_snapshot_worker_release_partitions  mock_accdb_snapshot_worker_release_partitions
#define fd_accdb_snapshot_write_batch_worker         mock_accdb_snapshot_write_batch_worker
#define fd_txncache_reset                            mock_txncache_reset
#define fd_txncache_snapin_scratch                   mock_txncache_snapin_scratch
#define fd_ssmanifest_parser_init                    mock_ssmanifest_parser_init
#define fd_slot_delta_parser_init                    mock_slot_delta_parser_init
#define fd_stake_delegations_reset                   mock_stake_delegations_reset
#define fd_feature_snoop_finalize                    mock_feature_snoop_finalize
#define fd_stem_publish                              test_stem_publish
#define fd_ssparse_advance                           test_ssparse_advance
#define fd_ssparse_appendvec_parse                   test_ssparse_appendvec_parse
#define pwrite                                       test_pwrite
#include "fd_snapin_tile.c"
#undef pwrite
#undef fd_ssparse_appendvec_parse
#undef fd_ssparse_advance
#undef fd_stem_publish
#undef fd_feature_snoop_finalize
#undef fd_stake_delegations_reset
#undef fd_slot_delta_parser_init
#undef fd_ssmanifest_parser_init
#undef fd_txncache_snapin_scratch
#undef fd_txncache_reset
#undef fd_accdb_snapshot_write_batch_worker
#undef fd_accdb_snapshot_worker_release_partitions
#undef fd_accdb_snapshot_recover_delta
#undef fd_accdb_snapshot_verify_readback
#undef fd_accdb_snapshot_flush_worker_metrics
#undef fd_accdb_snapshot_worker_close
#undef fd_accdb_snapshot_load_end
#undef fd_accdb_snapshot_load_begin
#undef fd_accdb_snapshot_writer_end
#undef fd_accdb_snapshot_writer_begin
#undef fd_accdb_advance_root
#undef fd_accdb_purge
#undef fd_accdb_attach_child
#undef fd_accdb_reset

#include <stdlib.h>

/* Production per-slot limits (tile->snapin.max_txn_per_slot and its
   derived staging bounds). */
#define TEST_MAX_GROUPS_PER_SLOT  (FD_MAX_TXN_PER_SLOT)
#define TEST_MAX_ENTRIES_PER_SLOT (2UL*FD_MAX_TXN_PER_SLOT)
#define TEST_MAX_STAGED_GROUPS    (FD_TXNCACHE_MAX_SLOT_DELTAS*TEST_MAX_GROUPS_PER_SLOT)
#define TEST_MAX_ENTRIES          (FD_TXNCACHE_MAX_SLOT_DELTAS*TEST_MAX_ENTRIES_PER_SLOT)

static snapin_writer_t test_writer[1];

/* Mocks ***************************************************************/

void mock_accdb_reset                            ( fd_accdb_t * accdb ) { (void)accdb; test_accdb_reset_cnt++;         }
void mock_accdb_snapshot_writer_begin            ( fd_accdb_t * accdb ) { (void)accdb; test_accdb_writer_begin_cnt++;  }
void mock_accdb_snapshot_writer_end              ( fd_accdb_t * accdb ) { (void)accdb; test_accdb_writer_end_cnt++;    }
void mock_accdb_snapshot_load_end                ( fd_accdb_t * accdb ) { (void)accdb; test_accdb_load_end_cnt++;      }

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
mock_accdb_advance_root( fd_accdb_t *       accdb,
                         fd_accdb_fork_id_t fork_id ) {
  (void)accdb;
  (void)fork_id;
  test_accdb_advance_root_cnt++;
}

void mock_accdb_snapshot_load_begin( fd_accdb_t * accdb ) { (void)accdb; test_accdb_load_begin_cnt++; }

/* The real close hands off the final partition and resets the whead.
   The tile reads whead.attempt_partition_cnt right after (to stamp its
   FAIL partition list), so the mock deliberately leaves the tracker
   count alone: the driver seeds it to model a writer that acquired
   partitions during the attempt. */
void
mock_accdb_snapshot_worker_close( fd_accdb_t *                accdb,
                                  fd_accdb_snapshot_whead_t * whead ) {
  (void)accdb;
  whead->val           = 0UL;
  whead->has_partition = 0;
  test_accdb_worker_close_cnt++;
}

/* Mirrors the real function: folds (and zeroes) the shared-counter
   deltas, leaves the eq_slot_* diagnostics untouched. */
void
mock_accdb_snapshot_flush_worker_metrics( fd_accdb_t *                         accdb,
                                          fd_accdb_snapshot_worker_metrics_t * m ) {
  (void)accdb;
  m->disk_used_added      = 0UL;
  m->disk_used_removed    = 0UL;
  m->accounts_total_added = 0UL;
}

void
mock_accdb_snapshot_verify_readback( fd_accdb_t * accdb,
                                     ulong        sample_max ) {
  (void)accdb;
  (void)sample_max;
  test_accdb_readback_cnt++;
}

int
mock_accdb_snapshot_recover_delta( fd_accdb_t *       accdb,
                                   fd_accdb_fork_id_t fork_id ) {
  (void)accdb;
  (void)fork_id;
  test_accdb_recover_delta_cnt++;
  return 0;
}

void
mock_accdb_snapshot_worker_release_partitions( fd_accdb_t * accdb,
                                               uint const * partition_idxs,
                                               ulong        cnt ) {
  (void)accdb;
  (void)partition_idxs;
  test_accdb_release_cnt++;
  test_accdb_release_total += cnt;
}

int
mock_accdb_snapshot_write_batch_worker( fd_accdb_t *                         accdb,
                                        fd_accdb_fork_id_t                   fork_id,
                                        ulong                                cnt,
                                        uchar const * const                  pubkeys[],
                                        ulong                                slot,
                                        ulong const                          lamports[],
                                        ulong const                          data_lens[],
                                        int const                            executables[],
                                        int const                            snoop_candidates[],
                                        fd_accdb_snapshot_whead_t *          whead,
                                        int *                                stripe_locks,
                                        ulong                                stripe_msk,
                                        fd_accdb_snapshot_worker_metrics_t * metrics,
                                        ulong                                file_offsets[],
                                        ulong *                              accounts_ignored,
                                        ulong *                              accounts_replaced,
                                        ulong *                              accounts_loaded,
                                        ulong *                              out_replaced_lamports,
                                        ulong *                              out_ignored_lamports,
                                        fd_accdb_snapshot_snoop_fn_t         snoop_fn,
                                        void *                               snoop_ctx ) {
  (void)accdb;
  (void)fork_id;
  (void)pubkeys;
  (void)slot;
  (void)lamports;
  (void)data_lens;
  (void)executables;
  (void)whead;
  (void)stripe_locks;
  (void)stripe_msk;
  (void)metrics;
  *accounts_ignored      = 0UL;
  *accounts_replaced     = 0UL;
  *accounts_loaded       = cnt;
  *out_replaced_lamports = 0UL;
  *out_ignored_lamports  = 0UL;
  for( ulong i=0UL; i<cnt; i++ ) {
    file_offsets[ i ] = ULONG_MAX;
    if( snoop_fn && snoop_candidates && snoop_candidates[ i ] ) snoop_fn( snoop_ctx, i );
  }
  return 0;
}

void mock_txncache_reset( fd_txncache_t * tc ) { (void)tc; }

void *
mock_txncache_snapin_scratch( fd_txncache_t * txncache,
                              ulong *         out_sz ) {
  if( FD_UNLIKELY( !txncache ) ) {
    *out_sz = TEST_MAX_STAGED_GROUPS*sizeof(blockhash_group_t);
    return (void *)4096UL;
  }
  return fd_txncache_snapin_scratch( txncache, out_sz );
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

void mock_stake_delegations_reset( fd_stake_delegations_t * sd ) { (void)sd; }

void
mock_feature_snoop_finalize( fd_features_t *             features,
                             ulong                       slot,
                             fd_epoch_schedule_t const * epoch_schedule,
                             fd_feature_snoop_t const *  snoop ) {
  (void)features;
  (void)slot;
  (void)epoch_schedule;
  (void)snoop;
  test_feature_finalize_cnt++;
}

static void
test_ssparse_appendvec_parse( fd_ssparse_t * parser ) {
  (void)parser;
  test_appendvec_parse_cnt++;
}

static int
test_ssparse_advance( fd_ssparse_t *                parser,
                      uchar const *                 data,
                      ulong                         data_sz,
                      fd_ssparse_advance_result_t * result ) {
  (void)parser;
  if( FD_UNLIKELY( !test_parser_script ) ) {
    (void)data;
    fd_memset( result, 0, sizeof(*result) );
    result->bytes_consumed = data_sz;
    ulong i = test_stream_pos[ test_cur_tile ]++;
    if( FD_LIKELY( i<test_av_cnt ) ) {
      result->appendvec.slot    = 100UL+i;
      result->appendvec.data_sz = test_av_sz[ i ];
      return FD_SSPARSE_ADVANCE_APPENDVEC;
    }
    return FD_SSPARSE_ADVANCE_DONE;
  }

  FD_TEST( test_parser_script>=1 && test_parser_script<=4 );
  if( test_parser_script==4 ) {
    FD_TEST( !test_parser_call_cnt );
    fd_memset( result, 0, sizeof(*result) );
    result->bytes_consumed       = data_sz;
    result->status_cache.data    = data;
    result->status_cache.data_sz = data_sz;
    result->status_cache.done    = 1;
    test_parser_call_cnt++;
    return FD_SSPARSE_ADVANCE_STATUS_CACHE;
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
  static uchar shmem_mem[ sizeof(fd_snapin_shmem_t)
                        + 2UL*4096UL
                        + FD_SNAPIN_SHMEM_STRIPE_CNT*sizeof(int)
                        + sizeof(fd_snapin_shmem_worker_t) ] __attribute__((aligned(4096)));
  static uchar init_mem[ FD_TOPO_MAX_TILE_IN_LINKS ][ sizeof(fd_ssctrl_init_t) ] __attribute__((aligned(FD_CHUNK_ALIGN)));

  fd_memset( ctx, 0, sizeof(*ctx) );
  fd_memset( init_mem, 0, sizeof(init_mem) );
  FD_TEST( fd_snapin_shmem_footprint( 1UL )<=sizeof(shmem_mem) );
  ctx->shmem = fd_snapin_shmem_join( fd_snapin_shmem_new( shmem_mem, 1UL ) );
  FD_TEST( ctx->shmem );

  ctx->state        = state;
  ctx->full         = 1;
  ctx->tile_idx     = 0UL;
  ctx->tile_cnt     = 1UL;
  ctx->lane_cnt     = lane_cnt;
  ctx->ct_out.idx   = 0UL;
  ctx->stripe_locks = fd_snapin_shmem_stripes( ctx->shmem );
  ctx->shmem_worker = fd_snapin_shmem_worker( ctx->shmem, 0UL );
  ctx->lead.shmem_workers[ 0 ] = ctx->shmem_worker;

  ctx->whead.attempt_partitions    = ctx->shmem_worker->fail_partitions;
  ctx->whead.attempt_partition_max = FD_SNAPIN_SHMEM_PARTITION_MAX;
  writer_init( &ctx->writer, FD_ACCDB_FD_RW );
  worker_reset_attempt( ctx );
  clear_control_barrier( ctx );

  ctx->lead.accdb_root_fork_id = (fd_accdb_fork_id_t){ .val = USHORT_MAX };
  ctx->lead.accdb_incr_fork_id = (fd_accdb_fork_id_t){ .val = USHORT_MAX };
  ctx->lead.boot_timestamp     = fd_log_wallclock();
  ctx->lead.txncache_max_groups_per_slot  = TEST_MAX_GROUPS_PER_SLOT;
  ctx->lead.txncache_max_entries_per_slot = TEST_MAX_ENTRIES_PER_SLOT;
  ctx->lead.txncache_entries_max          = TEST_MAX_ENTRIES;

  for( ulong lane=0UL; lane<lane_cnt; lane++ ) {
    ctx->in[ lane ].wksp   = (fd_wksp_t *)init_mem[ lane ];
    ctx->in[ lane ].chunk0 = 0UL;
    ctx->in[ lane ].wmark  = 0UL;
    ctx->in[ lane ].mtu    = sizeof(fd_ssctrl_init_t);
  }
}

static void
send_control( fd_snapin_tile_t * ctx,
              ulong              lane,
              ulong              sig ) {
  FD_TEST( !returnable_frag( ctx, lane, 0UL, sig, 0UL, 0UL, 0UL, 0UL, 0UL,
                             (fd_stem_context_t *)1UL ) );
}

/* Cluster harness *****************************************************/

#define TEST_LANE_MAX (2UL)
#define TEST_FRAG_SZ  (4096UL)

typedef struct {
  fd_snapin_shmem_t * shmem;
  void *              shmem_mem;
  void *                  sd_mem;    /* tile 0's real slot delta parser */
  ulong                   tile_cnt;
  ulong                   lane_cnt;
  uchar *                 in_mem;    /* tile_cnt*lane_cnt frag buffers */
  fd_stake_delegations_t * stake_delegations;
  fd_bank_t *              bank;
  fd_snapin_tile_t        ctx[ TEST_TILE_MAX ];
} test_cluster_t;

static void
test_counters_reset( void ) {
  test_pub_cnt                  = 0UL;
  test_accdb_reset_cnt          = 0UL;
  test_accdb_attach_cnt         = 0UL;
  test_accdb_purge_cnt          = 0UL;
  test_accdb_advance_root_cnt   = 0UL;
  test_accdb_writer_begin_cnt   = 0UL;
  test_accdb_writer_end_cnt     = 0UL;
  test_accdb_worker_close_cnt   = 0UL;
  test_accdb_load_begin_cnt     = 0UL;
  test_accdb_load_end_cnt       = 0UL;
  test_accdb_readback_cnt       = 0UL;
  test_accdb_recover_delta_cnt  = 0UL;
  test_accdb_release_cnt        = 0UL;
  test_accdb_release_total      = 0UL;
  test_feature_finalize_cnt     = 0UL;
  test_appendvec_parse_cnt      = 0UL;
  test_parser_script            = 0;
  test_parser_call_cnt          = 0UL;
  for( ulong t=0UL; t<TEST_TILE_MAX; t++ ) test_stream_pos[ t ] = 0UL;
}

/* Build a cluster of tile_cnt symmetric snapin tiles sharing one real
   snapin_shmem object, wired the way unprivileged_init wires them. */
static test_cluster_t *
test_cluster_new( ulong tile_cnt,
                  ulong lane_cnt ) {
  FD_TEST( tile_cnt && tile_cnt<=TEST_TILE_MAX );
  FD_TEST( lane_cnt && lane_cnt<=TEST_LANE_MAX );

  test_cluster_t * cl = aligned_alloc( 4096UL, fd_ulong_align_up( sizeof(test_cluster_t), 4096UL ) );
  FD_TEST( cl );
  fd_memset( cl, 0, sizeof(test_cluster_t) );
  cl->tile_cnt = tile_cnt;
  cl->lane_cnt = lane_cnt;

  cl->shmem_mem = aligned_alloc( fd_snapin_shmem_align(), fd_ulong_align_up( fd_snapin_shmem_footprint( tile_cnt ), fd_snapin_shmem_align() ) );
  FD_TEST( cl->shmem_mem );
  cl->shmem = fd_snapin_shmem_join( fd_snapin_shmem_new( cl->shmem_mem, tile_cnt ) );
  FD_TEST( cl->shmem );

  cl->sd_mem = aligned_alloc( fd_slot_delta_parser_align(), fd_ulong_align_up( fd_slot_delta_parser_footprint(), fd_slot_delta_parser_align() ) );
  FD_TEST( cl->sd_mem );

  cl->in_mem = aligned_alloc( 4096UL, tile_cnt*lane_cnt*TEST_FRAG_SZ );
  FD_TEST( cl->in_mem );
  fd_memset( cl->in_mem, 0, tile_cnt*lane_cnt*TEST_FRAG_SZ );

  /* log_snoop_checksums walks the root stake delegation pool; a zeroed
     struct (pool_idx_wmk_==0) is an empty pool. */
  cl->stake_delegations = aligned_alloc( 128UL, fd_ulong_align_up( sizeof(fd_stake_delegations_t), 128UL ) );
  FD_TEST( cl->stake_delegations );
  fd_memset( cl->stake_delegations, 0, sizeof(fd_stake_delegations_t) );

  cl->bank = aligned_alloc( 128UL, fd_ulong_align_up( sizeof(fd_bank_t), 128UL ) );
  FD_TEST( cl->bank );
  fd_memset( cl->bank, 0, sizeof(fd_bank_t) );

  for( ulong t=0UL; t<tile_cnt; t++ ) {
    fd_snapin_tile_t * ctx = &cl->ctx[ t ];
    fd_memset( ctx, 0, sizeof(*ctx) );
    ctx->tile_idx = t;
    ctx->tile_cnt = tile_cnt;
    ctx->lane_cnt = lane_cnt;
    ctx->full     = 1;
    ctx->state    = FD_SNAPSHOT_STATE_IDLE;
    clear_control_barrier( ctx );

    ctx->shmem        = cl->shmem;
    ctx->stripe_locks = fd_snapin_shmem_stripes( cl->shmem );
    ctx->shmem_worker = fd_snapin_shmem_worker( cl->shmem, t );
    if( FD_UNLIKELY( !t ) ) {
      for( ulong w=0UL; w<tile_cnt; w++ ) ctx->lead.shmem_workers[ w ] = fd_snapin_shmem_worker( cl->shmem, w );
    }

    ctx->whead.attempt_partitions    = ctx->shmem_worker->fail_partitions;
    ctx->whead.attempt_partition_cnt = 0UL;
    ctx->whead.attempt_partition_max = FD_SNAPIN_SHMEM_PARTITION_MAX;

    ctx->stake_delegations = cl->stake_delegations;
    writer_init( &ctx->writer, FD_ACCDB_FD_RW );

    ctx->ct_out.idx            = 1UL+t;
    ctx->lead.manifest_out.idx = ULONG_MAX;
    ctx->lead.gui_out.idx      = ULONG_MAX;
    if( FD_UNLIKELY( !t ) ) {
      ctx->lead.manifest_out.idx  = 0UL;
      ctx->lead.bank              = cl->bank;
      ctx->lead.slot_delta_parser = fd_slot_delta_parser_join( fd_slot_delta_parser_new( cl->sd_mem ) );
      FD_TEST( ctx->lead.slot_delta_parser );
    }

    for( ulong lane=0UL; lane<lane_cnt; lane++ ) {
      ctx->in[ lane ].wksp   = (fd_wksp_t *)( cl->in_mem + (t*lane_cnt+lane)*TEST_FRAG_SZ );
      ctx->in[ lane ].chunk0 = 0UL;
      ctx->in[ lane ].wmark  = 0UL;
      ctx->in[ lane ].mtu    = TEST_FRAG_SZ;
      ctx->in[ lane ].pos    = 0UL;
      /* Control frags for tile 0 are read as an fd_ssctrl_init_t. */
      fd_ssctrl_init_t * msg = (fd_ssctrl_init_t *)( cl->in_mem + (t*lane_cnt+lane)*TEST_FRAG_SZ );
      msg->slot = 440123518UL;
    }

    ctx->lead.accdb_root_fork_id = (fd_accdb_fork_id_t){ .val = USHORT_MAX };
    ctx->lead.accdb_incr_fork_id = (fd_accdb_fork_id_t){ .val = USHORT_MAX };
    ctx->lead.boot_timestamp      = fd_log_wallclock();

    worker_reset_attempt( ctx );
  }

  return cl;
}

static void
test_cluster_delete( test_cluster_t * cl ) {
  free( cl->bank );
  free( cl->stake_delegations );
  free( cl->in_mem );
  free( cl->sd_mem );
  free( cl->shmem_mem );
  free( cl );
}

static void
tile_send_control( fd_snapin_tile_t * ctx,
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

/* Drive one control barrier to completion on every tile, in tile order.
   Tile order matters for INIT: tile 0 publishes the attempt slot the
   other tiles' gates hold on, and this harness is single-threaded (a
   non-tile-0-first order would just park every other tile's gate until
   tile 0's INIT ran). */
static void
cluster_barrier( test_cluster_t * cl,
                 ulong            sig ) {
  for( ulong t=0UL; t<cl->tile_cnt; t++ ) {
    for( ulong lane=0UL; lane<cl->lane_cnt; lane++ ) {
      tile_send_control( &cl->ctx[ t ], lane, sig );
    }
  }
}

static int
tile_send_data( fd_snapin_tile_t * ctx,
                ulong              lane,
                ulong              sz ) {
  ulong sig = FD_SNAPSHOT_MSG_DATA;
  ulong ctl = fd_frag_meta_ctl( 0UL, 0, 0, 0 );
  test_cur_tile = ctx->tile_idx;
  FD_TEST( !before_frag( ctx, lane, 0UL, sig ) );
  return returnable_frag( ctx, lane, 0UL, sig, 0UL, sz, ctl, 0UL, 0UL, (fd_stem_context_t *)1UL );
}

/* Feed one stream event to one tile and report which appendvec ordinal
   (if any) the tile took ownership of. */
static ulong
tile_step( fd_snapin_tile_t * ctx ) {
  ulong owned0 = ctx->owned_appendvecs;
  FD_TEST( !tile_send_data( ctx, 0UL, TEST_FRAG_SZ ) );
  if( FD_UNLIKELY( ctx->owned_appendvecs==owned0 ) ) return ULONG_MAX;
  FD_TEST( ctx->owned_appendvecs==owned0+1UL );
  return ctx->appendvec_seq-1UL;
}

/* Stream orders the eager-claim coverage test drives.  Ownership is
   schedule dependent (that is the point of the counter), the coverage
   invariant is not. */
#define TEST_ORDER_ROUND_ROBIN (0)
#define TEST_ORDER_TILE_MAJOR  (1)
#define TEST_ORDER_REVERSE     (2)

/* Walk every tile through the whole mock stream in the given order,
   recording the owner of each appendvec ordinal.  owner[] must hold
   test_av_cnt entries. */
static void
cluster_stream( test_cluster_t * cl,
                int              order,
                ulong *          owner ) {
  ulong n = cl->tile_cnt;
  ulong T = test_av_cnt;
  for( ulong i=0UL; i<T; i++ ) owner[ i ] = ULONG_MAX;

  /* T appendvec events plus one trailing event that yields DONE. */
  if( order==TEST_ORDER_ROUND_ROBIN ) {
    for( ulong step=0UL; step<T+1UL; step++ ) {
      for( ulong t=0UL; t<n; t++ ) {
        ulong av = tile_step( &cl->ctx[ t ] );
        if( av!=ULONG_MAX ) { FD_TEST( av<T && owner[ av ]==ULONG_MAX ); owner[ av ] = t; }
      }
    }
  } else {
    for( ulong j=0UL; j<n; j++ ) {
      ulong t = order==TEST_ORDER_REVERSE ? n-1UL-j : j;
      for( ulong step=0UL; step<T+1UL; step++ ) {
        ulong av = tile_step( &cl->ctx[ t ] );
        if( av!=ULONG_MAX ) { FD_TEST( av<T && owner[ av ]==ULONG_MAX ); owner[ av ] = t; }
      }
    }
  }

  for( ulong t=0UL; t<n; t++ ) {
    FD_TEST( cl->ctx[ t ].state==FD_SNAPSHOT_STATE_FINISHING );
    FD_TEST( cl->ctx[ t ].appendvec_seq==T );
  }
  for( ulong i=0UL; i<T; i++ ) FD_TEST( owner[ i ]!=ULONG_MAX ); /* every ordinal claimed exactly once */
}

static void
test_stream_init( ulong av_cnt ) {
  FD_TEST( av_cnt<=TEST_AV_MAX );
  test_av_cnt = av_cnt;
  for( ulong i=0UL; i<av_cnt; i++ ) test_av_sz[ i ] = 1024UL*(i+1UL);
}

/* A SlotHistory sysvar the winner-gated capture would have snooped:
   has_bits, 16384 blocks of zeroed bits, then (bits_len, next_slot).
   The tile's verify_slot_deltas_with_slot_history gate needs
   next_slot-1 == bank_slot, bits_len == FD_SLOT_HISTORY_MAX_ENTRIES, and
   (with an empty slot delta set) nothing else. */
static void
test_stamp_slot_history( test_cluster_t * cl,
                         ulong            bank_slot ) {
  fd_snapin_shmem_t * shmem = cl->shmem;
  ulong blocks_len = FD_SLOT_HISTORY_MAX_ENTRIES/64UL;
  FD_TEST( 9UL+blocks_len*8UL+16UL==FD_SYSVAR_SLOT_HISTORY_BINCODE_SZ );

  uchar * buf = shmem->slot_history.buf;
  fd_memset( buf, 0, FD_SYSVAR_SLOT_HISTORY_BINCODE_SZ );
  buf[ 0 ] = 1;
  FD_STORE( ulong, buf+1UL, blocks_len );
  uchar * footer = buf + 9UL + blocks_len*8UL;
  FD_STORE( ulong, footer,      FD_SLOT_HISTORY_MAX_ENTRIES );
  FD_STORE( ulong, footer+8UL,  bank_slot+1UL               );

  shmem->slot_history.captured   = 1;
  shmem->slot_history.executable = 0;
  shmem->slot_history.slot       = bank_slot;
  shmem->slot_history.lamports   = 1UL;
  shmem->slot_history.data_len   = FD_SYSVAR_SLOT_HISTORY_BINCODE_SZ;
  fd_memcpy( shmem->slot_history.owner, fd_sysvar_owner_id.uc, 32UL );

  cl->ctx[ 0 ].lead.bank_slot = bank_slot;
}

/* Regression: scratch_align() must cover the largest FD_LAYOUT_APPEND
   alignment in scratch_footprint. */

static void
test_scratch_layout_fits( void ) {
  FD_TEST( scratch_align()>=alignof(fd_snapin_tile_t) );
  FD_TEST( scratch_align()>=fd_accdb_align() );

  fd_topo_tile_t tile[1];
  memset( tile, 0, sizeof(fd_topo_tile_t) );
  tile->snapin.max_live_slots   = 1024UL;
  tile->snapin.max_txn_per_slot = FD_MAX_TXN_PER_SLOT;

  for( ulong kind_id=0UL; kind_id<2UL; kind_id++ ) {
    tile->kind_id = kind_id;
    ulong footprint = scratch_footprint( tile );

    FD_SCRATCH_ALLOC_INIT( l, NULL );
    FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_snapin_tile_t), sizeof(fd_snapin_tile_t) );
    FD_SCRATCH_ALLOC_APPEND( l, fd_accdb_align(),          fd_accdb_footprint( tile->snapin.max_live_slots ) );
    if( !kind_id ) {
      FD_SCRATCH_ALLOC_APPEND( l, fd_txncache_align(),               fd_txncache_footprint( tile->snapin.max_live_slots ) );
      FD_SCRATCH_ALLOC_APPEND( l, fd_ssmanifest_parser_align(),      fd_ssmanifest_parser_footprint()                     );
      FD_SCRATCH_ALLOC_APPEND( l, fd_slot_delta_parser_align(),      fd_slot_delta_parser_footprint()                     );
      FD_SCRATCH_ALLOC_APPEND( l, alignof(recent_blockhash_group_t), sizeof(recent_blockhash_group_t)*FD_SNAPIN_MAX_RECENT_GROUPS );
      FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_sstxncache_hash_t),     sizeof(fd_sstxncache_hash_t)*FD_TXNCACHE_MAX_SLOT_DELTAS*2UL*tile->snapin.max_txn_per_slot );
    }
    ulong end = FD_SCRATCH_ALLOC_FINI( l, scratch_align() );
    FD_TEST( end<=footprint );
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
  FD_TEST( !ctx->lead.advertised_slot );
  FD_TEST( !returnable_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_META, 0UL, sizeof(fd_ssctrl_meta_t),
                             0UL, 0UL, 0UL, (fd_stem_context_t *)1UL ) );
  FD_TEST( ctx->lead.advertised_slot==22UL );
  FD_TEST( !memcmp( ctx->lead.advertised_hash, meta[1].resolved_hash, FD_HASH_FOOTPRINT ) );
  FD_TEST( !test_pub_cnt );

  sync_ctx_init( ctx, 2UL, FD_SNAPSHOT_STATE_PROCESSING );
  ctx->lead.advertised_slot = 33UL;
  fd_memset( ctx->lead.advertised_hash, 0x33, FD_HASH_FOOTPRINT );
  for( ulong i=0UL; i<2UL; i++ ) {
    meta[i].resolved_slot = ULONG_MAX;
    fd_memcpy( meta_mem[i], &meta[i], sizeof(fd_ssctrl_meta_t) );
    ctx->in[i].wksp = (fd_wksp_t *)meta_mem[i];
    FD_TEST( !returnable_frag( ctx, i, 0UL, FD_SNAPSHOT_MSG_META, 0UL, sizeof(fd_ssctrl_meta_t),
                               0UL, 0UL, 0UL, (fd_stem_context_t *)1UL ) );
  }
  FD_TEST( ctx->lead.advertised_slot==33UL );
  uchar expected_hash[ FD_HASH_FOOTPRINT ];
  fd_memset( expected_hash, 0x33, sizeof(expected_hash) );
  FD_TEST( !memcmp( ctx->lead.advertised_hash, expected_hash, sizeof(expected_hash) ) );
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
  ctx->lead.accdb_root_fork_id  = (fd_accdb_fork_id_t){ .val = 3U };
  test_pub_cnt              = 0UL;
  test_accdb_reset_cnt      = 0UL;
  test_accdb_attach_cnt     = 0UL;
  test_accdb_purge_cnt      = 0UL;

  FD_TEST( !before_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR ) );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( ctx->pending_control==FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( ctx->control_seen[0] );
  FD_TEST( !ctx->control_seen[1] );
  FD_TEST( ctx->full );
  FD_TEST( !ctx->lead.init_completed );

  FD_TEST( !before_frag( ctx, 0UL, 1UL, FD_SNAPSHOT_MSG_CTRL_ERROR ) );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( !ctx->lead.init_completed );
  FD_TEST( ctx->pending_control==FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( ctx->control_seen[0] );
  FD_TEST( !ctx->control_seen[1] );
  FD_TEST( test_pub_cnt==1UL );
  FD_TEST( test_pub_sig[0]==FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( before_frag( ctx, 1UL, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR )>0 );

  FD_TEST( !before_frag( ctx, 0UL, 2UL, FD_SNAPSHOT_MSG_CTRL_FAIL ) );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( !ctx->lead.init_completed );
  FD_TEST( !test_accdb_reset_cnt );
  FD_TEST( !before_frag( ctx, 1UL, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL ) );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( !ctx->lead.init_completed );
  FD_TEST( !test_accdb_reset_cnt );
  FD_TEST( !test_accdb_attach_cnt );
  FD_TEST( !test_accdb_purge_cnt );
  FD_TEST( !ctx->lead.rollback.pending );
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
  ctx->lead.accdb_root_fork_id  = (fd_accdb_fork_id_t){ .val = 3U };
  test_pub_cnt              = 0UL;
  test_accdb_reset_cnt      = 0UL;
  test_accdb_attach_cnt     = 0UL;
  test_accdb_purge_cnt      = 0UL;

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );
  FD_TEST( ctx->lead.init_completed );
  FD_TEST( !ctx->full );
  FD_TEST( test_accdb_attach_cnt==1UL );
  FD_TEST( ctx->lead.accdb_incr_fork_id.val==7U );

  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR );
  send_control( ctx, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  send_control( ctx, 1UL, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( !ctx->lead.init_completed );
  FD_TEST( !test_accdb_reset_cnt );
  FD_TEST( !test_accdb_purge_cnt );
  FD_TEST( ctx->lead.rollback.pending );
  FD_TEST( !ctx->lead.rollback.full );
  FD_TEST( ctx->lead.rollback.fork.val==7U );
}

static void
test_error_fail_and_retry( void ) {
  fd_snapin_tile_t ctx[1];
  sync_ctx_init( ctx, 4UL, FD_SNAPSHOT_STATE_FINISHING );
  ctx->lead.init_completed = 1;
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
  /* The final loader defers rollback until the retry's INIT setup,
     after every FAIL ack has quiesced.  Run that lead-only setup here
     without publishing another control ack, which keeps this test's
     control-pipeline assertions focused on ERROR and FAIL. */
  tile0_init_attempt( ctx, 0UL, 0UL );
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

/* Attempt-slot gate ***************************************************/

/* A tile whose INIT barrier completes before tile 0 published the
   attempt slot must not insert: nothing else orders tile 0's INIT-time
   accdb work against this tile's first insert.  The gate is
   non-blocking, so the tile acks INIT immediately and then HOLDS its
   data lane (before_frag returns -1) without opening its writer or
   drawing a claim.  Once the slot carries this attempt's generation the
   next data frag opens the gate: the fork id is cached, the writer is
   opened and the eager claim is taken. */
static void
test_init_gate_holds_data( void ) {
  test_cluster_t * cl = test_cluster_new( 2UL, 1UL );
  test_counters_reset();
  test_stream_init( 4UL );

  fd_snapin_tile_t * ctx = &cl->ctx[ 1 ];

  /* Stale attempt slot: generation 0 while the tile will be on
     generation 1 (bumped at its first INIT frag).  The stale fork id is
     left plausible (USHORT_MAX, i.e. what a previous full attempt would
     have published) so that only the generation gate can stop the tile
     -- the fork-id sanity check must not be what saves us. */
  FD_TEST( cl->shmem->attempt.generation==0UL );
  cl->shmem->attempt.fork_id = (ulong)USHORT_MAX;

  for( ulong lane=0UL; lane<cl->lane_cnt; lane++ ) tile_send_control( ctx, lane, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );

  /* The INIT barrier completed and was acked, but the write path is
     armed, not open. */
  FD_TEST( ctx->generation==1UL );
  FD_TEST( ctx->state==FD_SNAPSHOT_STATE_PROCESSING );
  FD_TEST( ctx->gate_pending );
  FD_TEST( ctx->incr_fork==ULONG_MAX );
  FD_TEST( !test_accdb_writer_begin_cnt );
  FD_TEST( !cl->shmem->next_appendvec );
  FD_TEST( test_pub_cnt==1UL && test_pub_sig[ 0 ]==FD_SNAPSHOT_MSG_CTRL_INIT_FULL );

  /* Data is held, repeatedly and without side effects.  Controls are
     not: an ERROR (and, after it, a FAIL) must still be deliverable. */
  test_cur_tile = ctx->tile_idx;
  for( ulong i=0UL; i<8UL; i++ ) {
    FD_TEST( before_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_DATA )==-1 );
    FD_TEST( ctx->gate_pending );
    FD_TEST( !test_accdb_writer_begin_cnt );
    FD_TEST( !cl->shmem->next_appendvec );
    FD_TEST( !ctx->appendvec_seq );
  }
  FD_TEST( before_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR )==0 );
  FD_TEST( before_frag( ctx, 0UL, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL  )==0 );

  /* Publish the attempt slot exactly as tile 0's INIT does. */
  FD_VOLATILE( cl->shmem->attempt.fork_id ) = (ulong)USHORT_MAX;
  FD_COMPILER_MFENCE();
  FD_VOLATILE( cl->shmem->attempt.generation ) = 1UL;

  /* The next data frag opens the gate and is admitted. */
  FD_TEST( tile_step( ctx )==0UL );
  FD_TEST( !ctx->gate_pending );
  FD_TEST( ctx->incr_fork==(ulong)USHORT_MAX );
  FD_TEST( test_accdb_writer_begin_cnt==1UL );
  FD_TEST( cl->shmem->next_appendvec==2UL );  /* the eager claim, then its replacement */
  FD_TEST( ctx->owned_appendvecs==1UL );
  FD_TEST( test_pub_cnt==1UL );             /* still just the INIT ack */

  test_cluster_delete( cl );
}

/* An ERROR can abort tile 0's INIT barrier mid-way (the remaining INIT
   frags are dropped by the ERROR-state filter), so the attempt slot is
   never published for that generation.  A tile that DID complete its
   INIT barrier for the attempt must not wedge: it holds its data,
   consumes the ERROR, acks the FAIL, and the retry (generation G+1)
   loads cleanly.  This is the sequence the old blocking spin gate
   deadlocked on -- and then crashed the validator on. */
static void
test_init_aborted_barrier_retries( void ) {
  ulong const n = 2UL;
  ulong const T = 5UL;

  test_cluster_t * cl = test_cluster_new( n, 2UL );
  test_counters_reset();
  test_stream_init( T );

  fd_snapin_tile_t * t0 = &cl->ctx[ 0 ];
  fd_snapin_tile_t * t1 = &cl->ctx[ 1 ];

  /* Tile 1 completes its INIT barrier on both lanes. */
  for( ulong lane=0UL; lane<cl->lane_cnt; lane++ ) tile_send_control( t1, lane, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( t1->generation==1UL );
  FD_TEST( t1->gate_pending );

  /* Tile 0 consumes INIT on lane 0 only, then the ERROR that sits right
     behind it: its barrier is abandoned, so the INIT handler never runs
     and nothing is published. */
  tile_send_control( t0, 0UL, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( t0->generation==1UL );
  FD_TEST( before_frag( t0, 0UL, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR )==0 );
  tile_send_control( t0, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( t0->state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( !t0->lead.init_completed );
  FD_TEST( !cl->shmem->attempt.generation );  /* slot never published */
  FD_TEST( !test_accdb_writer_begin_cnt );

  /* Tile 1 holds its data behind the unpublished slot, but the ERROR at
     its lane head is still deliverable. */
  test_cur_tile = 1UL;
  FD_TEST( before_frag( t1, 0UL, 0UL, FD_SNAPSHOT_MSG_DATA )==-1 );
  FD_TEST( before_frag( t1, 0UL, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR )==0 );
  tile_send_control( t1, 0UL, FD_SNAPSHOT_MSG_CTRL_ERROR );
  FD_TEST( t1->state==FD_SNAPSHOT_STATE_ERROR );
  /* In ERROR everything but FAIL is dropped, so the tile drains to the
     FAIL barrier instead of stalling on the held data. */
  FD_TEST( before_frag( t1, 0UL, 0UL, FD_SNAPSHOT_MSG_DATA )==1 );

  /* Both tiles ack the FAIL. */
  ulong pub0 = test_pub_cnt;
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( test_pub_cnt==pub0+n );
  for( ulong i=pub0; i<test_pub_cnt; i++ ) FD_TEST( test_pub_sig[ i ]==FD_SNAPSHOT_MSG_CTRL_FAIL );
  for( ulong t=0UL; t<n; t++ ) {
    FD_TEST( cl->ctx[ t ].state==FD_SNAPSHOT_STATE_IDLE );
    FD_TEST( !cl->ctx[ t ].gate_pending );
  }
  /* Tile 0's INIT never ran, so there is nothing to roll back -- and in
     particular the root fork id must not have been wiped on the stale
     `full` flag. */
  FD_TEST( !t0->lead.rollback.pending );

  /* Retry: generation 2 is published and the load completes. */
  test_counters_reset();
  test_stream_init( T );
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( cl->shmem->attempt.generation==2UL );
  for( ulong t=0UL; t<n; t++ ) FD_TEST( cl->ctx[ t ].generation==2UL );
  FD_TEST( !t0->gate_pending );  /* tile 0 publishes, so it never gates */
  FD_TEST( t1->gate_pending );   /* ... and tile 1 opens on its first data frag */

  ulong owner[ TEST_AV_MAX ];
  cluster_stream( cl, TEST_ORDER_ROUND_ROBIN, owner );
  for( ulong t=0UL; t<n; t++ ) FD_TEST( !cl->ctx[ t ].gate_pending );
  FD_TEST( test_accdb_writer_begin_cnt==n );

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( cl->shmem->totals.appendvecs_processed==T );
  FD_TEST( cl->shmem->next_appendvec==T+n );

  test_cluster_delete( cl );
}

/* The attempt slot left behind by an earlier attempt must not release
   the gate: the retry compares generations, not a flag. */
static void
test_init_gate_rejects_stale_generation( void ) {
  test_cluster_t * cl = test_cluster_new( 2UL, 1UL );
  test_counters_reset();
  test_stream_init( 4UL );

  fd_snapin_tile_t * t1 = &cl->ctx[ 1 ];

  /* Attempt 1 loads normally on tile 1 (tile 0 publishes generation 1). */
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( cl->shmem->attempt.generation==1UL );
  (void)tile_step( t1 );  /* the first data frag opens tile 1's gate */
  FD_TEST( !t1->gate_pending );

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FAIL );

  /* Attempt 2: only tile 1's barrier completes.  The slot still holds
     generation 1, which must NOT open the gate. */
  for( ulong lane=0UL; lane<cl->lane_cnt; lane++ ) tile_send_control( t1, lane, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( t1->generation==2UL );
  FD_TEST( cl->shmem->attempt.generation==1UL );
  test_cur_tile = 1UL;
  FD_TEST( before_frag( t1, 0UL, 0UL, FD_SNAPSHOT_MSG_DATA )==-1 );
  FD_TEST( t1->gate_pending );

  test_cluster_delete( cl );
}

/* Tile 0's INIT critical sequence publishes the slot LAST, after
   re-zeroing every attempt-scoped shared field.  A stale workspace
   (crashed load) must not leak into the new attempt. */
static void
test_init_publishes_after_reset( void ) {
  test_cluster_t * cl = test_cluster_new( 4UL, 1UL );
  test_counters_reset();

  /* Dirty every attempt-scoped shared field, as a killed load would
     leave them. */
  fd_snapin_shmem_t * shmem = cl->shmem;
  shmem->next_appendvec              = 999UL;
  shmem->totals.accounts_loaded      = 1234UL;
  shmem->totals.input_lamports       = 5678UL;
  shmem->totals.appendvecs_processed = 42UL;
  shmem->slot_history.captured       = 1;
  shmem->feature_snoop.present[ 0 ]  = 1;

  /* Only tile 0's INIT: it re-zeroes and publishes. */
  for( ulong lane=0UL; lane<cl->lane_cnt; lane++ ) tile_send_control( &cl->ctx[ 0 ], lane, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );

  FD_TEST( !shmem->totals.accounts_loaded );
  FD_TEST( !shmem->totals.input_lamports );
  FD_TEST( !shmem->totals.appendvecs_processed );
  FD_TEST( !shmem->slot_history.captured );
  FD_TEST( !shmem->feature_snoop.present[ 0 ] );
  FD_TEST( shmem->attempt.generation==1UL );
  FD_TEST( shmem->attempt.fork_id==(ulong)USHORT_MAX );
  /* Re-zeroed, then tile 0's own eager claim (it publishes the slot, so
     its gate opens inside the INIT handler; the other tiles draw theirs
     when their first data frag arrives). */
  FD_TEST( shmem->next_appendvec==1UL );
  FD_TEST( cl->ctx[ 0 ].claimed_appendvec==0UL );
  FD_TEST( !cl->ctx[ 0 ].gate_pending );

  FD_TEST( test_accdb_reset_cnt==1UL );
  FD_TEST( test_accdb_attach_cnt==1UL );
  FD_TEST( test_accdb_load_begin_cnt==1UL );

  test_cluster_delete( cl );
}

/* Eager claim coverage ************************************************/

/* Every appendvec in the stream is claimed by exactly one tile, no
   matter how the tiles interleave; every tile ends the attempt holding
   exactly one unmatched claim, so next_appendvec lands on T+N. */
static void
test_eager_claim_coverage( void ) {
  ulong const tile_cnts[] = { 1UL, 2UL, 3UL, 4UL, 5UL, 6UL, 7UL, 8UL, 9UL };
  int   const orders   [] = { TEST_ORDER_ROUND_ROBIN, TEST_ORDER_TILE_MAJOR, TEST_ORDER_REVERSE };
  ulong const T = 13UL;

  for( ulong n_idx=0UL; n_idx<sizeof(tile_cnts)/sizeof(tile_cnts[0]); n_idx++ ) {
    ulong n = tile_cnts[ n_idx ];
    for( ulong o_idx=0UL; o_idx<sizeof(orders)/sizeof(orders[0]); o_idx++ ) {
      test_cluster_t * cl = test_cluster_new( n, 1UL );
      test_counters_reset();
      test_stream_init( T );

      cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
      /* Only tile 0 claims at INIT (it publishes the slot its own gate
         waits on); every other tile draws its claim when its first data
         frag opens its gate. */
      FD_TEST( cl->shmem->next_appendvec==1UL );
      FD_TEST( cl->ctx[ 0 ].claimed_appendvec==0UL );
      for( ulong t=1UL; t<n; t++ ) FD_TEST( cl->ctx[ t ].gate_pending );

      ulong owner[ TEST_AV_MAX ];
      cluster_stream( cl, orders[ o_idx ], owner );
      for( ulong t=0UL; t<n; t++ ) FD_TEST( !cl->ctx[ t ].gate_pending );

      ulong owned_sum = 0UL;
      for( ulong t=0UL; t<n; t++ ) owned_sum += cl->ctx[ t ].owned_appendvecs;
      FD_TEST( owned_sum==T );
      FD_TEST( test_appendvec_parse_cnt==T ); /* the parser was flipped exactly once per ordinal */

      /* A single tile owns everything, in stream order. */
      if( n==1UL ) {
        FD_TEST( cl->ctx[ 0 ].owned_appendvecs==T );
        for( ulong i=0UL; i<T; i++ ) FD_TEST( owner[ i ]==0UL );
      }

      cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FINI );
      FD_TEST( cl->shmem->totals.appendvecs_processed==T );
      FD_TEST( cl->shmem->next_appendvec==T+n ); /* T consumed claims + N unmatched */

      test_cluster_delete( cl );
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
  ctx->lead.gui_out.idx       = 0UL;
  ctx->lead.gui_out.mem       = (fd_wksp_t *)gui_data;
  ctx->lead.gui_out.chunk0    = 0UL;
  ctx->lead.gui_out.wmark     = 0UL;
  ctx->lead.gui_out.chunk     = 0UL;
  ctx->lead.gui_config_acct_sz  = 2UL;
  ctx->lead.gui_config_acct_off = 0UL;

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
  FD_TEST( ctx->lead.init_completed );
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
new_banks( fd_wksp_t * wksp ) {
  void * mem = fd_wksp_alloc_laddr( wksp, fd_banks_align(), fd_banks_footprint( 16UL, 4UL, 16UL, 64UL, 16UL ), 1UL );
  FD_TEST( mem );
  fd_banks_t * banks = fd_banks_join( fd_banks_new( mem, 16UL, 4UL, 16UL, 64UL, 16UL, 0, 42UL ) );
  FD_TEST( banks );
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
test_batch_stake_delegation( fd_wksp_t * wksp ) {
  fd_banks_t * banks = new_banks( wksp );
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

  fd_snapin_tile_t ctx = { .full = 1, .banks = banks, .stake_delegations = stake_delegations };
  fd_ssparse_advance_result_t result = {
    .account_batch = {
      .batch     = { entry },
      .batch_cnt = 1UL,
      .slot      = 10UL,
    },
  };

  FD_TEST( !worker_process_account_batch( &ctx, &result ) );
  assert_stake_delegation( stake_delegations, &stake_account, &vote_account );
}

static void
test_streaming_stake_delegation( fd_wksp_t * wksp ) {
  fd_banks_t * banks = new_banks( wksp );
  fd_stake_delegations_t * stake_delegations = fd_banks_stake_delegations_root_query( banks );

  fd_pubkey_t stake_account = { .ul = { 11UL, 12UL, 13UL, 14UL } };
  fd_pubkey_t vote_account  = { .ul = { 15UL, 16UL, 17UL, 18UL } };
  fd_stake_state_t state[1];
  make_stake_state( state, &vote_account );

  fd_snapin_tile_t ctx = { .full = 1, .banks = banks, .stake_delegations = stake_delegations };
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
  FD_TEST( !worker_process_account_header( &ctx, &header ) );

  ulong split = sizeof(fd_stake_state_t)/2UL;
  fd_ssparse_advance_result_t data = {
    .account_data = {
      .data    = (uchar const *)state,
      .data_sz = split,
    },
  };
  FD_TEST( !worker_process_account_data( &ctx, &data ) );
  FD_TEST( !fd_stake_delegation_root_query( stake_delegations, &stake_account ) );

  data.account_data.data    = (uchar const *)state + split;
  data.account_data.data_sz = sizeof(fd_stake_state_t) - split;
  FD_TEST( !worker_process_account_data( &ctx, &data ) );
  assert_stake_delegation( stake_delegations, &stake_account, &vote_account );
}

static void
test_txncache_staging_entry_size( void ) {
  fd_snapin_tile_t ctx[ 1 ];
  FD_TEST( sizeof(ctx->lead.txncache_entries[ 0 ])==20UL );
}

static void
test_txncache_staging_group_record_size( void ) {
  FD_TEST( sizeof(blockhash_group_t)==40UL );
  FD_TEST( TEST_MAX_ENTRIES_PER_SLOT==FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT );
  FD_TEST( FD_SNAPIN_MAX_RECENT_GROUPS==FD_TXNCACHE_MAX_SLOT_DELTAS*FD_TXNCACHE_MAX_SLOT_DELTAS );
}

static void
test_txncache_staging_side_arrays_alloc( fd_snapin_tile_t * ctx,
                                         fd_wksp_t *        wksp ) {
  ctx->lead.recent_groups    = fd_wksp_alloc_laddr( wksp, alignof(recent_blockhash_group_t), FD_SNAPIN_MAX_RECENT_GROUPS*sizeof(recent_blockhash_group_t), 1UL );
  ctx->lead.txncache_entries = fd_wksp_alloc_laddr( wksp, alignof(fd_sstxncache_hash_t),     TEST_MAX_ENTRIES*sizeof(fd_sstxncache_hash_t), 1UL  );
  FD_TEST( ctx->lead.recent_groups );
  FD_TEST( ctx->lead.txncache_entries );
}

static void
test_txncache_staging_ctx_init( fd_snapin_tile_t * ctx,
                                fd_wksp_t *        wksp ) {
  fd_memset( ctx, 0, sizeof(*ctx) );
  ctx->lead.seed = 1UL;
  ctx->lead.txncache_max_groups_per_slot  = TEST_MAX_GROUPS_PER_SLOT;
  ctx->lead.txncache_max_entries_per_slot = TEST_MAX_ENTRIES_PER_SLOT;
  ctx->lead.txncache_entries_max          = TEST_MAX_ENTRIES;
  txncache_staging_reset( ctx );
  ctx->lead.blockhash_groups = fd_wksp_alloc_laddr( wksp, alignof(blockhash_group_t), TEST_MAX_STAGED_GROUPS*sizeof(blockhash_group_t), 1UL );
  FD_TEST( ctx->lead.blockhash_groups );
  test_txncache_staging_side_arrays_alloc( ctx, wksp );
}

/* Builds a txncache with one live slot and returns the local join. */
static fd_txncache_t *
new_txncache( fd_wksp_t * wksp,
              ulong       max_txn_per_slot ) {
  void * shmem = fd_wksp_alloc_laddr( wksp, fd_txncache_shmem_align(), fd_txncache_shmem_footprint( 1UL, max_txn_per_slot ), 1UL );
  FD_TEST( shmem );
  fd_txncache_shmem_t * txncache_shmem = fd_txncache_shmem_join( fd_txncache_shmem_new( shmem, 1UL, max_txn_per_slot, 0UL ) );
  FD_TEST( txncache_shmem );

  void * local = fd_wksp_alloc_laddr( wksp, fd_txncache_align(), fd_txncache_footprint( 1UL ), 1UL );
  FD_TEST( local );
  fd_txncache_t * txncache = fd_txncache_join( fd_txncache_new( local, txncache_shmem ) );
  FD_TEST( txncache );
  return txncache;
}

static void
test_txncache_staging_groups_fit_txncache_scratch( fd_wksp_t * wksp ) {
  fd_txncache_t * txncache = new_txncache( wksp, FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT );

  ulong scratch_sz = 0UL;
  void * scratch = fd_txncache_snapin_scratch( txncache, &scratch_sz );
  FD_TEST( scratch );

  blockhash_group_t * groups = txncache_staging_groups_join( scratch, scratch_sz, TEST_MAX_STAGED_GROUPS );
  FD_TEST( groups );
  FD_TEST( fd_ulong_is_aligned( (ulong)groups, alignof(blockhash_group_t) ) );
  FD_TEST( (uchar *)groups>=(uchar *)scratch );
  FD_TEST( (uchar *)(groups+TEST_MAX_STAGED_GROUPS)<=(uchar *)scratch+scratch_sz );

  ulong ring_sz = TEST_MAX_STAGED_GROUPS*sizeof(blockhash_group_t);
  FD_TEST(  txncache_staging_groups_join( (void *)64UL, ring_sz,     TEST_MAX_STAGED_GROUPS ) );
  FD_TEST( !txncache_staging_groups_join( (void *)64UL, ring_sz-1UL, TEST_MAX_STAGED_GROUPS ) );
  FD_TEST( !txncache_staging_groups_join( (void *)66UL, ring_sz,     TEST_MAX_STAGED_GROUPS ) );
  FD_TEST(  txncache_staging_groups_join( (void *)66UL, ring_sz+2UL, TEST_MAX_STAGED_GROUPS ) );
}

static void
test_txncache_staging_evicts_oldest_slot( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  test_txncache_staging_ctx_init( ctx, wksp );

  ulong oldest_idx = ULONG_MAX;
  for( ulong i=0UL; i<FD_TXNCACHE_MAX_SLOT_DELTAS; i++ ) {
    ulong slot_idx = txncache_staging_slot_begin( ctx, 1000UL+i );
    FD_TEST( slot_idx!=ULONG_MAX );
    FD_TEST( ctx->lead.txncache_current_slot_idx==slot_idx );
    if( FD_UNLIKELY( !i ) ) oldest_idx = slot_idx;
  }
  FD_TEST( oldest_idx!=ULONG_MAX );
  FD_TEST( ctx->lead.txncache_slots_len==FD_TXNCACHE_MAX_SLOT_DELTAS );

  FD_TEST( txncache_staging_slot_begin( ctx, 999UL )==ULONG_MAX );
  FD_TEST( ctx->lead.txncache_current_slot_idx==ULONG_MAX );
  FD_TEST( ctx->lead.txncache_slots[ oldest_idx ].slot==1000UL );

  ulong replacement_idx = txncache_staging_slot_begin( ctx, 1200UL );
  FD_TEST( replacement_idx==oldest_idx );
  FD_TEST( ctx->lead.txncache_slots[ replacement_idx ].slot==1200UL );
  FD_TEST( ctx->lead.txncache_slots[ replacement_idx ].entry_cnt==0UL );
  FD_TEST( ctx->lead.txncache_slots[ replacement_idx ].group_cnt==0UL );
}

static void
test_txncache_staging_evicted_slot_drops_groups( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  test_txncache_staging_ctx_init( ctx, wksp );

  static uchar const blockhash_x[ 32UL ] = { 0x11 };
  static uchar const blockhash_y[ 32UL ] = { 0x22 };
  static uchar const txnhash[ 20UL ]     = { 0x33 };

  ulong oldest_idx = txncache_staging_slot_begin( ctx, 1000UL );
  FD_TEST( oldest_idx!=ULONG_MAX );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash_x, 3UL ) );
  FD_TEST( !txncache_staging_entry_add( ctx, 1000UL, txnhash ) );
  FD_TEST( !txncache_staging_entry_add( ctx, 1000UL, txnhash ) );
  FD_TEST( ctx->lead.txncache_slots[ oldest_idx ].group_cnt==1UL );
  FD_TEST( ctx->lead.txncache_slots[ oldest_idx ].entry_cnt==2UL );

  blockhash_group_t const * group = &ctx->lead.blockhash_groups[ oldest_idx*TEST_MAX_GROUPS_PER_SLOT ];
  FD_TEST( !memcmp( group->blockhash, blockhash_x, 32UL ) );
  FD_TEST( group->txnhash_offset==3UL );
  FD_TEST( group->txncache_entry_cnt==2UL );
  FD_TEST( ctx->lead.txncache_entries[ ctx->lead.txncache_slots[ oldest_idx ].entry_base ].txnhash[ 0 ]==0x33 );

  for( ulong i=1UL; i<FD_TXNCACHE_MAX_SLOT_DELTAS; i++ ) FD_TEST( txncache_staging_slot_begin( ctx, 1000UL+i )!=ULONG_MAX );

  FD_TEST( txncache_staging_slot_begin( ctx, 999UL )==ULONG_MAX );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash_y, 5UL ) );
  FD_TEST( !txncache_staging_entry_add( ctx, 999UL, txnhash ) );
  FD_TEST( ctx->lead.blockhash_groups_cnt==2UL );
  FD_TEST( ctx->lead.txncache_slots[ oldest_idx ].slot==1000UL );
  FD_TEST( ctx->lead.txncache_slots[ oldest_idx ].group_cnt==1UL );
  FD_TEST( ctx->lead.txncache_slots[ oldest_idx ].entry_cnt==2UL );
  FD_TEST( !memcmp( group->blockhash, blockhash_x, 32UL ) );

  ulong replacement_idx = txncache_staging_slot_begin( ctx, 1200UL );
  FD_TEST( replacement_idx==oldest_idx );
  FD_TEST( ctx->lead.txncache_slots[ replacement_idx ].group_cnt==0UL );
  FD_TEST( ctx->lead.txncache_slots[ replacement_idx ].entry_cnt==0UL );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash_y, 5UL ) );
  FD_TEST( ctx->lead.txncache_slots[ replacement_idx ].group_cnt==1UL );
  FD_TEST( !memcmp( group->blockhash, blockhash_y, 32UL ) );
  FD_TEST( group->txnhash_offset==5UL );
  FD_TEST( group->txncache_entry_cnt==0UL );
  FD_TEST( ctx->lead.blockhash_groups_cnt==3UL );
}

static void
test_txncache_staging_rejects_group_overflow( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  test_txncache_staging_ctx_init( ctx, wksp );

  uchar blockhash[ 32UL ] = {0};
  FD_TEST( txncache_staging_slot_begin( ctx, 1000UL )==0UL );
  for( ulong i=0UL; i<TEST_MAX_GROUPS_PER_SLOT; i++ ) {
    FD_STORE( ulong, blockhash, i );
    FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 0UL ) );
  }
  FD_TEST( ctx->lead.txncache_slots[ 0 ].group_cnt==TEST_MAX_GROUPS_PER_SLOT );
  FD_TEST( txncache_staging_group_begin( ctx, blockhash, 0UL )==-1 );

  /* Bound ignored slots too, so malformed input cannot bypass the
     per-slot work limit. */
  for( ulong i=1UL; i<FD_TXNCACHE_MAX_SLOT_DELTAS; i++ ) FD_TEST( txncache_staging_slot_begin( ctx, 1000UL+i )!=ULONG_MAX );
  FD_TEST( txncache_staging_slot_begin( ctx, 999UL )==ULONG_MAX );
  for( ulong i=0UL; i<TEST_MAX_GROUPS_PER_SLOT; i++ ) FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 0UL ) );
  FD_TEST( txncache_staging_group_begin( ctx, blockhash, 0UL )==-1 );
}

/* The entry bound is on the total, not per slot delta: a single delta
   may hold every entry of the 151 rooted blockhashes (which is how
   Firedancer-produced status caches look). */
static void
test_txncache_staging_rejects_entry_overflow( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  test_txncache_staging_ctx_init( ctx, wksp );

  static uchar const blockhash[ 32UL ] = { 0x11 };
  static uchar const txnhash[ 20UL ]   = { 0x33 };
  FD_TEST( txncache_staging_slot_begin( ctx, 1000UL )==0UL );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 0UL ) );
  for( ulong i=0UL; i<ctx->lead.txncache_entries_max; i++ ) FD_TEST( !txncache_staging_entry_add( ctx, 1000UL, txnhash ) );
  FD_TEST( ctx->lead.txncache_slots[ 0 ].entry_cnt==ctx->lead.txncache_entries_max );
  FD_TEST( ctx->lead.blockhash_groups[ 0 ].txncache_entry_cnt==ctx->lead.txncache_entries_max );
  FD_TEST( txncache_staging_entry_add( ctx, 1000UL, txnhash )==-1 );
}

/* A staged slot delta that is later evicted must give its entry range
   back: fill the pool through a low slot, evict it with 151 higher
   slots, then stage more entries than were left over. */
static void
test_txncache_staging_reclaims_evicted_entries( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  test_txncache_staging_ctx_init( ctx, wksp );

  static uchar const blockhash[ 32UL ] = { 0x11 };
  uchar txnhash[ 20UL ] = { 0x33 };

  ulong const big_cnt = TEST_MAX_ENTRIES-200UL;
  FD_TEST( txncache_staging_slot_begin( ctx, 1000UL )==0UL );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 0UL ) );
  for( ulong i=0UL; i<big_cnt; i++ ) FD_TEST( !txncache_staging_entry_add( ctx, 1000UL, txnhash ) );

  /* 150 more retained slots, one entry each (pool: big_cnt+150) */
  for( ulong i=1UL; i<FD_TXNCACHE_MAX_SLOT_DELTAS; i++ ) {
    FD_TEST( txncache_staging_slot_begin( ctx, 1000UL+i )!=ULONG_MAX );
    FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 0UL ) );
    txnhash[ 1 ] = (uchar)i;
    FD_TEST( !txncache_staging_entry_add( ctx, 1000UL+i, txnhash ) );
  }
  FD_TEST( ctx->lead.txncache_entries_len==big_cnt+150UL );

  /* Slot 2000 evicts slot 1000; 400 entries do not fit the 50 left,
     so the pool must be compacted to reclaim slot 1000's range. */
  ulong idx = txncache_staging_slot_begin( ctx, 2000UL );
  FD_TEST( idx==0UL );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 0UL ) );
  txnhash[ 1 ] = 0xEE;
  for( ulong i=0UL; i<400UL; i++ ) FD_TEST( !txncache_staging_entry_add( ctx, 2000UL, txnhash ) );
  FD_TEST( ctx->lead.txncache_entries_len==150UL+400UL );
  FD_TEST( ctx->lead.txncache_slots[ 0 ].entry_base==150UL );
  FD_TEST( ctx->lead.txncache_slots[ 0 ].entry_cnt ==400UL );

  /* The retained slots' entries survived the move, in order */
  for( ulong i=1UL; i<FD_TXNCACHE_MAX_SLOT_DELTAS; i++ ) {
    txncache_staging_slot_t const * s = &ctx->lead.txncache_slots[ i ];
    FD_TEST( s->slot==1000UL+i && s->entry_cnt==1UL && s->entry_base==i-1UL );
    FD_TEST( ctx->lead.txncache_entries[ s->entry_base ].txnhash[ 1 ]==(uchar)i );
  }
  FD_TEST( ctx->lead.txncache_entries[ 150UL ].txnhash[ 1 ]==0xEE );
  FD_TEST( ctx->lead.txncache_entries[ 549UL ].txnhash[ 1 ]==0xEE );
}

/* Entries of an evicted (older than the 151 retained) slot delta are
   discarded and do not consume the entry pool. */
static void
test_txncache_staging_evicted_entries_not_pooled( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  test_txncache_staging_ctx_init( ctx, wksp );

  static uchar const blockhash[ 32UL ] = { 0x11 };
  static uchar const txnhash[ 20UL ]   = { 0x33 };
  for( ulong i=0UL; i<FD_TXNCACHE_MAX_SLOT_DELTAS; i++ ) FD_TEST( txncache_staging_slot_begin( ctx, 1000UL+i )!=ULONG_MAX );
  FD_TEST( txncache_staging_slot_begin( ctx, 999UL )==ULONG_MAX );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 0UL ) );
  for( ulong i=0UL; i<3UL; i++ ) FD_TEST( !txncache_staging_entry_add( ctx, 999UL, txnhash ) );
  FD_TEST( ctx->lead.txncache_entries_len==0UL );

  ulong idx = txncache_staging_slot_begin( ctx, 2000UL );
  FD_TEST( idx!=ULONG_MAX );
  FD_TEST( ctx->lead.txncache_slots[ idx ].entry_base==0UL );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 0UL ) );
  FD_TEST( !txncache_staging_entry_add( ctx, 2000UL, txnhash ) );
  FD_TEST( ctx->lead.txncache_entries_len==1UL );
}

static blockhash_map_t *
test_txncache_staging_recent_set( void *                 mem,
                                  fd_blockhash_entry_t * pool,
                                  ulong                  seed,
                                  uchar const * const *  blockhashes,
                                  ulong                  blockhashes_cnt ) {
  blockhash_map_t * map = blockhash_map_join( blockhash_map_new( mem, 1024UL, seed ) );
  FD_TEST( map );
  for( ulong i=0UL; i<blockhashes_cnt; i++ ) {
    fd_memcpy( pool[ i ].blockhash.uc, blockhashes[ i ], 32UL );
    blockhash_map_ele_insert( map, &pool[ i ], pool );
  }
  return map;
}

static void
test_txncache_staging_filters_recent_groups( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  test_txncache_staging_ctx_init( ctx, wksp );

  static uchar const recent_a[ 32UL ] = { 0xA1 };
  static uchar const recent_b[ 32UL ] = { 0xB2 };
  static uchar const nonce_c[ 32UL ]  = { 0xC3 };
  static uchar const nonce_d[ 32UL ]  = { 0xD4 };
  uchar txnhash[ 20UL ] = {0};
  uchar next_txn = 1;

  FD_TEST( txncache_staging_slot_begin( ctx, 1000UL )==0UL );
  FD_TEST( !txncache_staging_group_begin( ctx, recent_a, 1UL ) );
  for( ulong i=0UL; i<2UL; i++ ) { txnhash[ 0 ] = next_txn++; FD_TEST( !txncache_staging_entry_add( ctx, 1000UL, txnhash ) ); }
  FD_TEST( !txncache_staging_group_begin( ctx, nonce_c, 7UL ) );
  for( ulong i=0UL; i<2UL; i++ ) { txnhash[ 0 ] = next_txn++; FD_TEST( !txncache_staging_entry_add( ctx, 1000UL, txnhash ) ); }
  FD_TEST( !txncache_staging_group_begin( ctx, recent_b, 2UL ) );
  for( ulong i=0UL; i<2UL; i++ ) { txnhash[ 0 ] = next_txn++; FD_TEST( !txncache_staging_entry_add( ctx, 1000UL, txnhash ) ); }

  FD_TEST( txncache_staging_slot_begin( ctx, 1001UL )==1UL );
  FD_TEST( !txncache_staging_group_begin( ctx, nonce_d, 9UL ) );
  for( ulong i=0UL; i<2UL; i++ ) { txnhash[ 0 ] = next_txn++; FD_TEST( !txncache_staging_entry_add( ctx, 1001UL, txnhash ) ); }
  FD_TEST( !txncache_staging_group_begin( ctx, recent_a, 1UL ) );
  for( ulong i=0UL; i<2UL; i++ ) { txnhash[ 0 ] = next_txn++; FD_TEST( !txncache_staging_entry_add( ctx, 1001UL, txnhash ) ); }

  uchar __attribute__((aligned(alignof(blockhash_map_t)))) _map[ blockhash_map_footprint( 1024UL ) ];
  fd_blockhash_entry_t pool[ 2UL ];
  uchar const * recent[ 2UL ] = { recent_a, recent_b };
  blockhash_map_t * map = test_txncache_staging_recent_set( _map, pool, ctx->lead.seed, recent, 2UL );

  FD_TEST( !filter_staged_groups( ctx, map, pool ) );
  FD_TEST( ctx->lead.recent_groups_len==3UL );

  recent_blockhash_group_t const * g = ctx->lead.recent_groups;
  FD_TEST( g[ 0 ].chain_idx==0UL );
  FD_TEST( g[ 0 ].txnhash_offset==1UL );
  FD_TEST( g[ 0 ].txncache_entry_idx==0UL );
  FD_TEST( g[ 0 ].txncache_entry_cnt==2UL );

  FD_TEST( g[ 1 ].chain_idx==1UL );
  FD_TEST( g[ 1 ].txnhash_offset==2UL );
  FD_TEST( g[ 1 ].txncache_entry_idx==4UL );
  FD_TEST( g[ 1 ].txncache_entry_cnt==2UL );

  FD_TEST( g[ 2 ].chain_idx==0UL );
  FD_TEST( g[ 2 ].txnhash_offset==1UL );
  FD_TEST( g[ 2 ].txncache_entry_idx==8UL );
  FD_TEST( g[ 2 ].txncache_entry_cnt==2UL );

  FD_TEST( ctx->lead.txncache_entries[ g[ 0 ].txncache_entry_idx     ].txnhash[ 0 ]==1  );
  FD_TEST( ctx->lead.txncache_entries[ g[ 1 ].txncache_entry_idx     ].txnhash[ 0 ]==5  );
  FD_TEST( ctx->lead.txncache_entries[ g[ 1 ].txncache_entry_idx+1UL ].txnhash[ 0 ]==6  );
  FD_TEST( ctx->lead.txncache_entries[ g[ 2 ].txncache_entry_idx     ].txnhash[ 0 ]==9  );
  FD_TEST( ctx->lead.txncache_entries[ g[ 2 ].txncache_entry_idx+1UL ].txnhash[ 0 ]==10 );
}

static void
test_txncache_staging_rejects_recent_group_overflow( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  test_txncache_staging_ctx_init( ctx, wksp );

  static uchar const recent_a[ 32UL ] = { 0xA1 };
  FD_TEST( txncache_staging_slot_begin( ctx, 1000UL )==0UL );
  for( ulong i=0UL; i<FD_SNAPIN_MAX_RECENT_GROUPS+1UL; i++ ) FD_TEST( !txncache_staging_group_begin( ctx, recent_a, 0UL ) );

  uchar __attribute__((aligned(alignof(blockhash_map_t)))) _map[ blockhash_map_footprint( 1024UL ) ];
  fd_blockhash_entry_t pool[ 1UL ];
  uchar const * recent[ 1UL ] = { recent_a };
  blockhash_map_t * map = test_txncache_staging_recent_set( _map, pool, ctx->lead.seed, recent, 1UL );

  FD_TEST( filter_staged_groups( ctx, map, pool )==-1 );
}

static void
test_txncache_staging_fits_one_gigantic_page( void ) {
  fd_topo_tile_t tile = {0};
  tile.snapin.max_live_slots   = 2048UL;
  tile.snapin.max_txn_per_slot = FD_MAX_TXN_PER_SLOT;
  ulong footprint = scratch_footprint( &tile );
  FD_TEST( footprint<(1UL<<30) );

  /* The staged entries scale with the per-slot limit, within the 512
     byte scratch alignment. */
  tile.snapin.max_txn_per_slot = 2UL*FD_MAX_TXN_PER_SLOT;
  ulong delta = scratch_footprint( &tile )-footprint;
  ulong expected_delta = TEST_MAX_ENTRIES*sizeof(fd_sstxncache_hash_t);
  FD_TEST( delta+scratch_align()>expected_delta && delta<expected_delta+scratch_align() );
}

/* Group and entry bounds are runtime limits, so a raised
   max_txn_per_slot admits proportionally more before rejection. */
static void
test_txncache_staging_runtime_limits( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  test_txncache_staging_ctx_init( ctx, wksp );
  ctx->lead.txncache_max_groups_per_slot  = 3UL;
  ctx->lead.txncache_max_entries_per_slot = 6UL;
  ctx->lead.txncache_entries_max          = FD_TXNCACHE_MAX_SLOT_DELTAS*6UL;

  uchar blockhash[ 32UL ] = {0};
  static uchar const txnhash[ 20UL ] = { 0x33 };
  FD_TEST( txncache_staging_slot_begin( ctx, 1000UL )==0UL );
  for( ulong i=0UL; i<3UL; i++ ) {
    FD_STORE( ulong, blockhash, i );
    FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 0UL ) );
    FD_TEST( !txncache_staging_entry_add( ctx, 1000UL, txnhash ) );
    FD_TEST( !txncache_staging_entry_add( ctx, 1000UL, txnhash ) );
  }
  FD_TEST( txncache_staging_group_begin( ctx, blockhash, 0UL )==-1 );

  /* Slot 1's groups start at the runtime stride, not the production one. */
  FD_TEST( txncache_staging_slot_begin( ctx, 1001UL )==1UL );
  FD_STORE( ulong, blockhash, 7UL );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 0UL ) );
  FD_TEST( !txncache_staging_entry_add( ctx, 1001UL, txnhash ) );
  FD_TEST( !memcmp( ctx->lead.blockhash_groups[ 3UL ].blockhash, blockhash, 32UL ) );
  FD_TEST( ctx->lead.txncache_entries[ 6UL ].txnhash[ 0 ]==0x33 );

  uchar __attribute__((aligned(alignof(blockhash_map_t)))) _map[ blockhash_map_footprint( 1024UL ) ];
  fd_blockhash_entry_t pool[ 1UL ];
  uchar const * recent[ 1UL ] = { blockhash };
  blockhash_map_t * map = test_txncache_staging_recent_set( _map, pool, ctx->lead.seed, recent, 1UL );
  FD_TEST( !filter_staged_groups( ctx, map, pool ) );
  FD_TEST( ctx->lead.recent_groups_len==1UL );
  FD_TEST( ctx->lead.recent_groups[ 0 ].txncache_entry_idx==6UL );
  FD_TEST( ctx->lead.recent_groups[ 0 ].txncache_entry_cnt==1UL );
}

static int
test_txncache_staging_populate( fd_snapin_tile_t * ctx,
                                fd_wksp_t *        wksp,
                                uchar const *      recent_blockhash ) {
  ctx->lead.txncache = new_txncache( wksp, 1UL );

  fd_snapshot_manifest_blockhash_t blockhashes[ FD_BLOCKHASHES_MAX ] = {{ .hash_index = 0UL }};
  fd_memcpy( blockhashes[ 0UL ].hash, recent_blockhash, 32UL );
  int res = populate_txncache( ctx, blockhashes, 1UL );
  return res;
}

static void
test_txncache_staging_rejects_conflicting_group_offsets( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  test_txncache_staging_ctx_init( ctx, wksp );

  static uchar const blockhash[ 32UL ] = { 1U };
  FD_TEST( txncache_staging_slot_begin( ctx, 1000UL )!=ULONG_MAX );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 1UL ) );
  FD_TEST( txncache_staging_slot_begin( ctx, 1001UL )!=ULONG_MAX );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 2UL ) );

  FD_TEST( test_txncache_staging_populate( ctx, wksp, blockhash )==1 );
}

static void
test_txncache_staging_ignores_evicted_group_offsets( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  test_txncache_staging_ctx_init( ctx, wksp );

  static uchar const blockhash[ 32UL ] = { 1U };
  FD_TEST( txncache_staging_slot_begin( ctx, 1000UL )!=ULONG_MAX );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 1UL ) );
  for( ulong i=1UL; i<FD_TXNCACHE_MAX_SLOT_DELTAS; i++ ) FD_TEST( txncache_staging_slot_begin( ctx, 1000UL+i )!=ULONG_MAX );
  FD_TEST( txncache_staging_slot_begin( ctx, 1200UL )==0UL );
  FD_TEST( !txncache_staging_group_begin( ctx, blockhash, 2UL ) );

  FD_TEST( test_txncache_staging_populate( ctx, wksp, blockhash )==0 );
  FD_TEST( ctx->lead.recent_groups_len==1UL );
  FD_TEST( ctx->lead.recent_groups[ 0 ].txnhash_offset==2UL );
}

static void
test_txncache_staging_populate_inserts_recent_only( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  sync_ctx_init( ctx, 1UL, FD_SNAPSHOT_STATE_PROCESSING );

  /* Transactions reference an ancestor's blockhash, never their own
     slot's, so the chain needs the referenced blockhash below the
     root. */
  static uchar const recent_old[ 32UL ] = { 0xA1 };
  static uchar const recent_new[ 32UL ] = { 0xA2 };
  static uchar const nonce[ 32UL ]      = { 0xC3 };
  uchar txnhash_recent[ 32UL ];
  uchar txnhash_nonce [ 32UL ];
  for( ulong i=0UL; i<32UL; i++ ) {
    txnhash_recent[ i ] = (uchar)(i+1UL);
    txnhash_nonce [ i ] = (uchar)(0x80UL+i);
  }

  ctx->lead.txncache = new_txncache( wksp, FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT );
  fd_txncache_reset( ctx->lead.txncache );

  ctx->lead.blockhash_groups = txncache_staging_scratch( ctx );

  test_txncache_staging_side_arrays_alloc( ctx, wksp );
  txncache_staging_reset( ctx );

  void * parser_mem = fd_wksp_alloc_laddr( wksp, fd_slot_delta_parser_align(), fd_slot_delta_parser_footprint(), 1UL );
  FD_TEST( parser_mem );
  ctx->lead.slot_delta_parser = fd_slot_delta_parser_join( fd_slot_delta_parser_new( parser_mem ) );
  FD_TEST( ctx->lead.slot_delta_parser );
  fd_slot_delta_parser_init( ctx->lead.slot_delta_parser );

  uchar status_cache[ 169UL ] __attribute__((aligned(FD_CHUNK_ALIGN)));
  uchar const * status_blockhashes[ 2UL ] = { nonce,          recent_old     };
  uchar const * status_txnhashes [ 2UL ] = { txnhash_nonce,  txnhash_recent };
  ulong const   status_offsets   [ 2UL ] = { 7UL,            5UL            };
  uchar * p = status_cache;
  FD_STORE( ulong, p, 1UL );    p += sizeof(ulong);
  FD_STORE( ulong, p, 1000UL ); p += sizeof(ulong);
  *p++ = 1U;
  FD_STORE( ulong, p, 2UL );    p += sizeof(ulong);
  for( ulong i=0UL; i<2UL; i++ ) {
    fd_memcpy( p, status_blockhashes[ i ], 32UL ); p += 32UL;
    FD_STORE( ulong, p, status_offsets[ i ] );     p += sizeof(ulong);
    FD_STORE( ulong, p, 1UL );                     p += sizeof(ulong);
    fd_memcpy( p, status_txnhashes[ i ]+status_offsets[ i ], 20UL ); p += 20UL;
    FD_STORE( uint, p, 0U ); p += sizeof(uint);
  }
  FD_TEST( p==status_cache+sizeof(status_cache) );

  ctx->in[ 0 ].wksp   = (fd_wksp_t *)status_cache;
  ctx->in[ 0 ].chunk0 = 0UL;
  ctx->in[ 0 ].wmark  = 0UL;
  ctx->in[ 0 ].mtu    = sizeof(status_cache);
  test_parser_script   = 4;
  test_parser_call_cnt = 0UL;
  FD_TEST( !handle_data_frag( ctx, 0UL, 0UL, sizeof(status_cache), (fd_stem_context_t *)1UL ) );
  FD_TEST( test_parser_call_cnt==1UL );
  FD_TEST( ctx->lead.flags.status_cache_done );
  FD_TEST( ctx->lead.txncache_slots_len==1UL );
  FD_TEST( ctx->lead.txncache_slots[ 0UL ].group_cnt==2UL );
  FD_TEST( ctx->lead.txncache_slots[ 0UL ].entry_cnt==2UL );

  fd_snapshot_manifest_blockhash_t blockhashes[ FD_BLOCKHASHES_MAX ] = {{ .hash_index = 0UL }, { .hash_index = 1UL }};
  fd_memcpy( blockhashes[ 0UL ].hash, recent_old, 32UL );
  fd_memcpy( blockhashes[ 1UL ].hash, recent_new, 32UL );
  FD_TEST( populate_txncache( ctx, blockhashes, 2UL )==0 );
  FD_TEST( ctx->lead.recent_groups_len==1UL );
  FD_TEST( ctx->lead.recent_groups[ 0 ].chain_idx==1UL ); /* recent_old is the root's parent */

  fd_txncache_fork_id_t child = fd_txncache_attach_child( ctx->lead.txncache, ctx->lead.txncache_root_fork_id );
  FD_TEST(  fd_txncache_query( ctx->lead.txncache, child, recent_old, txnhash_recent ) );
  FD_TEST( !fd_txncache_query( ctx->lead.txncache, child, recent_old, txnhash_nonce  ) );
}

/* An entry keyed by the newest blockhash (the root's own) is protocol
   invalid, as a transaction cannot reference the blockhash of the block
   it executes in.  It must be rejected as malformed, not abort. */
static void
test_txncache_staging_populate_rejects_newest_blockhash_entries( fd_wksp_t * wksp ) {
  fd_snapin_tile_t ctx[ 1 ];
  sync_ctx_init( ctx, 1UL, FD_SNAPSHOT_STATE_PROCESSING );

  static uchar const recent_old[ 32UL ] = { 0xA1 };
  static uchar const recent_new[ 32UL ] = { 0xA2 };
  uchar txnhash[ 32UL ];
  for( ulong i=0UL; i<32UL; i++ ) txnhash[ i ] = (uchar)(i+1UL);

  ctx->lead.txncache = new_txncache( wksp, FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT );
  fd_txncache_reset( ctx->lead.txncache );
  ctx->lead.blockhash_groups = txncache_staging_scratch( ctx );
  test_txncache_staging_side_arrays_alloc( ctx, wksp );
  txncache_staging_reset( ctx );

  void * parser_mem = fd_wksp_alloc_laddr( wksp, fd_slot_delta_parser_align(), fd_slot_delta_parser_footprint(), 1UL );
  FD_TEST( parser_mem );
  ctx->lead.slot_delta_parser = fd_slot_delta_parser_join( fd_slot_delta_parser_new( parser_mem ) );
  FD_TEST( ctx->lead.slot_delta_parser );
  fd_slot_delta_parser_init( ctx->lead.slot_delta_parser );

  fd_snapshot_manifest_blockhash_t blockhashes[ FD_BLOCKHASHES_MAX ] = {{ .hash_index = 0UL }, { .hash_index = 1UL }};
  fd_memcpy( blockhashes[ 0UL ].hash, recent_old, 32UL );
  fd_memcpy( blockhashes[ 1UL ].hash, recent_new, 32UL );

  /* One delta, one group keyed by recent_new (highest hash_index, i.e.
     the root's own blockhash), one entry. */
  FD_TEST( txncache_staging_slot_begin( ctx, 1000UL )==0UL );
  FD_TEST( !txncache_staging_group_begin( ctx, recent_new, 5UL ) );
  FD_TEST( !txncache_staging_entry_add( ctx, 1000UL, txnhash+5UL ) );
  FD_TEST( populate_txncache( ctx, blockhashes, 2UL )==1 );

  /* Control: the same group with no entries is fine. */
  fd_txncache_reset( ctx->lead.txncache );
  txncache_staging_reset( ctx );
  FD_TEST( txncache_staging_slot_begin( ctx, 1000UL )==0UL );
  FD_TEST( !txncache_staging_group_begin( ctx, recent_new, 5UL ) );
  FD_TEST( populate_txncache( ctx, blockhashes, 2UL )==0 );
}

/* Retry resets ********************************************************/

/* A failed attempt leaves nothing behind for the retry: the shared
   claim counter is re-zeroed and the claim sequence restarts at 0, no
   ordinal is processed twice, and every tile's per-attempt parse state
   is back to zero. */
static void
test_retry_resets( void ) {
  ulong const n = 4UL;
  ulong const T = 9UL;

  test_cluster_t * cl = test_cluster_new( n, 1UL );
  test_counters_reset();
  test_stream_init( T );

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( cl->shmem->next_appendvec==1UL );

  /* Partial walk: every tile gets three events in. */
  for( ulong step=0UL; step<3UL; step++ ) {
    for( ulong t=0UL; t<n; t++ ) (void)tile_step( &cl->ctx[ t ] );
  }
  ulong mid_claims = cl->shmem->next_appendvec;
  FD_TEST( mid_claims>n );

  /* Every tile acquired partitions during the attempt; the FAIL handler
     must publish each list for tile 0's deferred rollback. */
  for( ulong t=0UL; t<n; t++ ) {
    cl->ctx[ t ].whead.attempt_partition_cnt = 2UL+t;
    for( ulong i=0UL; i<2UL+t; i++ ) cl->ctx[ t ].shmem_worker->fail_partitions[ i ] = (uint)(100UL*t+i);
  }

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FAIL );
  for( ulong t=0UL; t<n; t++ ) {
    fd_snapin_tile_t * ctx = &cl->ctx[ t ];
    FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
    FD_TEST( !ctx->appendvec_seq );
    FD_TEST( !ctx->owned_appendvecs );
    FD_TEST( ctx->incr_fork==ULONG_MAX );
    FD_TEST( ctx->shmem_worker->fail_partition_cnt==2UL+t ); /* published for the rollback */
  }
  FD_TEST( cl->ctx[ 0 ].lead.rollback.pending );
  FD_TEST( cl->ctx[ 0 ].lead.rollback.full );
  /* The claim counter is deliberately NOT reset by FAIL: only tile 0's
     next INIT re-zeroes it. */
  FD_TEST( cl->shmem->next_appendvec==mid_claims );

  /* Retry.  Tile 0 rolls back first, then re-zeroes and republishes. */
  test_counters_reset();
  test_stream_init( T );
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );

  FD_TEST( !cl->ctx[ 0 ].lead.rollback.pending );
  FD_TEST( test_accdb_reset_cnt==1UL );      /* full retry wipes the fork wholesale */
  FD_TEST( !test_accdb_purge_cnt );          /* ... so no incremental purge */
  FD_TEST( !test_accdb_release_cnt );        /* ... and no partition release */
  FD_TEST( !cl->ctx[ 0 ].lead.doomed_partition_cnt );
  for( ulong t=0UL; t<n; t++ ) FD_TEST( !cl->ctx[ t ].shmem_worker->fail_partition_cnt ); /* gathered and cleared */

  FD_TEST( cl->shmem->next_appendvec==1UL );   /* claim sequence restarted at 0 */
  FD_TEST( cl->ctx[ 0 ].claimed_appendvec==0UL );
  for( ulong t=0UL; t<n; t++ ) FD_TEST( cl->ctx[ t ].generation==2UL );
  for( ulong t=1UL; t<n; t++ ) FD_TEST( cl->ctx[ t ].gate_pending );

  /* The retry covers every ordinal exactly once (cluster_stream would
     trip on a double claim). */
  ulong owner[ TEST_AV_MAX ];
  cluster_stream( cl, TEST_ORDER_ROUND_ROBIN, owner );

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( cl->shmem->totals.appendvecs_processed==T );
  FD_TEST( cl->shmem->next_appendvec==T+n );
  FD_TEST( !cl->shmem->totals.eq_slot_dups );

  test_cluster_delete( cl );
}

/* FINI gates **********************************************************/

/* FINI can only arrive after the tile's own parser reached EOF (every
   tile walks the whole tar itself).  A FINI in PROCESSING means the
   stream was truncated: the tile must flag the snapshot malformed and
   NOT ack the FINI. */
static void
test_fini_truncated_malform( void ) {
  ulong const n = 2UL;
  test_cluster_t * cl = test_cluster_new( n, 1UL );
  test_counters_reset();
  test_stream_init( 4UL );

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );

  /* Two of four appendvecs in: the parser never returned DONE. */
  for( ulong t=0UL; t<n; t++ ) { (void)tile_step( &cl->ctx[ t ] ); (void)tile_step( &cl->ctx[ t ] ); }
  for( ulong t=0UL; t<n; t++ ) FD_TEST( cl->ctx[ t ].state==FD_SNAPSHOT_STATE_PROCESSING );

  ulong pub0 = test_pub_cnt;
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FINI );

  for( ulong t=0UL; t<n; t++ ) FD_TEST( cl->ctx[ t ].state==FD_SNAPSHOT_STATE_ERROR );
  /* One unsolicited ERROR per tile, and no FINI ack from any of them. */
  FD_TEST( test_pub_cnt==pub0+n );
  for( ulong i=pub0; i<test_pub_cnt; i++ ) FD_TEST( test_pub_sig[ i ]==FD_SNAPSHOT_MSG_CTRL_ERROR );
  /* Nothing was folded: tile 0 must not read a partial attempt. */
  FD_TEST( !cl->shmem->totals.appendvecs_processed );

  /* In ERROR only FAIL flows; everything else is held. */
  FD_TEST( before_frag( &cl->ctx[ 0 ], 0UL, 0UL, FD_SNAPSHOT_MSG_DATA )==1 );
  FD_TEST( before_frag( &cl->ctx[ 0 ], 0UL, 0UL, FD_SNAPSHOT_MSG_CTRL_NEXT )==1 );
  FD_TEST( before_frag( &cl->ctx[ 0 ], 0UL, 0UL, FD_SNAPSHOT_MSG_CTRL_FAIL )==0 );

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FAIL );
  for( ulong t=0UL; t<n; t++ ) FD_TEST( cl->ctx[ t ].state==FD_SNAPSHOT_STATE_IDLE );

  test_cluster_delete( cl );
}

static void
test_fini_storage_error_retries( void ) {
  test_cluster_t * cl = test_cluster_new( 1UL, 1UL );
  test_counters_reset();
  test_io_reset();
  test_stream_init( 1UL );

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  ulong owner[ TEST_AV_MAX ];
  cluster_stream( cl, TEST_ORDER_ROUND_ROBIN, owner );

  uchar data = 1U;
  FD_TEST( !buffer_write( &cl->ctx[ 0 ].writer, 0UL, &data, 1UL ) );
  test_pwrite_push( -1L, ENOSPC );

  ulong pub0 = test_pub_cnt;
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( cl->ctx[ 0 ].state==FD_SNAPSHOT_STATE_ERROR );
  FD_TEST( test_pub_cnt==pub0+1UL );
  FD_TEST( test_pub_sig[ pub0 ]==FD_SNAPSHOT_MSG_CTRL_ERROR );

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( cl->ctx[ 0 ].state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( !cl->ctx[ 0 ].writer.buf_used );

  test_counters_reset();
  test_io_reset();
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( cl->ctx[ 0 ].state==FD_SNAPSHOT_STATE_PROCESSING );
  FD_TEST( !cl->ctx[ 0 ].writer.buf_used );

  test_cluster_delete( cl );
}

/* Equal-slot cross-appendvec duplicates are accepted, not fatal (see
   the eq-slot branch in fd_accdb_snapshot_write_batch_worker): the
   load completes and every tile, including the one that saw the
   duplicates, acks FINI normally and folds its counters. */
static void
test_eq_slot_fini_accepts( void ) {
  ulong const n = 4UL;
  ulong const T = 6UL;

  /* Drive the duplicate from a non-zero tile: counting is per tile,
     not a tile-0 privilege. */
  for( ulong bad=1UL; bad<n; bad+=2UL ) {
    test_cluster_t * cl = test_cluster_new( n, 1UL );
    test_counters_reset();
    test_stream_init( T );

    cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
    ulong owner[ TEST_AV_MAX ];
    cluster_stream( cl, TEST_ORDER_ROUND_ROBIN, owner );

    for( ulong t=0UL; t<n; t++ ) cl->ctx[ t ].worker.accounts_loaded = 10UL;
    cl->ctx[ bad ].worker_metrics->eq_slot_dups = 3UL;

    ulong pub0 = test_pub_cnt;
    cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FINI );

    /* Every tile acks FINI, no ERROR, no withheld fold. */
    FD_TEST( test_pub_cnt==pub0+n );
    for( ulong t=0UL; t<n; t++ ) FD_TEST( test_pub_sig[ pub0+t ]==FD_SNAPSHOT_MSG_CTRL_FINI );
    for( ulong t=0UL; t<n; t++ ) FD_TEST( cl->ctx[ t ].state==FD_SNAPSHOT_STATE_FINISHING );

    /* The flagging tile's counters, dups included, are folded like
       everyone else's. */
    FD_TEST( cl->shmem->totals.accounts_loaded==10UL*n );
    FD_TEST( cl->shmem->totals.eq_slot_dups==3UL );
    FD_TEST( cl->shmem->totals.appendvecs_processed==T );

    test_cluster_delete( cl );
  }
}

/* Accumulator fold ****************************************************/

/* Every tile FD_ATOMIC_FETCH_AND_ADDs its FINI-time locals straight
   into hdr->totals, and tile 0 reads that fold at NEXT: known per-tile
   locals must produce exact totals and exact derived tile-0 gauges. */
static void
test_accumulator_fold( void ) {
  ulong const n = 4UL;
  ulong const T = 8UL;
  ulong const bank_slot = 440123518UL;

  test_cluster_t * cl = test_cluster_new( n, 1UL );
  test_counters_reset();
  test_stream_init( T );

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  ulong owner[ TEST_AV_MAX ];
  cluster_stream( cl, TEST_ORDER_ROUND_ROBIN, owner );

  /* Known per-tile locals.  owned_appendvecs is left as the stream walk
     produced it: tile 0's fold asserts the claim identity against it. */
  ulong exp_loaded=0UL, exp_replaced=0UL, exp_ignored=0UL;
  ulong exp_input=0UL, exp_repl_l=0UL, exp_ign_l=0UL;
  for( ulong t=0UL; t<n; t++ ) {
    fd_snapin_tile_t * ctx = &cl->ctx[ t ];
    ctx->worker.accounts_loaded   = 1000UL+t;
    ctx->worker.accounts_replaced =   20UL+t;
    ctx->worker.accounts_ignored  =    3UL+t;
    ctx->worker.input_lamports    = 1000000UL*(t+1UL);
    ctx->worker.replaced_lamports =   5000UL*(t+1UL);
    ctx->worker.ignored_lamports  =    700UL*(t+1UL);
    /* Gauges the tile must keep live through FINI: the dashboards sum
       them across all snapin tiles, so a per-tile dip at the barrier (or
       tile 0 folding the cross-tile total into its own) breaks them. */
    ctx->metrics.accounts_loaded   = 77UL;
    ctx->metrics.accounts_replaced = 78UL;
    ctx->metrics.accounts_ignored  = 79UL;

    exp_loaded   += ctx->worker.accounts_loaded;
    exp_replaced += ctx->worker.accounts_replaced;
    exp_ignored  += ctx->worker.accounts_ignored;
    exp_input    += ctx->worker.input_lamports;
    exp_repl_l   += ctx->worker.replaced_lamports;
    exp_ign_l    += ctx->worker.ignored_lamports;
  }

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FINI );

  fd_snapin_shmem_totals_t const * tot = &cl->shmem->totals;
  FD_TEST( tot->accounts_loaded      ==exp_loaded   );
  FD_TEST( tot->accounts_replaced    ==exp_replaced );
  FD_TEST( tot->accounts_ignored     ==exp_ignored  );
  FD_TEST( tot->input_lamports       ==exp_input    );
  FD_TEST( tot->replaced_lamports    ==exp_repl_l   );
  FD_TEST( tot->ignored_lamports     ==exp_ign_l    );
  FD_TEST( tot->appendvecs_processed ==T            );
  FD_TEST( !tot->eq_slot_dups );
  for( ulong t=0UL; t<n; t++ ) {
    FD_TEST( cl->ctx[ t ].metrics.accounts_loaded  ==77UL );
    FD_TEST( cl->ctx[ t ].metrics.accounts_replaced==78UL );
    FD_TEST( cl->ctx[ t ].metrics.accounts_ignored ==79UL );
  }

  /* Tile 0 reads the fold at NEXT.  A full attempt's capitalization is
     input - ignored - replaced; the manifest value it is checked
     against is what process_manifest would have parsed. */
  test_stamp_slot_history( cl, bank_slot );
  cl->ctx[ 0 ].lead.manifest_capitalization = exp_input-exp_ign_l-exp_repl_l;

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_NEXT );

  fd_snapin_tile_t * t0 = &cl->ctx[ 0 ];
  FD_TEST( t0->state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( t0->lead.attempt_folded );
  /* The cross-tile fold lands in tile 0's diagnostic totals, NOT in its
     gauge: the gauges stay per-tile so the dashboards' sum is exact. */
  FD_TEST( t0->lead.totals_fold.accounts_loaded  ==exp_loaded   );
  FD_TEST( t0->lead.totals_fold.accounts_replaced==exp_replaced );
  FD_TEST( t0->lead.totals_fold.accounts_ignored ==exp_ignored  );
  FD_TEST( t0->lead.dup_capitalization       ==exp_repl_l   );
  FD_TEST( t0->lead.capitalization           ==exp_input-exp_ign_l-exp_repl_l );
  FD_TEST( !t0->lead.worker_fold.eq_slot_dups );
  /* The full snapshot's totals are saved for the incremental revert. */
  FD_TEST( t0->lead.recovery.capitalization==t0->lead.capitalization );
  /* Every tile latched its own share, and the cross-tile sum is
     unchanged by the barrier -- that is the continuity the GUI and the
     snapshot-load watch depend on. */
  ulong gauge_sum = 0UL;
  for( ulong t=0UL; t<n; t++ ) {
    FD_TEST( cl->ctx[ t ].metrics.full_accounts_loaded  ==77UL );
    FD_TEST( cl->ctx[ t ].metrics.accounts_loaded       ==77UL );
    gauge_sum += cl->ctx[ t ].metrics.accounts_loaded;
  }
  FD_TEST( gauge_sum==77UL*n );
  FD_TEST( t0->lead.slot_history.captured );
  FD_TEST( t0->lead.slot_history.data_len==FD_SYSVAR_SLOT_HISTORY_BINCODE_SZ );

  test_cluster_delete( cl );
}

/* The per-tile ACCOUNT_LOADED gauges must never dip mid-load: the GUI
   and the snapshot-load watch sum them across all snapin tiles and take
   deltas off that sum.  Walk a full load, an incremental load and a
   failed-then-retried incremental, sampling the sum at every barrier. */
static void
test_gauge_sum_continuity( void ) {
  ulong const n = 4UL;
  ulong const T = 6UL;
  ulong const bank_slot = 440123518UL;

  test_cluster_t * cl = test_cluster_new( n, 1UL );
  test_counters_reset();
  test_stream_init( T );
  ulong owner[ TEST_AV_MAX ];

# define GAUGE_SUM() (__extension__({                                            \
    ulong _s = 0UL;                                                              \
    for( ulong _t=0UL; _t<n; _t++ ) _s += cl->ctx[ _t ].metrics.accounts_loaded;  \
    _s; }))

  /* --- Full load ------------------------------------------------- */
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( !GAUGE_SUM() );
  cluster_stream( cl, TEST_ORDER_ROUND_ROBIN, owner );

  ulong full_share = 100UL;
  for( ulong t=0UL; t<n; t++ ) {
    cl->ctx[ t ].metrics.accounts_loaded = full_share;
    cl->ctx[ t ].worker.accounts_loaded  = full_share;
  }
  FD_TEST( GAUGE_SUM()==full_share*n );

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( GAUGE_SUM()==full_share*n );   /* was ~0 before: every tile zeroed here */

  test_stamp_slot_history( cl, bank_slot );
  cl->ctx[ 0 ].lead.manifest_capitalization = 0UL;
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_NEXT );
  FD_TEST( GAUGE_SUM()==full_share*n );   /* was full_share*n + the fold: double counted */
  FD_TEST( cl->ctx[ 0 ].lead.totals_fold.accounts_loaded==full_share*n );

  /* --- Incremental that fails ------------------------------------ */
  test_counters_reset();
  test_stream_init( T );
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( GAUGE_SUM()==full_share*n );   /* resumes from the full share */

  for( ulong t=0UL; t<n; t++ ) cl->ctx[ t ].metrics.accounts_loaded += 7UL;
  FD_TEST( GAUGE_SUM()==(full_share+7UL)*n );
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( GAUGE_SUM()==(full_share+7UL)*n ); /* FAIL alone does not rewind */

  /* --- Incremental retry ----------------------------------------- */
  test_counters_reset();
  test_stream_init( T );
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  /* The retry's INIT is the one point the sum steps back, and only by
     the failed attempt's own contribution -- never to zero. */
  FD_TEST( GAUGE_SUM()==full_share*n );

  cluster_stream( cl, TEST_ORDER_ROUND_ROBIN, owner );
  ulong incr_share = 11UL;
  for( ulong t=0UL; t<n; t++ ) {
    cl->ctx[ t ].metrics.accounts_loaded += incr_share;
    cl->ctx[ t ].worker.accounts_loaded   = incr_share;
  }
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( GAUGE_SUM()==(full_share+incr_share)*n );

  test_stamp_slot_history( cl, bank_slot );
  cl->ctx[ 0 ].lead.manifest_capitalization = cl->ctx[ 0 ].lead.recovery.capitalization;
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_DONE );
  FD_TEST( GAUGE_SUM()==(full_share+incr_share)*n );
  /* Tile 0's diagnostic total accumulates over the session. */
  FD_TEST( cl->ctx[ 0 ].lead.totals_fold.accounts_loaded==(full_share+incr_share)*n );

# undef GAUGE_SUM

  test_cluster_delete( cl );
}

/* Full lifecycle ******************************************************/

/* Nine tiles through a whole load: full attempt, an incremental
   attempt that fails and is retried, then the successful incremental
   promotion.  Pins the cross-attempt bookkeeping the individual cases
   above only touch in isolation. */
static void
test_full_lifecycle_9_tiles( void ) {
  ulong const n = 9UL;
  ulong const T = 11UL;
  ulong const bank_slot = 440123518UL;

  test_cluster_t * cl = test_cluster_new( n, 2UL );
  test_counters_reset();
  test_stream_init( T );
  ulong owner[ TEST_AV_MAX ];

  /* --- Full attempt --------------------------------------------- */
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_FULL );
  FD_TEST( test_accdb_reset_cnt==1UL );
  FD_TEST( test_accdb_load_begin_cnt==1UL );
  FD_TEST( test_accdb_writer_begin_cnt==1UL );   /* tile 0 only; the rest are gated */
  FD_TEST( cl->shmem->next_appendvec==1UL );
  FD_TEST( cl->ctx[ 0 ].incr_fork==(ulong)USHORT_MAX );

  cluster_stream( cl, TEST_ORDER_ROUND_ROBIN, owner );
  FD_TEST( test_accdb_writer_begin_cnt==n );     /* every gate opened on first data */
  for( ulong t=0UL; t<n; t++ ) FD_TEST( cl->ctx[ t ].incr_fork==(ulong)USHORT_MAX );
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( test_accdb_worker_close_cnt==n );
  FD_TEST( test_accdb_writer_end_cnt==n );
  FD_TEST( cl->shmem->totals.appendvecs_processed==T );

  test_stamp_slot_history( cl, bank_slot );
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_NEXT );
  for( ulong t=0UL; t<n; t++ ) FD_TEST( cl->ctx[ t ].state==FD_SNAPSHOT_STATE_IDLE );
  FD_TEST( !cl->ctx[ 0 ].lead.init_completed );

  /* --- Incremental attempt that fails --------------------------- */
  test_counters_reset();
  test_stream_init( T );
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( !test_accdb_reset_cnt );
  FD_TEST( test_accdb_attach_cnt==1UL );          /* child fork for the incremental writes */
  FD_TEST( cl->shmem->attempt.fork_id==7UL );
  FD_TEST( cl->ctx[ 0 ].incr_fork==7UL );
  FD_TEST( cl->shmem->next_appendvec==1UL );

  for( ulong step=0UL; step<4UL; step++ ) {
    for( ulong t=0UL; t<n; t++ ) (void)tile_step( &cl->ctx[ t ] );
  }
  for( ulong t=0UL; t<n; t++ ) FD_TEST( cl->ctx[ t ].incr_fork==7UL );
  ulong exp_release = 0UL;
  for( ulong t=0UL; t<n; t++ ) {
    cl->ctx[ t ].whead.attempt_partition_cnt = 1UL+t;
    exp_release += 1UL+t;
  }
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FAIL );
  FD_TEST( cl->ctx[ 0 ].lead.rollback.pending );
  FD_TEST( !cl->ctx[ 0 ].lead.rollback.full );
  FD_TEST( cl->ctx[ 0 ].lead.accdb_incr_fork_id.val==USHORT_MAX );

  /* --- Incremental retry ---------------------------------------- */
  test_counters_reset();
  test_stream_init( T );
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_INIT_INCR );
  FD_TEST( test_accdb_purge_cnt==1UL );                    /* failed fork purged */
  FD_TEST( test_accdb_release_cnt==1UL );                  /* its partitions released */
  FD_TEST( test_accdb_release_total==exp_release );
  FD_TEST( !cl->ctx[ 0 ].lead.doomed_partition_cnt );
  for( ulong t=0UL; t<n; t++ ) FD_TEST( !cl->ctx[ t ].shmem_worker->fail_partition_cnt );
  FD_TEST( cl->shmem->next_appendvec==1UL );

  cluster_stream( cl, TEST_ORDER_REVERSE, owner );
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_FINI );
  FD_TEST( cl->shmem->totals.appendvecs_processed==T );
  FD_TEST( cl->shmem->next_appendvec==T+n );

  /* An incremental load's capitalization starts from the full
     snapshot's saved total; nothing was inserted here, so it is
     unchanged. */
  test_stamp_slot_history( cl, bank_slot );
  cl->ctx[ 0 ].lead.manifest_capitalization = cl->ctx[ 0 ].lead.recovery.capitalization;

  ulong pub0 = test_pub_cnt;
  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_DONE );
  FD_TEST( test_accdb_readback_cnt==1UL );
  FD_TEST( test_accdb_recover_delta_cnt==1UL );
  FD_TEST( test_accdb_advance_root_cnt==1UL );
  FD_TEST( test_accdb_load_end_cnt==1UL );
  FD_TEST( test_feature_finalize_cnt==1UL );
  FD_TEST( cl->ctx[ 0 ].lead.accdb_root_fork_id.val==7U );
  FD_TEST( cl->ctx[ 0 ].lead.accdb_incr_fork_id.val==USHORT_MAX );
  /* n DONE acks plus tile 0's replay notification on snapin_manif. */
  FD_TEST( test_pub_cnt==pub0+n+1UL );
  ulong manif_pubs = 0UL;
  for( ulong i=pub0; i<test_pub_cnt; i++ ) manif_pubs += test_pub_out_idx[ i ]==0UL;
  FD_TEST( manif_pubs==1UL );

  cluster_barrier( cl, FD_SNAPSHOT_MSG_CTRL_SHUTDOWN );
  for( ulong t=0UL; t<n; t++ ) {
    FD_TEST( cl->ctx[ t ].state==FD_SNAPSHOT_STATE_SHUTDOWN );
    FD_TEST( should_shutdown( &cl->ctx[ t ] ) );
  }

  test_cluster_delete( cl );
}

static void
test_writer_short_write_and_eintr( void ) {
  uchar data[ 5UL ] = { 1, 2, 3, 4, 5 };
  snapin_writer_t * writer = test_writer;

  test_io_reset();
  test_pwrite_push( -1L, EINTR );
  test_pwrite_push(  2L, 0     );
  test_pwrite_push(  3L, 0     );

  writer_init( writer, FD_ACCDB_FD_RW );
  writer_begin( writer );
  FD_TEST( !buffer_write( writer, 10UL, data, sizeof(data) ) );
  FD_TEST( !writer_end( writer ) );
  FD_TEST( test_pwrite_call_cnt==3UL );
  FD_TEST( writer->bytes_written==sizeof(data) );
  FD_TEST( !writer->buf_used );
}

static void
test_writer_errors( void ) {
  uchar data[ 4UL ] = {0};
  snapin_writer_t * writer = test_writer;

  test_io_reset();
  writer_init( writer, FD_ACCDB_FD_RW );
  writer_begin( writer );
  test_pwrite_push( -1L, ENOSPC );
  FD_TEST( !buffer_write( writer, 0UL, data, sizeof(data) ) );
  FD_TEST( writer_end( writer )==-1 );
  writer_abort( writer );
  FD_TEST( !writer->buf_used );

  test_io_reset();
  writer_init( writer, FD_ACCDB_FD_RW );
  writer_begin( writer );
  test_pwrite_push(  2L, 0   );
  test_pwrite_push( -1L, EIO );
  FD_TEST( !buffer_write( writer, 0UL, data, sizeof(data) ) );
  FD_TEST( writer_end( writer )==-1 );
  FD_TEST( writer->bytes_written==2UL );
  FD_TEST( writer->buf_off==2UL && writer->buf_used==2UL );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  test_writer_short_write_and_eintr();
  test_writer_errors();
  test_scratch_layout_fits();

  /* The end-to-end populate test holds a full-size txncache (~1.3 GiB)
     and the staged transaction entries (~0.55 GiB) at once. */
  char const * _page_sz = fd_env_strip_cmdline_cstr ( &argc, &argv, "--page-sz",  NULL, "gigantic"               );
  ulong        page_cnt = fd_env_strip_cmdline_ulong( &argc, &argv, "--page-cnt", NULL, 3UL                      );
  ulong        numa_idx = fd_env_strip_cmdline_ulong( &argc, &argv, "--numa-idx", NULL, fd_shmem_numa_idx( 0UL ) );
  fd_wksp_t * wksp      = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ), page_cnt, fd_shmem_cpu_idx( numa_idx ), "wksp", 0UL );
  FD_TEST( wksp );

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
  fd_wksp_reset( wksp, 1UL ); test_batch_stake_delegation( wksp );
  fd_wksp_reset( wksp, 1UL ); test_streaming_stake_delegation( wksp );
  test_txncache_staging_entry_size();
  test_txncache_staging_group_record_size();
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_groups_fit_txncache_scratch( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_evicts_oldest_slot( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_evicted_slot_drops_groups( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_rejects_group_overflow( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_rejects_entry_overflow( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_evicted_entries_not_pooled( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_reclaims_evicted_entries( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_filters_recent_groups( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_rejects_recent_group_overflow( wksp );
  test_txncache_staging_fits_one_gigantic_page();
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_runtime_limits( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_rejects_conflicting_group_offsets( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_ignores_evicted_group_offsets( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_populate_inserts_recent_only( wksp );
  fd_wksp_reset( wksp, 1UL ); test_txncache_staging_populate_rejects_newest_blockhash_entries( wksp );

  fd_wksp_delete_anonymous( wksp );

  test_init_gate_holds_data();
  test_init_aborted_barrier_retries();
  test_init_gate_rejects_stale_generation();
  test_init_publishes_after_reset();
  test_eager_claim_coverage();
  test_retry_resets();
  test_fini_truncated_malform();
  test_fini_storage_error_retries();
  test_eq_slot_fini_accepts();
  test_accumulator_fold();
  test_gauge_sum_continuity();
  test_full_lifecycle_9_tiles();

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
