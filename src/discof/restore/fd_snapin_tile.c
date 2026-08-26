#define _GNU_SOURCE /* sync_file_range */
#include "utils/fd_ssctrl.h"
#include "utils/fd_ssload.h"
#include "utils/fd_ssmsg.h"
#include "utils/fd_ssparse.h"
#include "utils/fd_ssmanifest_parser.h"
#include "utils/fd_slot_delta_parser.h"
#include "utils/fd_snapin_io.h"
#include "../../util/fd_hash32.h"

#include "../../disco/topo/fd_topo.h"
#include "../../disco/metrics/fd_metrics.h"
#include "../../disco/gui/fd_gui_config_parse.h"
#include "../../flamenco/runtime/fd_txncache.h"
#include "../../flamenco/runtime/fd_system_ids.h"
#include "../../flamenco/runtime/fd_hashes.h"
#include "../../flamenco/runtime/sysvar/fd_sysvar_epoch_schedule.h"
#include "../../flamenco/runtime/sysvar/fd_sysvar_slot_history.h"

#include "../../flamenco/runtime/fd_txncache.h"
#include "../../flamenco/runtime/fd_bank.h"
#include "../../flamenco/features/fd_feature_snoop.h"
#include "../../flamenco/stakes/fd_stake_types.h"
#include "../../disco/stem/fd_stem.h"
#include "../../flamenco/accdb/fd_accdb.h"
#include "../../disco/events/generated/fd_event_gen.h"

#include "generated/fd_snapin_tile_seccomp.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#define NAME "snapin"

#define FD_SNAPIN_ROLE_COORDINATOR  (0)
#define FD_SNAPIN_ROLE_ACCDB_WORKER (1)
#define FD_SNAPIN_WORKER_MAX        (FD_SNAPIN_TILE_MAX-1UL)

/* Per-worker staging buffer for buffered pwrites into the worker's own
   accdb partitions (same size as the deleted snapwr tile's buffer). */
#define FD_SNAPIN_WRITE_BUF_SZ      (2UL<<20)

/* Write-behind: workers kick async writeback of their flushed records
   in large contiguous runs and bound their in-flight (kicked but not
   yet completed) dirty bytes.  Without this, N workers writing ~9 GB/s
   aggregate into the page cache outrun the array's sustained multi-
   stream writeback rate; once the accumulated dirty pages cross the
   kernel's balance_dirty_pages engagement point, every pwrite gets
   throttled with coarse (up to ~100 ms) sleeps -- and a sleeping
   worker freezes its lane fseqs (worker fseq == scan position), which
   convoys the ENTIRE pipe (measured on the n9 bench: raw collapsed
   from 9.3 GB/s to an oscillating 1.4-7 GB/s once ~100 GB of dirty
   accumulated ~22 s in).  The write-behind backstop replaces those
   coarse kernel sleeps with smooth 64 MiB-granular self-throttling to
   the device, and starting writeback immediately maximizes the bytes
   drained during the load.  The aggregate window stays below the
   throttle engagement point ((dirty_background_ratio+dirty_ratio)/2 =
   6.5% of RAM ~ 98 GB on the bench box) while preserving most of the
   page-cache elasticity the 4-worker arm relies on. */
#define FD_SNAPIN_WB_KICK_SZ      (64UL<<20) /* kick writeback per this many contiguous flushed bytes */
#define FD_SNAPIN_WB_TOTAL_WINDOW (80UL<<30) /* aggregate kicked-not-waited budget across all workers */
#define FD_SNAPIN_WB_RING_CNT     (4096UL)   /* max outstanding kicked ranges (pow2) */

/* The snapin tile is a state machine that parses and loads a full
   and optionally an incremental snapshot.  It is currently responsible
   for loading accounts into an in-memory database, though this may
   change. */

/* 300 root slots in the slot deltas array, and each one references all
   151 prior blockhashes that it's able to. */
#define FD_SNAPIN_MAX_SLOT_DELTA_GROUPS (300UL*151UL)

struct fd_blockhash_entry {
  fd_hash_t blockhash;

  struct {
    ulong prev;
    ulong next;
  } map;
};

typedef struct fd_blockhash_entry fd_blockhash_entry_t;

#define MAP_NAME                           blockhash_map
#define MAP_KEY                            blockhash
#define MAP_KEY_T                          fd_hash_t
#define MAP_ELE_T                          fd_blockhash_entry_t
#define MAP_KEY_EQ(k0,k1)                  (!memcmp((k0),(k1), sizeof(fd_hash_t)))
#define MAP_KEY_HASH(key,seed)             (fd_hash32( (key)->uc, (seed) ))
#define MAP_PREV                           map.prev
#define MAP_NEXT                           map.next
#define MAP_OPTIMIZE_RANDOM_ACCESS_REMOVAL 1
#include "../../util/tmpl/fd_map_chain.c"

/* For a transaction to be valid to be inserted into the txncache, it
   must reference a blockhash that is in the set of recent blockhashes.
   This means that only transactions executed in the latest 151 slots
   can be in the txncache: the remaining entries can be ignored. */
#define FD_SNAPIN_TXNCACHE_MAX_ENTRIES (FD_TXNCACHE_MAX_SLOT_DELTAS*FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT)

FD_STATIC_ASSERT( FD_TXNCACHE_MAX_SLOT_DELTAS<=FD_SLOT_DELTA_MAX_ENTRIES, txncache_staging_slot_cnt );

struct blockhash_group {
  uchar blockhash[ 32UL ];
  ulong slot;
  ulong txnhash_offset;
  ulong txncache_entry_idx;
  ulong txncache_entry_cnt;
};

typedef struct blockhash_group blockhash_group_t;

struct txncache_staging_slot {
  ulong slot;
  ulong entry_cnt;
};

typedef struct txncache_staging_slot txncache_staging_slot_t;

struct fd_snapin_out_link {
  ulong       idx;
  fd_wksp_t * mem;
  ulong       chunk0;
  ulong       wmark;
  ulong       chunk;
  ulong       mtu;
};
typedef struct fd_snapin_out_link fd_snapin_out_link_t;

/* Parallel-loader state (D9 tar-boundary sharding).  The snapin tile
   has two roles, selected by kind_id:

     kind 0 (coordinator): the existing snapin state machine.  With
       workers attached it runs the ssparse appendvec passthrough: it
       never parses individual accounts, but instead assigns whole
       appendvecs to workers at tar-header parse time (ASSIGN on the
       owning worker's snapin_io ring) and publishes a scalar
       stream-offset coverage watermark ("assignments complete below X")
       to every worker.  It still fully parses/verifies the manifest +
       status cache, owns the control protocol with snapct (forwarding
       each control only after all workers acked it on their own lane
       barriers), and merges the workers' snoop staging at end of load.

     kind 1..N (worker): a fused parse+insert+write loader.  It consumes
       the same snapdc_in lanes (full reliable consumer with the same
       expected-frame rotation), holds its stream cursor behind the
       published byte coverage (returnable-frag hold), parses the
       appendvecs assigned to it via fd_ssparse_accv_init, inserts
       accounts via the striped-lock decide-then-allocate accdb path,
       and pwrite()s the packed disk records at its own explicit
       offsets. */

#define FD_SNAPIN_FIFO_CNT (1UL<<15) /* pending owned appendvecs; covers the max in-flight fctl window */

struct fd_snapin_extent {
  ulong body_off;      /* stream offset of appendvec body */
  ulong body_sz;       /* tar entry size in bytes */
  ulong slot;
  ulong appendvec_idx; /* stream sequence number */
};

typedef struct fd_snapin_extent fd_snapin_extent_t;

FD_STATIC_ASSERT( FD_SNAPIN_WORKER_MAX<=FD_SNAPIO_WORKER_MAX, snapin_worker_max );

struct fd_snapin_tile {
  int  role;
  int  io_enabled;
  int  state;
  uint full           : 1;  /* loading a full snapshot? */
  uint init_completed : 1;  /* did INIT complete for this attempt? */

  ulong lane_cnt;
  ulong worker_idx;
  ulong worker_cnt;
  ulong ack_in_idx[ FD_SNAPIN_WORKER_MAX ];
  ulong generation;         /* attempt counter, bumped at the first INIT frag (both roles, in lockstep via their lane barriers) */
  ulong pending_worker_control;
  ulong pending_worker_ack_mask;
  ulong expected_frame;
  ulong pending_control;    /* control message expected from snapdc tiles */
  uchar control_seen[ FD_TOPO_MAX_TILE_IN_LINKS ]; /* lane indexed */

  ulong seed;
  long boot_timestamp;

  fd_accdb_t *    accdb;
  fd_txncache_t * txncache;

  fd_banks_t * banks;
  fd_bank_t *  bank;

  fd_feature_snoop_t feature_snoop[1];
  struct {
    int         capturing;
    fd_pubkey_t pubkey;
    ulong       lamports;
    uchar       owner[ 32UL ];
    ulong       need;
    ulong       write_pos;
    uchar       buf[ sizeof(fd_feature_t) ];
  } feature_reasm;
  struct {
    int         capturing;
    fd_pubkey_t pubkey;
    ulong       lamports;
    ulong       data_len;
    ulong       write_pos;
    uchar       buf[ sizeof(fd_stake_state_t) ];
  } stake_reasm;

  fd_ssparse_t             ssparse[1];
  fd_ssmanifest_parser_t * manifest_parser;
  fd_slot_delta_parser_t * slot_delta_parser;

  struct {
    int manifest_done;
    int status_cache_done;
    int manifest_processed;
  } flags;

  ulong advertised_slot;
  ulong bank_slot;
  ulong epoch;

  fd_epoch_schedule_t epoch_schedule;

  ulong full_genesis_creation_time_seconds;
  uchar advertised_hash[ FD_HASH_FOOTPRINT ];

  ulong capitalization;          /* tracks capitalization of all loaded accounts in the current snapshot */
  ulong dup_capitalization;      /* tracks capitalization of duplicate accounts encountered during incremental snapshot loading */
  ulong manifest_capitalization; /* capitalization according to the current snapshot manifest */

  struct {
    ulong                        capitalization;
    fd_accdb_snapshot_recovery_t accdb_metadata;
    fd_feature_snoop_t           feature_snoop;
  } recovery; /* stores state from the last full snapshot for incremental revert */

  ulong               blockhash_groups_len;
  blockhash_group_t * blockhash_groups;

  int alpenglow;

  fd_sstxncache_hash_t *  txncache_entries;
  txncache_staging_slot_t txncache_slots[ FD_TXNCACHE_MAX_SLOT_DELTAS ];
  ulong                   txncache_slots_len;
  ulong                   txncache_current_slot_idx;
  ulong                   txncache_current_slot_entry_cnt;

  fd_accdb_fork_id_t accdb_root_fork_id;
  fd_accdb_fork_id_t accdb_incr_fork_id; /* child fork for incremental writes (purge on failure) */
  fd_txncache_fork_id_t txncache_root_fork_id;

  struct {
    ulong full_bytes_read;
    ulong incremental_bytes_read;

    /* Account counters (full + incremental) */
    ulong accounts_loaded;
    ulong accounts_replaced;
    ulong accounts_ignored;

    /* Account counters (snapshot taken for full snapshot only) */
    ulong full_accounts_loaded;
    ulong full_accounts_replaced;
    ulong full_accounts_ignored;

    /* Persistent counters */
    ulong total_accounts_processed;
    ulong total_account_batches_processed;
  } metrics;

  struct {
    fd_wksp_t * wksp;
    ulong       chunk0;
    ulong       wmark;
    ulong       mtu;
    ulong       pos;
  } in[ FD_TOPO_MAX_TILE_IN_LINKS ];

  fd_snapin_out_link_t ct_out;
  fd_snapin_out_link_t manifest_out;
  fd_snapin_out_link_t gui_out;
  fd_snapin_out_link_t io_out[ FD_SNAPIN_WORKER_MAX ];
  fd_snapin_out_link_t ack_out;

  struct {
    fd_wksp_t * wksp;
    ulong       chunk0;
    ulong       wmark;
    ulong       mtu;
  } io_in[ FD_SNAPIN_WORKER_MAX ];

  ulong io_pub_cnt; /* snapin_io publishes so far in the current data
                       frag callback, to respect STEM_BURST.  A frag
                       packed with tiny appendvecs can otherwise publish
                       more than STEM_BURST jobs. */

  /* Coordinator (io_enabled): tar-boundary sharding state. */
  ulong stream_cursor;             /* decompressed-stream bytes consumed */
  ulong appendvec_seq;
  ulong covered_until;             /* coordinator: end of last fully-assigned entry;
                                      worker: assignments complete below this offset */
  int   io_watermark_dirty;        /* watermark advanced since last broadcast */
  ulong io_frags_since_watermark;  /* consumed lane frags since last broadcast */
  ulong io_jobs_since_credit;      /* ring publishes since last after_credit */
  int   abort_published;           /* ABORT published for this attempt? */
  int   snoop_merged;              /* snoop staging merged for this attempt? */
  ulong worker_assigned_bytes[ FD_SNAPIN_WORKER_MAX ];
  fd_snapio_worker_snoop_t * snoops[ FD_SNAPIN_WORKER_MAX ];
  struct {
    ulong eq_slot_dups;
    ulong eq_slot_lamports_diff;
    ulong bytes_written;
  } worker_fold; /* folded from FINI acks */
  struct {       /* step-0 measurement: appendvec size distribution */
    ulong cnt;
    ulong bytes;
    ulong max_sz;
    ulong over_64m_cnt;
    ulong over_256m_bytes;
    ulong log2_hist[ 48 ];
  } av_stats;

  struct {
    ulong input_lamports;
    ulong replaced_lamports;
    ulong ignored_lamports;
  } worker;

  /* Worker: coverage-gated appendvec scan + write engine. */
  ulong job_in_idx;                                /* in link carrying ring jobs */
  ulong in_lane[ FD_TOPO_MAX_TILE_IN_LINKS ];      /* in_idx -> lane (ULONG_MAX for job link) */
  int   eos_seen;
  int   pending_fini;                              /* FINI barrier complete, ack deferred until work drains */
  ulong incr_fork;                                 /* accdb fork adopted from this attempt's ASSIGNs (ULONG_MAX = none yet) */
  ulong cursor;                                    /* stream offset consumed by this worker */
  fd_snapin_extent_t * fifo;                       /* owned appendvecs not yet started */
  ulong fifo_head;
  ulong fifo_tail;
  int   av_active;
  fd_snapin_extent_t cur_av;
  ulong av_consumed;                               /* bytes of cur_av body consumed */
  ulong rec_idx;                                   /* record index within cur_av */
  fd_accdb_snapshot_whead_t whead;                 /* private layer-0 write head */
  uchar * write_buf;
  ulong   write_buf_used;
  ulong   flush_off;                               /* file offset of write_buf[0] */
  ulong   bytes_written;
  fd_accdb_snapshot_worker_metrics_t worker_metrics[1];
  fd_snapio_worker_snoop_t * my_snoop;
  ulong stake_log_max;
  struct {                                         /* streaming-path slot history capture */
    int   capturing;
    ulong write_pos;
    ulong data_len;
  } sh_reasm;
  ulong reasm_av_idx;                              /* stream tags captured at arm time */
  ulong reasm_rec_idx;
  struct {
    int   accepted;    /* 0 = ignored duplicate: drop the data bytes */
    ulong received;
    ulong file_off;    /* allocated meta offset; data at +sizeof(disk_meta) */
  } open_acc;
  struct {                                         /* day-1 instrumentation (logged at FINI) */
    ulong hold_reprocess;                          /* coverage-hold lane reprocesses */
    ulong lag_samples;                             /* coverage lag sampled at each coverage advance */
    ulong lag_sum;
    ulong lag_min;
    ulong lag_max;
  } cov_stats;

  /* Worker: write-behind (bounded in-flight dirty bytes). */
  ulong wb_kick_sz;                                /* kick granularity (test override) */
  ulong wb_window;                                 /* per-worker in-flight cap; 0 disables */
  ulong wb_run_off;                                /* current un-kicked contiguous flushed run */
  ulong wb_run_sz;
  ulong wb_pending;                                /* kicked but not yet waited-on bytes */
  ulong wb_head;                                   /* ring of kicked ranges */
  ulong wb_tail;
  ulong wb_kick_cnt;                               /* instrumentation */
  ulong wb_wait_cnt;
  struct { ulong off; ulong sz; } wb_ring[ FD_SNAPIN_WB_RING_CNT ];

  /* both roles */
  uint * stripe_locks;

  ulong gui_config_acct_sz;   /* total expected account data length (0 when not accumulating) */
  ulong gui_config_acct_off;  /* bytes accumulated so far into the current gui_out link chunk */

  /* In-memory copy of the SlotHistory sysvar account, captured by
     snooping the account stream as the snapshot is loaded.  The accdb
     read-back path is unsafe at the end of load because the snapwr
     tile may not have flushed the bytes yet; the snoop path observes
     the bytes directly.  The captured copy is then used by
     verify_slot_deltas_with_slot_history.

     Replacement uses the same precedence as fd_accdb_snapshot_write_*:
     a write with slot >= captured.slot replaces the captured copy.
     This handles the incremental snapshot superseding the full. */
  struct {
    int   captured;
    int   capturing; /* streaming-path: currently appending data for this account */
    ulong slot;
    ulong lamports;
    ulong data_len;
    uchar owner[ 32UL ];
    int   executable;
    ulong write_pos; /* bytes written into buf during the current streaming capture */
    uchar buf[ FD_SYSVAR_SLOT_HISTORY_BINCODE_SZ ];
  } slot_history;
};

typedef struct fd_snapin_tile fd_snapin_tile_t;

static inline int
is_accdb_worker( fd_snapin_tile_t const * ctx ) {
  return ctx->role!=FD_SNAPIN_ROLE_COORDINATOR;
}

static void
format_count( char * out, ulong out_sz, ulong n ) {
  if(      n>=1000000UL ) FD_TEST( fd_cstr_printf_check( out, out_sz, NULL, "%.1fM", (double)n/1e6 ) );
  else if( n>=1000UL    ) FD_TEST( fd_cstr_printf_check( out, out_sz, NULL, "%.1fK", (double)n/1e3 ) );
  else                    FD_TEST( fd_cstr_printf_check( out, out_sz, NULL, "%lu",   n             ) );
}

static inline int
should_shutdown( fd_snapin_tile_t * ctx ) {
  if( FD_UNLIKELY( is_accdb_worker( ctx ) ) ) return ctx->state==FD_SNAPSHOT_STATE_SHUTDOWN;
  int ready = ctx->state==FD_SNAPSHOT_STATE_SHUTDOWN &&
              ( !ctx->io_enabled || ctx->pending_worker_control==ULONG_MAX );
  if( FD_UNLIKELY( ready ) ) {
    ulong accounts_dup = ctx->metrics.accounts_ignored + ctx->metrics.accounts_replaced;
    long  elapsed_ns   = fd_log_wallclock() - ctx->boot_timestamp;
    char  loaded_buf[ 32 ];
    char  dup_buf   [ 32 ];
    format_count( loaded_buf, sizeof(loaded_buf), ctx->metrics.accounts_loaded );
    format_count( dup_buf,    sizeof(dup_buf),    accounts_dup                 );
    FD_LOG_NOTICE(( "loaded %s accounts %s(%s dups)%s from snapshot in %.3f seconds",
                    loaded_buf, fd_log_style_dim(), dup_buf, fd_log_style_normal(), (double)elapsed_ns/1e9 ));
  }
  return ready;
}

static ulong
scratch_align( void ) {
  /* Must cover the largest FD_LAYOUT_APPEND alignment in
     scratch_footprint (the 4096-aligned worker write buffer).  The
     footprint is computed from a zero base, so if the topology placed
     the tile object at a smaller alignment the runtime layout could
     consume up to align-scratch_align more bytes than the footprint and
     overflow into the next workspace object (with >=2 workers that is
     the next worker tile's ctx). */
  return 4096UL;
}

static ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_snapin_tile_t),     sizeof(fd_snapin_tile_t)                                    );
  if( FD_UNLIKELY( tile->kind_id!=FD_SNAPIN_ROLE_COORDINATOR ) ) {
    l = FD_LAYOUT_APPEND( l, fd_accdb_align(),            fd_accdb_footprint( tile->snapin.max_live_slots ) );
    l = FD_LAYOUT_APPEND( l, alignof(fd_snapin_extent_t), FD_SNAPIN_FIFO_CNT*sizeof(fd_snapin_extent_t)     );
    l = FD_LAYOUT_APPEND( l, 4096UL,                      FD_SNAPIN_WRITE_BUF_SZ                            );
    return FD_LAYOUT_FINI( l, scratch_align() );
  }
  l = FD_LAYOUT_APPEND( l, fd_txncache_align(),           fd_txncache_footprint( tile->snapin.max_live_slots )        );
  l = FD_LAYOUT_APPEND( l, fd_accdb_align(),              fd_accdb_footprint( tile->snapin.max_live_slots )           );
  l = FD_LAYOUT_APPEND( l, fd_ssmanifest_parser_align(),  fd_ssmanifest_parser_footprint()                            );
  l = FD_LAYOUT_APPEND( l, fd_slot_delta_parser_align(),  fd_slot_delta_parser_footprint()                            );
  l = FD_LAYOUT_APPEND( l, alignof(blockhash_group_t),    sizeof(blockhash_group_t)*FD_SNAPIN_MAX_SLOT_DELTA_GROUPS   );
  l = FD_LAYOUT_APPEND( l, alignof(fd_sstxncache_hash_t), sizeof(fd_sstxncache_hash_t)*FD_SNAPIN_TXNCACHE_MAX_ENTRIES );
  return FD_LAYOUT_FINI( l, scratch_align() );
}

static void
metrics_write( fd_snapin_tile_t * ctx ) {
  fd_accdb_flush_metrics( ctx->accdb );

  FD_MGAUGE_SET( SNAPIN, STATE,                  (ulong)ctx->state );
  FD_MGAUGE_SET( SNAPIN, FULL_BYTES_READ,        ctx->metrics.full_bytes_read );
  FD_MGAUGE_SET( SNAPIN, INCREMENTAL_BYTES_READ, ctx->metrics.incremental_bytes_read );
  FD_MGAUGE_SET( SNAPIN, ACCOUNT_LOADED,         ctx->metrics.accounts_loaded );
  FD_MGAUGE_SET( SNAPIN, ACCOUNT_REPLACED,       ctx->metrics.accounts_replaced );
  FD_MGAUGE_SET( SNAPIN, ACCOUNT_IGNORED,        ctx->metrics.accounts_ignored );
  FD_MCNT_SET  ( SNAPIN, ACCOUNT_PROCESSED,       ctx->metrics.total_accounts_processed );
  FD_MCNT_SET  ( SNAPIN, ACCOUNT_BATCH_PROCESSED, ctx->metrics.total_account_batches_processed );
}

/* verify_slot_deltas_with_slot_history verifies the 'SlotHistory'
   sysvar account after loading a snapshot.  Uses the in-memory copy
   captured by snooping the account stream (process_account_batch /
   process_account_header / process_account_data).  We cannot read
   from accdb at this point because the snapwr tile's pwritev2 may
   not have completed yet for the SlotHistory bytes.

   Returns 0 if verification passed, -1 if not. */

static int
verify_slot_deltas_with_slot_history( fd_snapin_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->slot_history.captured ) ) {
    FD_LOG_WARNING(( "SlotHistory sysvar account was not present in the snapshot stream" ));
    return -1;
  }
  if( FD_UNLIKELY( !ctx->slot_history.lamports || !ctx->slot_history.data_len ) ) {
    FD_LOG_WARNING(( "SlotHistory sysvar account missing or empty" ));
    return -1;
  }
  if( FD_UNLIKELY( !fd_memeq( ctx->slot_history.owner, fd_sysvar_owner_id.uc, sizeof(fd_pubkey_t) ) ) ) {
    FD_BASE58_ENCODE_32_BYTES( ctx->slot_history.owner, owner_b58 );
    FD_LOG_WARNING(( "SlotHistory sysvar owner is invalid: %s != sysvar_owner_id", owner_b58 ));
    return -1;
  }

  fd_slot_history_view_t view[1];
  if( FD_UNLIKELY( !fd_sysvar_slot_history_view( view, ctx->slot_history.buf, ctx->slot_history.data_len ) ) ) {
    FD_LOG_WARNING(( "SlotHistory sysvar account data is corrupt" ));
    return -1;
  }

  /* Sanity checks for slot history:
     https://github.com/anza-xyz/agave/blob/v3.1.8/runtime/src/snapshot_bank_utils.rs#L586 */

  ulong newest_slot = view->next_slot - 1UL;
  if( FD_UNLIKELY( newest_slot!=ctx->bank_slot ) ) {
    /* VerifySlotHistoryError::InvalidNewestSlot
       https://github.com/anza-xyz/agave/blob/v3.1.8/runtime/src/snapshot_bank_utils.rs#L621 */
    FD_LOG_WARNING(( "SlotHistory sysvar has an invalid newest slot: %lu != bank slot: %lu", newest_slot, ctx->bank_slot ));
    return -1;
  }

  if( FD_UNLIKELY( view->bits_len!=FD_SLOT_HISTORY_MAX_ENTRIES ) ) {
    /* VerifySlotHistoryError::InvalidNumEntries
       https://github.com/anza-xyz/agave/blob/v3.1.8/runtime/src/snapshot_bank_utils.rs#L625 */
    FD_LOG_WARNING(( "SlotHistory sysvar has invalid number of entries: %lu != expected: %lu", view->bits_len, FD_SLOT_HISTORY_MAX_ENTRIES ));
    return -1;
  }

  /* All slots in slot deltas should be present in the slot history */
  fd_slot_delta_slot_set_t slot_set = fd_slot_delta_parser_slot_set( ctx->slot_delta_parser );
  for( ulong i=0UL; i<slot_set.ele_cnt; i++ ) {
    ulong slot = slot_set.pool[ i ].slot;
    if( FD_UNLIKELY( fd_sysvar_slot_history_find_slot( view, slot )!=FD_SLOT_HISTORY_SLOT_FOUND ) ) {
      /* VerifySlotDeltasError::SlotNotFoundInHistory
         https://github.com/anza-xyz/agave/blob/v3.1.8/snapshots/src/error.rs#L144
         https://github.com/anza-xyz/agave/blob/v3.1.8/runtime/src/snapshot_bank_utils.rs#L593 */
      FD_LOG_WARNING(( "slot %lu missing from SlotHistory sysvar account", slot ));
      return -1;
    }
  }

  /* The most recent slots (up to the number of slots in the txncache)
     in the SlotHistory should be present in the txncache. */
  if( FD_LIKELY( slot_set.ele_cnt ) ) {
    ulong oldest = newest_slot - slot_set.ele_cnt;
    for( ulong i=newest_slot; i>oldest; i-- ) {
      if( FD_LIKELY( fd_sysvar_slot_history_find_slot( view, i )==FD_SLOT_HISTORY_SLOT_FOUND ) ) {
        if( FD_UNLIKELY( slot_set_ele_query( slot_set.map, &i, NULL, slot_set.pool )==NULL ) ) {
          /* VerifySlotDeltasError::SlotNotFoundInDeltas
             https://github.com/anza-xyz/agave/blob/v3.1.8/snapshots/src/error.rs#L147
             https://github.com/anza-xyz/agave/blob/v3.1.8/runtime/src/snapshot_bank_utils.rs#L609 */
          FD_LOG_WARNING(( "slot %lu missing from slot deltas but present in SlotHistory", i ));
          return -1;
        }
      }
    }
  }

  return 0;
}

/* verification of epoch stakes from manifest
   https://github.com/anza-xyz/agave/blob/v3.1.8/runtime/src/snapshot_bank_utils.rs#L632 */
static int
verify_epoch_stakes( fd_snapshot_manifest_t const * manifest ) {
  fd_epoch_schedule_t epoch_schedule = (fd_epoch_schedule_t){
    .slots_per_epoch             = manifest->epoch_schedule_params.slots_per_epoch,
    .leader_schedule_slot_offset = manifest->epoch_schedule_params.leader_schedule_slot_offset,
    .warmup                      = manifest->epoch_schedule_params.warmup,
    .first_normal_epoch          = manifest->epoch_schedule_params.first_normal_epoch,
    .first_normal_slot           = manifest->epoch_schedule_params.first_normal_slot,
  };

  ulong min_required_epoch = fd_slot_to_epoch( &epoch_schedule, manifest->slot, NULL );
  ulong max_required_epoch = fd_slot_to_leader_schedule_epoch( &epoch_schedule, manifest->slot );

  /* ensure all required epochs are present in epoch stakes */
  for( ulong i=min_required_epoch; i<=max_required_epoch; i++ ) {
    int found = 0;
    for( ulong j=0UL; j<FD_RUNTIME_MANIFEST_EPOCH_STAKES_LEN; j++ ) {
      if( manifest->epoch_stakes[j].epoch==i ) {
        found = 1;
        break;
      }
    }

    if( FD_UNLIKELY( !found ) ) {
      /* VerifyEpochStakesError::StakesNotFound
         https://github.com/anza-xyz/agave/blob/v3.1.8/runtime/src/snapshot_bank_utils.rs#L667 */
      FD_LOG_WARNING(( "stakes not found for epoch %lu in manifest", i ));
      return -1;
    }
  }

  return 0;
}

static int
verify_slot_deltas_with_bank_slot( fd_snapin_tile_t * ctx,
                                   ulong              bank_slot ) {
  fd_slot_delta_slot_set_t slot_set = fd_slot_delta_parser_slot_set( ctx->slot_delta_parser );
  for( ulong i=0UL; i<slot_set.ele_cnt; i++ ) {
    ulong slot = slot_set.pool[ i ].slot;
    /* VerifySlotDeltasError::SlotGreaterThanMaxRoot
       https://github.com/anza-xyz/agave/blob/v3.1.8/snapshots/src/error.rs#L138
       https://github.com/anza-xyz/agave/blob/v3.1.8/runtime/src/snapshot_bank_utils.rs#L550 */
    if( FD_UNLIKELY( slot>bank_slot ) ) {
      FD_LOG_WARNING(( "entry slot %lu is greater than bank slot %lu", slot, bank_slot ));
      return -1;
    }
  }
  return 0;
}

static int
verify_bank_hash( fd_snapin_tile_t const *       ctx,
                  fd_snapshot_manifest_t const * manifest ) {
  if( FD_UNLIKELY( manifest->blockhashes_len==0UL ) ) {
    FD_LOG_WARNING(( "%s manifest for epoch %lu and slot %lu has no blockhashes",
                     ctx->full?"full":"incr", ctx->epoch, manifest->slot ));
    return -1;
  }

  if( FD_UNLIKELY( !manifest->has_accounts_lthash ) ) {
    FD_LOG_WARNING(( "%s manifest for epoch %lu and slot %lu is missing accounts lthash",
                     ctx->full?"full":"incr", ctx->epoch, manifest->slot ));
    return -1;
  }

  /* find the last blockhash */
  ulong max_hash_idx = 0UL;
  ulong last_bh_idx  = 0UL;
  for( ulong i=0UL; i<manifest->blockhashes_len; i++ ) {
    if( FD_LIKELY( manifest->blockhashes[ i ].hash_index > max_hash_idx ) ) {
      max_hash_idx = manifest->blockhashes[ i ].hash_index;
      last_bh_idx  = i;
    }
  }

  /* fd_lthash_value_t is aligned to 64B but the accounts_lthash in the
     manifest may not be because its simply a uchar array.  Copy is
     needed to avoid undefined behavior. */
  fd_lthash_value_t accounts_lthash[ 1UL ];
  fd_memcpy( accounts_lthash, manifest->accounts_lthash, sizeof(fd_lthash_value_t) );

  fd_hash_t const * parent_bank_hash = (fd_hash_t const *)fd_type_pun_const( manifest->parent_bank_hash );
  fd_hash_t const * last_blockhash   = (fd_hash_t const *)fd_type_pun_const( manifest->blockhashes[ last_bh_idx ].hash );
  fd_hash_t         computed_bank_hash[ 1UL ];
  fd_hashes_hash_bank( accounts_lthash, parent_bank_hash, last_blockhash, manifest->signature_count, computed_bank_hash );
  fd_hashes_apply_hard_forks(
      computed_bank_hash,
      manifest->slot,
      manifest->parent_slot,
      manifest->hard_forks,
      manifest->hard_fork_cnt );

  if( FD_UNLIKELY( memcmp( computed_bank_hash, manifest->bank_hash, FD_HASH_FOOTPRINT ) ) ) {
    FD_BASE58_ENCODE_32_BYTES( computed_bank_hash->hash, computed_bank_hash_enc );
    FD_BASE58_ENCODE_32_BYTES( manifest->bank_hash, manifest_bank_hash_enc );
    FD_LOG_WARNING(( "%s manifest for epoch %lu and slot %lu bank hash verification failed: computed %s does not match manifest %s",
                     ctx->full?"full":"incr", ctx->epoch, manifest->slot,
                     computed_bank_hash_enc, manifest_bank_hash_enc ));
    return -1;
  }

  return 0;
}

static inline void
clear_control_barrier( fd_snapin_tile_t * ctx ) {
  ctx->pending_control = ULONG_MAX;
  fd_memset( ctx->control_seen, 0, sizeof(ctx->control_seen) );
}

static inline void
io_out_advance( fd_snapin_tile_t * ctx,
                ulong              worker_idx ) {
  fd_snapin_out_link_t * out = &ctx->io_out[ worker_idx ];
  out->chunk = fd_dcache_compact_next( out->chunk, out->mtu, out->chunk0, out->wmark );
}

static inline void
ack_out_advance( fd_snapin_tile_t * ctx ) {
  ctx->ack_out.chunk = fd_dcache_compact_next( ctx->ack_out.chunk,
                                                ctx->ack_out.mtu,
                                                ctx->ack_out.chunk0,
                                                ctx->ack_out.wmark );
}

/* Coordinator: publish one job on a worker's ring. */

static void
publish_io_job( fd_snapin_tile_t *  ctx,
                fd_stem_context_t * stem,
                ulong               worker_idx,
                ulong               kind,
                ulong               appendvec_idx,
                ulong               slot,
                ulong               body_off,
                ulong               body_sz,
                ulong               covered_until,
                ulong               fork_id ) {
  fd_snapin_out_link_t * out = &ctx->io_out[ worker_idx ];
  fd_snapin_io_job_t * job = fd_chunk_to_laddr( out->mem, out->chunk );
  job->kind          = kind;
  job->generation    = ctx->generation;
  job->appendvec_idx = appendvec_idx;
  job->slot          = slot;
  job->body_off      = body_off;
  job->body_sz       = body_sz;
  job->covered_until = covered_until;
  job->fork_id       = fork_id;
  fd_stem_publish( stem, out->idx, kind, out->chunk, sizeof(fd_snapin_io_job_t), 0UL, 0UL, 0UL );
  io_out_advance( ctx, worker_idx );
  ctx->io_pub_cnt++;
  ctx->io_jobs_since_credit++;
}

/* Coordinator: broadcast the coverage watermark to every worker ring.
   ASSIGN-before-watermark invariant: covered_until only ever reflects
   fully-assigned entries, and the owning worker's ASSIGN was published
   on its ring before covered_until advanced past the entry. */

static void
publish_watermarks( fd_snapin_tile_t *  ctx,
                    fd_stem_context_t * stem ) {
  for( ulong worker_idx=0UL; worker_idx<ctx->worker_cnt; worker_idx++ ) {
    publish_io_job( ctx, stem, worker_idx, FD_SNAPIN_IO_KIND_WATERMARK, 0UL, 0UL, 0UL, 0UL, ctx->covered_until, ULONG_MAX );
  }
  ctx->io_watermark_dirty       = 0;
  ctx->io_frags_since_watermark = 0UL;
}

/* Coordinator: on any failure of the current attempt, unblock workers
   that may be holding lane frags waiting for byte coverage that will
   never come.  ABORT makes them drop the attempt and drain data until
   FAIL. */

static void
publish_abort( fd_snapin_tile_t *  ctx,
               fd_stem_context_t * stem ) {
  if( FD_UNLIKELY( !ctx->io_enabled || ctx->abort_published ) ) return;
  ctx->abort_published = 1;
  for( ulong worker_idx=0UL; worker_idx<ctx->worker_cnt; worker_idx++ ) {
    publish_io_job( ctx, stem, worker_idx, FD_SNAPIN_IO_KIND_ABORT, 0UL, 0UL, 0UL, 0UL, 0UL, ULONG_MAX );
  }
}

static void
transition_malformed( fd_snapin_tile_t *  ctx,
                      fd_stem_context_t * stem ) {
  if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR ) ) return;
  ctx->state = FD_SNAPSHOT_STATE_ERROR;
  if( FD_UNLIKELY( ctx->io_enabled ) ) {
    publish_abort( ctx, stem );
    /* Stop waiting for acks of an abandoned control (FAIL acks must
       still be collected so the revert stays deferred). */
    if( ctx->pending_worker_control!=FD_SNAPSHOT_MSG_CTRL_FAIL ) {
      ctx->pending_worker_control  = ULONG_MAX;
      ctx->pending_worker_ack_mask = 0UL;
    }
  }
  fd_stem_publish( stem, ctx->ct_out.idx, FD_SNAPSHOT_MSG_CTRL_ERROR, 0UL, 0UL, 0UL, 0UL, 0UL );
}

static int
populate_txncache( fd_snapin_tile_t *                     ctx,
                   fd_snapshot_manifest_blockhash_t const blockhashes[ static FD_BLOCKHASHES_MAX ],
                   ulong                                  blockhashes_len ) {
  /* Our txncache internally contains the fork structure for the chain,
     which we need to recreate here.  Because snapshots are only served
     for rooted slots, there is actually no forking, and the bank forks
     are just a single bank, the root, like

       _root

     But the txncache also must contain the 150 more recent banks prior
     to the root (151 rooted banks total), looking like,


       _root_150 -> _root_149 -> ... -> _root_2 -> _root_1 -> _root

     Our txncache is "slot agnostic" meaning there is no concept of a
     slot number in it.  It just has a fork tree structure.  So long as
     the fork tree is isomorphic to the actual bank forks, and each bank
     has the correct blockhash, it works.

     So the challenge is simply to create this chain of 151 forks in the
     txncache, with correct blockhashes, and then insert all the
     transactions into it.

     Constructing the chain of blockhashes is easy.  It is just the
     BLOCKHASH_QUEUE array in the manifest.  This array is unfortunately
     not sorted and appears in random order, but it has a hash_index
     field which is a gapless index, starting at some arbitrary offset,
     so we can back out the 151 blockhashes we need from this, by first
     finding the max hash_index as _max and then collecting hash entries
     via,

       _root_150 -> _root_149 -> ... -> _root_2 -> _root_1 -> _root
       _max-150  -> _max-149  -> ... -> _max-2  -> _max-1  -> _max

     Now the remaining problem is inserting transactions into this
     chain.  Remember each transaction needs to be inserted with:

      (a) The fork ID (position of the bank in the chain) it was executed in.
      (b) The blockhash of the bank it referenced.

    (b) is trivial to retrieve, as it's in the actual slot_deltas entry
    in the manifest served by Agave.  But (a) is mildly annoying.  Agave
    serves slot_deltas based on slot, so we need an additional mapping
    from slot to position in our banks chain.  It turns out we have to
    go to yet another structure in the manifest to retrieve this, the
    ancestors array.  This is just an array of slot values,  so we need
    to sort it, and line it up against our banks chain like so,

       _root_150  -> _root_149  -> ... -> _root_2  -> _root_1  -> _root
       _max-150   -> _max-149   -> ... -> _max-2   -> _max-1   -> _max
       _slots_150 -> _slots_149 -> ... -> _slots_2 -> _slots_1 -> _slots

    From there we are done.

    Well almost ... if you were paying attention you might have noticed
    this is a lot of work and we are lazy.  Why don't we just ignore the
    slot mapping and assume everything executed at the root slot
    exactly?  The only invariant we should maintain from a memory
    perspective is that at most, across all active banks,
    FD_MAX_TXN_PER_SLOT transactions are stored per slot, but we
    have preserved that.  It is not true "per slot" technically, but
    it's true across all slots, and the memory is aggregated.  It will
    also always be true, even as slots are garbage collected, because
    entries are collected by reference blockhash, not executed slot.

    ... actually we can't do this.  There's more broken things here.
    The Agave status decided to only store 20 bytes for 32 byte
    transaction hashes to save on memory.  That's OK, but they didn't
    just take the first 20 bytes.  They instead, for each blockhash,
    take a random offset between 0 and 12, and store bytes
    [ offset, offset+20 ) of the transaction hash.  We need to know this
    offset to be able to query the txncache later, so we need to
    retrieve it from the slot_deltas entry in the manifest, and key it
    into our txncache.  Unfortunately this offset is stored per slot in
    the slot_deltas entry.  So we need to first go and retrieve the
    ancestors array, sort it, and line it up against our banks chain as
    described above, and then go through slot deltas, to retrieve the
    offset for each slot, and stick it into the appropriate bank in
    our chain. */

  if( FD_UNLIKELY( blockhashes_len>FD_BLOCKHASHES_MAX ) ) {
    FD_LOG_WARNING(( "corrupt snapshot: blockhash queue length %lu exceeds maximum %lu", blockhashes_len, FD_BLOCKHASHES_MAX ));
    return 1;
  }
  if( FD_UNLIKELY( !blockhashes_len ) ) {
    FD_LOG_WARNING(( "corrupt snapshot: blockhash queue is empty" ));
    return 1;
  }

  ulong seq_min = ULONG_MAX;
  for( ulong i=0UL; i<blockhashes_len; i++ ) seq_min = fd_ulong_min( seq_min, blockhashes[ i ].hash_index );

  ulong seq_max;
  if( FD_UNLIKELY( __builtin_uaddl_overflow( seq_min, blockhashes_len, &seq_max ) ) ) {
    FD_LOG_WARNING(( "corrupt snapshot: blockhash queue sequence number wraparound (seq_min=%lu age_cnt=%lu)", seq_min, blockhashes_len ));
    return 1;
  }

  /* First let's construct the chain array as described above.  But
     index 0 will be the root, index 1 the root's parent, etc. */

  struct {
    int exists;
    uchar blockhash[ 32UL ];
    fd_txncache_fork_id_t fork_id;
    ulong txnhash_offset;
  } banks[ FD_BLOCKHASHES_MAX ] = {0};

  for( ulong i=0UL; i<blockhashes_len; i++ ) {
    fd_snapshot_manifest_blockhash_t const * elem = &blockhashes[ i ];
    ulong idx;
    if( FD_UNLIKELY( __builtin_usubl_overflow( elem->hash_index, seq_min, &idx ) ) ) {
      FD_LOG_WARNING(( "corrupt snapshot: gap in blockhash queue (seq=[%lu,%lu) idx=%lu)", seq_min, seq_max, blockhashes[ i ].hash_index ));
      return 1;
    }

    if( FD_UNLIKELY( idx>=blockhashes_len ) ) {
      FD_LOG_WARNING(( "corrupt snapshot: blockhash queue index out of range (seq_min=%lu age_cnt=%lu idx=%lu)", seq_min, blockhashes_len, idx ));
      return 1;
    }

    if( FD_UNLIKELY( banks[ blockhashes_len-1UL-idx ].exists ) ) {
      FD_LOG_WARNING(( "corrupt snapshot: duplicate blockhash hash_index %lu", elem->hash_index ));
      return 1;
    }

    banks[ blockhashes_len-1UL-idx ].fork_id.val = USHORT_MAX;
    banks[ blockhashes_len-1UL-idx ].txnhash_offset = ULONG_MAX;
    memcpy( banks[ blockhashes_len-1UL-idx ].blockhash, elem->hash, 32UL );
    banks[ blockhashes_len-1UL-idx ].exists = 1;
  }

  ulong chain_len = fd_ulong_min( blockhashes_len, 151UL );

  /* Now we need a hashset of just the 151 most recent blockhashes,
     anything else is a nonce transaction which we do not insert, or an
     already expired transaction which can also be discarded. */

  uchar __attribute__((aligned(alignof(blockhash_map_t)))) _map[ blockhash_map_footprint( 1024UL ) ];
  blockhash_map_t * blockhash_map = blockhash_map_join( blockhash_map_new( _map, 1024UL, ctx->seed ) );
  if( FD_UNLIKELY( !blockhash_map ) ) FD_LOG_ERR(( "failed to create blockhash map" ));

  fd_blockhash_entry_t blockhash_pool[ 151UL ];
  for( ulong i=0UL; i<chain_len; i++ ) {
    fd_memcpy( blockhash_pool[ i ].blockhash.uc, banks[ i ].blockhash, 32UL );

    if( FD_UNLIKELY( blockhash_map_ele_query_const( blockhash_map, &blockhash_pool[ i ].blockhash, NULL, blockhash_pool ) ) ) {
      FD_BASE58_ENCODE_32_BYTES( banks[ i ].blockhash, blockhash_b58 );
      FD_LOG_WARNING(( "corrupt snapshot: duplicate blockhash %s in 151 most recent blockhashes", blockhash_b58 ));
      return 1;
    }

    blockhash_map_ele_insert( blockhash_map, &blockhash_pool[ i ], blockhash_pool );
  }

  /* Now load the blockhash offsets for these blockhashes ... */
  if( FD_UNLIKELY( !ctx->blockhash_groups_len ) ) {
    fd_slot_delta_slot_set_t ss = fd_slot_delta_parser_slot_set( ctx->slot_delta_parser );
    /* No offsets AND no rooted slots is corruption in either mode.  No
       offsets WITH rooted slots only happens under Alpenglow. */
    if( FD_UNLIKELY( !ctx->alpenglow || !ss.ele_cnt ) ) {
      FD_LOG_WARNING(( "corrupt snapshot: no blockhash offsets found (rooted_slots=%lu)", ss.ele_cnt ));
      return 1;
    }
    FD_LOG_WARNING(( "status cache has no blockhash offsets (rooted_slots=%lu); proceeding with empty txncache offsets",
                     ss.ele_cnt ));
  }
  for( ulong i=0UL; i<ctx->blockhash_groups_len; i++ ) {
    blockhash_group_t const * group = &ctx->blockhash_groups[ i ];
    fd_hash_t key;
    fd_memcpy( key.uc, group->blockhash, 32UL );
    fd_blockhash_entry_t * entry = blockhash_map_ele_query( blockhash_map, &key, NULL, blockhash_pool );
    if( FD_UNLIKELY( !entry ) ) continue; /* Not in the most recent 151 blockhashes */

    ulong chain_idx = (ulong)(entry - blockhash_pool);

    if( FD_UNLIKELY( banks[ chain_idx ].txnhash_offset!=ULONG_MAX && banks[ chain_idx ].txnhash_offset!=group->txnhash_offset ) ) {
      FD_BASE58_ENCODE_32_BYTES( entry->blockhash.uc, blockhash_b58 );
      FD_LOG_WARNING(( "corrupt snapshot: conflicting txnhash offsets for blockhash %s", blockhash_b58 ));
      return 1;
    }

    banks[ chain_idx ].txnhash_offset = group->txnhash_offset;
  }

  /* Construct the linear fork chain in the txncache. */

  fd_txncache_fork_id_t parent = { .val = USHORT_MAX };
  for( ulong i=0UL; i<chain_len; i++ ) banks[ chain_len-1UL-i ].fork_id = parent = fd_txncache_attach_child( ctx->txncache, parent );
  for( ulong i=0UL; i<chain_len; i++ ) fd_txncache_attach_blockhash( ctx->txncache, banks[ i ].fork_id, banks[ i ].blockhash );

  /* Now insert all transactions as if they executed at the current
     root, per above. */

  for( ulong i=0UL; i<ctx->blockhash_groups_len; i++ ) {
    blockhash_group_t const * group = &ctx->blockhash_groups[ i ];
    /* Skip if there are no transaction in this group or if the slot is
       too old. */
    if( FD_UNLIKELY( group->txncache_entry_idx==ULONG_MAX || !group->txncache_entry_cnt ) ) continue;

    ulong slot_idx = group->txncache_entry_idx/FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT;
    FD_TEST( slot_idx<ctx->txncache_slots_len );
    /* Skip groups whose entries correspond to a different slot.  This
       happens when an older slot is evicted and its storage is reused. */
    if( FD_UNLIKELY( ctx->txncache_slots[ slot_idx ].slot!=group->slot ) ) continue;
    ulong slot_entry_idx = group->txncache_entry_idx-slot_idx*FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT;
    FD_TEST( slot_entry_idx<=ctx->txncache_slots[ slot_idx ].entry_cnt );
    FD_TEST( group->txncache_entry_cnt<=ctx->txncache_slots[ slot_idx ].entry_cnt-slot_entry_idx );
    fd_sstxncache_hash_t const * entries = &ctx->txncache_entries[ group->txncache_entry_idx ];

    fd_hash_t key;
    fd_memcpy( key.uc, group->blockhash, 32UL );
    if( FD_UNLIKELY( !blockhash_map_ele_query_const( blockhash_map, &key, NULL, blockhash_pool ) ) ) continue;

    for( ulong j=0UL; j<group->txncache_entry_cnt; j++ ) {
      fd_sstxncache_hash_t const * entry = &entries[ j ];
      fd_txncache_insert( ctx->txncache, banks[ 0UL ].fork_id, group->blockhash, entry->txnhash );
    }
  }

  /* Then finalize all the banks (freezing them) and setting the txnhash
     offset so future queries use the correct offset.  If the offset is
     ULONG_MAX this is valid, it means the blockhash had no transactions
     in it, so there's nothing in the status cache under that blockhash.

     Just set the offset to 0 in this case, it doesn't matter, but
     should be valid between 0 and 12 inclusive. */
  for( ulong i=0UL; i<chain_len; i++ ) {
    ulong txnhash_offset = banks[ chain_len-1UL-i ].txnhash_offset==ULONG_MAX ? 0UL : banks[ chain_len-1UL-i ].txnhash_offset;
    fd_txncache_finalize_fork( ctx->txncache, banks[ chain_len-1UL-i ].fork_id, txnhash_offset, banks[ chain_len-1UL-i ].blockhash );
  }

  for( ulong i=1UL; i<chain_len; i++ ) fd_txncache_advance_root( ctx->txncache, banks[ chain_len-1UL-i ].fork_id );

  ctx->txncache_root_fork_id = parent;

  return 0;
}

static void
process_manifest( fd_snapin_tile_t *  ctx,
                  fd_stem_context_t * stem ) {
  fd_snapshot_manifest_t * manifest = fd_chunk_to_laddr( ctx->manifest_out.mem, ctx->manifest_out.chunk );

  if( FD_UNLIKELY( ctx->advertised_slot!=manifest->slot ) ) {
    /* SnapshotError::MismatchedSlot
       https://github.com/anza-xyz/agave/blob/v3.1.8/runtime/src/snapshot_bank_utils.rs#L472 */
    FD_LOG_WARNING(( "snapshot manifest bank slot %lu does not match advertised slot %lu from snapshot peer",
                     manifest->slot, ctx->advertised_slot ));
    transition_malformed( ctx, stem );
    return;
  }

  if( FD_UNLIKELY( !manifest->has_accounts_lthash ) ) {
    /* The manifest must contain accounts lthash, irrespective of
       whether lthash verification is disabled or not.
       https://github.com/anza-xyz/agave/blob/v3.1.9/runtime/src/serde_snapshot.rs#L482 */
    FD_LOG_WARNING(( "snapshot manifest missing accounts lthash" ));
    transition_malformed( ctx, stem );
    return;
  }

  uchar const * sum = manifest->accounts_lthash;
  uchar hash32[32]; fd_blake3_hash( sum, FD_LTHASH_LEN_BYTES, hash32 );
  FD_BASE58_ENCODE_32_BYTES( sum,    sum_enc    );
  FD_BASE58_ENCODE_32_BYTES( hash32, hash32_enc );
  FD_LOG_INFO(( "snapshot manifest slot=%lu indicates lthash[..32]=%s blake3(lthash)=%s",
                manifest->slot, sum_enc, hash32_enc ));

  if( FD_UNLIKELY( memcmp( ctx->advertised_hash, hash32, FD_HASH_FOOTPRINT ) ) ) {
    /* SnapshotError::MismatchedHash
        https://github.com/anza-xyz/agave/blob/v3.1.8/runtime/src/snapshot_bank_utils.rs#L479 */
    FD_BASE58_ENCODE_32_BYTES( ctx->advertised_hash, advertised_hash_enc );
    FD_LOG_WARNING(( "snapshot manifest accounts lthash %s does not match advertised hash from snapshot peer %s",
                     hash32_enc, advertised_hash_enc ));
    transition_malformed( ctx, stem );
    return;
  }

  ctx->bank_slot = manifest->slot;
  ctx->manifest_capitalization = manifest->capitalization;
  if( FD_UNLIKELY( ctx->manifest_capitalization>LONG_MAX ) ) {
    /* Calculations downstream require capitalization to be treated
       as long (to handle addition and subtraction). */
    FD_LOG_WARNING(( "snapshot manifest capitalization %lu exceeds LONG_MAX", ctx->manifest_capitalization ));
    transition_malformed( ctx, stem );
    return;
  }

  if( FD_UNLIKELY( fd_ssload_manifest_validate( manifest, FD_RUNTIME_MAX_VAT_VOTE_ACCOUNTS, FD_RUNTIME_MAX_STAKE_ACCOUNTS ) ) ) {
    FD_LOG_WARNING(( "snapshot manifest validation failed" ));
    transition_malformed( ctx, stem );
    return;
  }

  fd_epoch_schedule_t epoch_schedule = (fd_epoch_schedule_t){
    .slots_per_epoch             = manifest->epoch_schedule_params.slots_per_epoch,
    .leader_schedule_slot_offset = manifest->epoch_schedule_params.leader_schedule_slot_offset,
    .warmup                      = manifest->epoch_schedule_params.warmup,
    .first_normal_epoch          = manifest->epoch_schedule_params.first_normal_epoch,
    .first_normal_slot           = manifest->epoch_schedule_params.first_normal_slot,
  };
  ctx->epoch          = fd_slot_to_epoch( &epoch_schedule, manifest->slot, NULL );
  ctx->epoch_schedule = epoch_schedule;

  if( FD_UNLIKELY( verify_bank_hash( ctx, manifest ) ) ) {
    /* https://github.com/anza-xyz/agave/blob/v3.1.9/runtime/src/bank.rs#L4682 */
    transition_malformed( ctx, stem );
    return;
  }

  if( FD_UNLIKELY( verify_slot_deltas_with_bank_slot( ctx, manifest->slot ) ) ) {
    FD_LOG_WARNING(( "slot deltas verification failed" ));
    transition_malformed( ctx, stem );
    return;
  }

  if( FD_UNLIKELY( verify_epoch_stakes( manifest ) ) ) {
    FD_LOG_WARNING(( "epoch stakes verification failed" ));
    transition_malformed( ctx, stem );
    return;
  }

  if( FD_UNLIKELY( populate_txncache( ctx, manifest->blockhashes, manifest->blockhashes_len ) ) ) {
    FD_LOG_WARNING(( "populating txncache failed" ));
    transition_malformed( ctx, stem );
    return;
  }

  if( ctx->full ) {
    ctx->full_genesis_creation_time_seconds = manifest->creation_time_seconds;
  } else {
    if( FD_UNLIKELY( manifest->creation_time_seconds!=ctx->full_genesis_creation_time_seconds ) ) {
      FD_LOG_WARNING(( "snapshot manifest genesis creation time seconds %lu does not match full snapshot genesis creation time seconds %lu",
                       manifest->creation_time_seconds, ctx->full_genesis_creation_time_seconds ));
      transition_malformed( ctx, stem );
      return;
    }
  }

  manifest->accdb_fork_id    = fd_ushort_if( ctx->full, ctx->accdb_root_fork_id.val, ctx->accdb_incr_fork_id.val );
  manifest->txncache_fork_id = ctx->txncache_root_fork_id.val;

  ulong sig = ctx->full ? fd_ssmsg_sig( FD_SSMSG_MANIFEST_FULL ) :
                          fd_ssmsg_sig( FD_SSMSG_MANIFEST_INCREMENTAL );
  fd_stem_publish( stem, ctx->manifest_out.idx, sig, ctx->manifest_out.chunk, sizeof(fd_snapshot_manifest_t), 0UL, 0UL, 0UL );
  ctx->manifest_out.chunk = fd_dcache_compact_next( ctx->manifest_out.chunk, sizeof(fd_snapshot_manifest_t), ctx->manifest_out.chunk0, ctx->manifest_out.wmark );
}

static void
snoop_stake_delegation( fd_snapin_tile_t *  ctx,
                        fd_pubkey_t const * stake_account,
                        ulong               lamports,
                        ulong               data_len,
                        uchar const *       data,
                        ulong               data_sz ) {
  fd_stake_state_t const * stake_state = fd_stake_state_view( data, data_sz );
  if( FD_UNLIKELY( !stake_state || stake_state->stake_type!=FD_STAKE_STATE_STAKE ) ) return;

  fd_delegation_t const * delegation = &stake_state->stake.stake.delegation;
  if( FD_UNLIKELY( ( delegation->activation_epoch!=ULONG_MAX &&
                    delegation->activation_epoch>=(ulong)USHORT_MAX ) ||
                   ( delegation->deactivation_epoch!=ULONG_MAX &&
                    delegation->deactivation_epoch>=(ulong)USHORT_MAX ) ) ) return;

  fd_stake_delegations_root_update(
      fd_banks_stake_delegations_root_query( ctx->banks ),
      stake_account,
      &delegation->voter_pubkey,
      delegation->stake,
      delegation->activation_epoch,
      delegation->deactivation_epoch,
      stake_state->stake.stake.credits_observed,
      lamports,
      (uint)data_len,
      /* fd_stake_delegations_refresh recomputes this after load. */
      FD_STAKE_DELEGATIONS_WARMUP_COOLDOWN_RATE_ENUM_025 );
}

static int
write_account_batch( fd_snapin_tile_t *  ctx,
                     ulong               cnt,
                     uchar const * const pubkeys[],
                     ulong               slot,
                     ulong const         lamports[],
                     ulong const         data_lens[],
                     int const           executables[],
                     fd_stem_context_t * stem ) {
  (void)stem;
  fd_accdb_fork_id_t fork_id = ctx->full ? (fd_accdb_fork_id_t){ .val = USHORT_MAX } : ctx->accdb_incr_fork_id;

  ctx->metrics.total_accounts_processed += cnt;
  ctx->metrics.total_account_batches_processed++;

  ulong accounts_ignored, accounts_replaced, accounts_loaded, replaced_lamports, ignored_lamports;
  if( FD_UNLIKELY( 0!=fd_accdb_snapshot_write_batch( ctx->accdb, fork_id, cnt, pubkeys, slot, lamports, data_lens,
                                                     executables, &accounts_ignored, &accounts_replaced, &accounts_loaded,
                                                     &replaced_lamports, &ignored_lamports ) ) ) {
    return -1;
  }
  ctx->metrics.accounts_ignored  += accounts_ignored;
  ctx->metrics.accounts_replaced += accounts_replaced;
  ctx->metrics.accounts_loaded   += accounts_loaded;
  for( ulong i=0UL; i<cnt; i++ ) ctx->capitalization = fd_ulong_sat_add( ctx->capitalization, lamports[ i ] );
  ctx->capitalization     = fd_ulong_sat_sub( ctx->capitalization, ignored_lamports );
  ctx->dup_capitalization = fd_ulong_sat_add( ctx->dup_capitalization, replaced_lamports );

  if( accounts_ignored )  return 0;
  if( accounts_replaced ) return 2;
  return 1;
}

static int
process_account_batch( fd_snapin_tile_t *            ctx,
                       fd_ssparse_advance_result_t * result,
                       fd_stem_context_t *           stem ) {
  uchar const * const * entries    = result->account_batch.batch;
  ulong                 cnt        = result->account_batch.batch_cnt;
  ulong                 batch_slot = result->account_batch.slot;

  uchar const * pubkeys    [ FD_SSPARSE_ACC_BATCH_MAX ] = {0};
  ulong         lamports   [ FD_SSPARSE_ACC_BATCH_MAX ] = {0};
  ulong         data_lens  [ FD_SSPARSE_ACC_BATCH_MAX ] = {0};
  int           executables[ FD_SSPARSE_ACC_BATCH_MAX ] = {0};

  /* Resolve pubkey pointers first and start the acc_map prefetch, so the
     per-account loop below runs while those lines are in flight.  In the
     multi-worker path the coordinator never sees account results (the
     appendvec passthrough skips them). */
  for( ulong i=0UL; i<cnt; i++ ) pubkeys[ i ] = entries[ i ] + 16UL;
  fd_accdb_snapshot_prefetch_batch( ctx->accdb, cnt, pubkeys );

  for( ulong i=0UL; i<cnt; i++ ) {
    uchar const * e = entries[ i ];
    lamports[ i ]    = fd_ulong_load_8_fast( e+48UL );
    data_lens[ i ]   = fd_ulong_load_8_fast( e+8UL );
    executables[ i ] = e[ 96UL ];

    /* Snoop SlotHistory sysvar.  Account body in the batch path is
       contiguous starting at e+136. */
    if( FD_UNLIKELY( !memcmp( pubkeys[ i ], fd_sysvar_slot_history_id.uc, 32UL ) ) &&
        ( !ctx->slot_history.captured || batch_slot>=ctx->slot_history.slot ) &&
        data_lens[ i ]<=FD_SYSVAR_SLOT_HISTORY_BINCODE_SZ ) {
      ctx->slot_history.slot       = batch_slot;
      ctx->slot_history.lamports   = lamports[ i ];
      ctx->slot_history.data_len   = data_lens[ i ];
      ctx->slot_history.executable = executables[ i ];
      memcpy( ctx->slot_history.owner, e+64UL, 32UL );
      memcpy( ctx->slot_history.buf, e+136UL, data_lens[ i ] );
      ctx->slot_history.captured   = 1;
    }

    fd_feature_snoop_account( ctx->feature_snoop, (fd_pubkey_t const *)pubkeys[ i ], lamports[ i ], e+64UL, e+136UL, data_lens[ i ] );

    if( FD_UNLIKELY( lamports[ i ] &&
                     !memcmp( e+64UL, &fd_solana_stake_program_id, sizeof(fd_pubkey_t) ) ) ) {
      snoop_stake_delegation( ctx, (fd_pubkey_t const *)pubkeys[ i ], lamports[ i ],
                              data_lens[ i ], e+136UL, data_lens[ i ] );
    }
  }

  return fd_int_if( write_account_batch( ctx, cnt, pubkeys, batch_slot, lamports, data_lens, executables, stem )<0, -1, 0 );
}

static int
process_account_header( fd_snapin_tile_t *            ctx,
                        fd_ssparse_advance_result_t * result,
                        fd_stem_context_t *           stem ) {
  uchar const * pubkeys[ 1 ] = { result->account_header.pubkey };
  ulong lamports   [ 1 ] = { result->account_header.lamports   };
  ulong data_lens  [ 1 ] = { result->account_header.data_len  };
  int   executables[ 1 ] = { result->account_header.executable };
  int account = write_account_batch( ctx, 1UL, pubkeys, result->account_header.slot,
                                     lamports, data_lens, executables, stem );
  if( FD_UNLIKELY( account<0 ) ) return -1;

  /* Snoop SlotHistory sysvar.  Streaming path: arm the capture window
     here; process_account_data appends bytes while armed. */
  ctx->slot_history.capturing = 0;
  if( FD_UNLIKELY( !memcmp( result->account_header.pubkey, fd_sysvar_slot_history_id.uc, 32UL ) ) &&
      ( !ctx->slot_history.captured || result->account_header.slot>=ctx->slot_history.slot ) &&
      result->account_header.data_len<=FD_SYSVAR_SLOT_HISTORY_BINCODE_SZ ) {
    ctx->slot_history.slot       = result->account_header.slot;
    ctx->slot_history.lamports   = result->account_header.lamports;
    ctx->slot_history.data_len   = result->account_header.data_len;
    ctx->slot_history.executable = result->account_header.executable;
    memcpy( ctx->slot_history.owner, result->account_header.owner, 32UL );
    ctx->slot_history.write_pos  = 0UL;
    ctx->slot_history.capturing  = 1;
  }
  ctx->feature_reasm.capturing = 0;
  if( FD_UNLIKELY( !memcmp( result->account_header.owner, fd_solana_feature_program_id.uc, 32UL ) &&
                   result->account_header.lamports ) ) {
    memcpy( ctx->feature_reasm.pubkey.uc, result->account_header.pubkey, 32UL );
    memcpy( ctx->feature_reasm.owner,     result->account_header.owner,  32UL );
    ctx->feature_reasm.lamports  = result->account_header.lamports;
    ctx->feature_reasm.need      = fd_ulong_min( result->account_header.data_len, sizeof(ctx->feature_reasm.buf) );
    ctx->feature_reasm.write_pos = 0UL;
    ctx->feature_reasm.capturing = 1;
    if( FD_UNLIKELY( !ctx->feature_reasm.need ) ) {
      fd_feature_snoop_account( ctx->feature_snoop, &ctx->feature_reasm.pubkey,
                                ctx->feature_reasm.lamports, ctx->feature_reasm.owner,
                                ctx->feature_reasm.buf, 0UL );
      ctx->feature_reasm.capturing = 0;
    }
  }

  ctx->stake_reasm.capturing = 0;
  if( FD_UNLIKELY( account!=0 &&
                   result->account_header.lamports &&
                   result->account_header.data_len>=sizeof(fd_stake_state_t) &&
                   !memcmp( result->account_header.owner, &fd_solana_stake_program_id, sizeof(fd_pubkey_t) ) ) ) {
    memcpy( ctx->stake_reasm.pubkey.uc, result->account_header.pubkey, sizeof(fd_pubkey_t) );
    ctx->stake_reasm.lamports  = result->account_header.lamports;
    ctx->stake_reasm.data_len  = result->account_header.data_len;
    ctx->stake_reasm.write_pos = 0UL;
    ctx->stake_reasm.capturing = 1;
  }

  return 0;
}

static void
process_account_data( fd_snapin_tile_t *            ctx,
                      fd_ssparse_advance_result_t * result ) {
  if( FD_UNLIKELY( ctx->slot_history.capturing ) ) {
    ulong remaining = ctx->slot_history.data_len - ctx->slot_history.write_pos;
    ulong copy_sz   = fd_ulong_min( result->account_data.data_sz, remaining );
    memcpy( ctx->slot_history.buf + ctx->slot_history.write_pos, result->account_data.data, copy_sz );
    ctx->slot_history.write_pos += copy_sz;
    if( ctx->slot_history.write_pos==ctx->slot_history.data_len ) {
      ctx->slot_history.captured  = 1;
      ctx->slot_history.capturing = 0;
    }
  }

  if( FD_UNLIKELY( ctx->feature_reasm.capturing ) ) {
    ulong remaining = ctx->feature_reasm.need - ctx->feature_reasm.write_pos;
    ulong copy_sz   = fd_ulong_min( result->account_data.data_sz, remaining );
    memcpy( ctx->feature_reasm.buf + ctx->feature_reasm.write_pos, result->account_data.data, copy_sz );
    ctx->feature_reasm.write_pos += copy_sz;
    if( ctx->feature_reasm.write_pos==ctx->feature_reasm.need ) {
      fd_feature_snoop_account( ctx->feature_snoop, &ctx->feature_reasm.pubkey,
                                ctx->feature_reasm.lamports, ctx->feature_reasm.owner,
                                ctx->feature_reasm.buf, ctx->feature_reasm.need );
      ctx->feature_reasm.capturing = 0;
    }
  }

  if( FD_UNLIKELY( ctx->stake_reasm.capturing ) ) {
    ulong remaining = sizeof(ctx->stake_reasm.buf) - ctx->stake_reasm.write_pos;
    ulong copy_sz   = fd_ulong_min( result->account_data.data_sz, remaining );
    memcpy( ctx->stake_reasm.buf + ctx->stake_reasm.write_pos, result->account_data.data, copy_sz );
    ctx->stake_reasm.write_pos += copy_sz;
    if( ctx->stake_reasm.write_pos==sizeof(ctx->stake_reasm.buf) ) {
      snoop_stake_delegation( ctx, &ctx->stake_reasm.pubkey, ctx->stake_reasm.lamports,
                              ctx->stake_reasm.data_len, ctx->stake_reasm.buf,
                              sizeof(ctx->stake_reasm.buf) );
      ctx->stake_reasm.capturing = 0;
    }
  }
}

static int
handle_data_frag( fd_snapin_tile_t *  ctx,
                  ulong               in_idx,
                  ulong               chunk,
                  ulong               sz,
                  fd_stem_context_t * stem ) {
  if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_FINISHING ) ) {
    FD_LOG_WARNING(( "received unexpected data frag while in state %s (%lu)",
                     fd_ssctrl_state_str( (ulong)ctx->state ), (ulong)ctx->state  ));
    transition_malformed( ctx, stem );
    return 0;
  }
  if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR ) ) {
    /* Ignore all data frags after observing an error in the stream until
       we receive fail & init control messages to restart processing. */
    return 0;
  }
  if( FD_UNLIKELY( ctx->state!=FD_SNAPSHOT_STATE_PROCESSING ) ) {
    FD_LOG_ERR(( "received data frag during invalid state %s (%lu)",
                 fd_ssctrl_state_str( (ulong)ctx->state ), (ulong)ctx->state ));
  }

  if( FD_UNLIKELY( chunk<ctx->in[ in_idx ].chunk0 || chunk>ctx->in[ in_idx ].wmark || sz>ctx->in[ in_idx ].mtu ) ) {
    FD_LOG_ERR(( "invalid data frag bounds (chunk=%lu chunk0=%lu wmark=%lu sz=%lu mtu=%lu)", chunk, ctx->in[ in_idx ].chunk0, ctx->in[ in_idx ].wmark, sz, ctx->in[ in_idx ].mtu ));
  }

  ctx->io_pub_cnt = 0UL;

  for(;;) {
    if( FD_UNLIKELY( sz-ctx->in[ in_idx ].pos==0UL ) ) break;
    /* Cap ring publishes per callback (one frag can hold up to ~127 tar
       headers); the unconsumed tail of the frag is reprocessed with
       fresh credits.  A single parse step can add up to worker_cnt (<=8)
       publishes (REGION/EOS broadcast) past the cap, and the callback
       tail plus an idle after_credit in the same stem iteration can add
       up to worker_cnt watermarks each, plus manifest/ct publishes: cap
       at BURST-32 so the total stays under STEM_BURST. */
    if( FD_UNLIKELY( ctx->io_enabled && ctx->io_pub_cnt>=FD_SNAPIN_IO_BURST-32UL ) ) break;

    uchar const * data = (uchar const *)fd_chunk_to_laddr_const( ctx->in[ in_idx ].wksp, chunk ) + ctx->in[ in_idx ].pos;

    int early_exit = 0;
    fd_ssparse_advance_result_t result[1];
    int res = fd_ssparse_advance( ctx->ssparse, data, sz-ctx->in[ in_idx ].pos, result );
    switch( res ) {
      case FD_SSPARSE_ADVANCE_ERROR:
        FD_LOG_WARNING(( "error while parsing snapshot stream" ));
        transition_malformed( ctx, stem );
        return 0;
      case FD_SSPARSE_ADVANCE_AGAIN:
        break;
      case FD_SSPARSE_ADVANCE_APPENDVEC: {
        /* io_enabled only (passthrough).  Assign the whole appendvec to
           the least-loaded worker: publish the ASSIGN on that worker's
           ring, then advance the coverage watermark through the entry
           (ASSIGN before watermark; the inline covered_until gives the
           owner its coverage for free, other workers get it on the
           periodic broadcast). */
        ulong body_sz  = result->appendvec.data_sz;
        ulong body_off = ctx->stream_cursor + result->bytes_consumed;
        ulong w = 0UL;
        for( ulong i=1UL; i<ctx->worker_cnt; i++ ) {
          if( ctx->worker_assigned_bytes[ i ]<ctx->worker_assigned_bytes[ w ] ) w = i;
        }
        ctx->worker_assigned_bytes[ w ] += body_sz;
        ulong end = body_off + fd_ulong_align_up( body_sz, 512UL );
        ctx->covered_until = fd_ulong_max( ctx->covered_until, end );
        /* The ASSIGN carries the accdb fork for this appendvec's
           inserts: USHORT_MAX in the full phase, the incremental fork
           (attached at INIT_INCR, before any incremental ASSIGN can
           exist) otherwise. */
        ulong assign_fork = ctx->full ? (ulong)USHORT_MAX : (ulong)ctx->accdb_incr_fork_id.val;
        publish_io_job( ctx, stem, w, FD_SNAPIN_IO_KIND_ASSIGN, ctx->appendvec_seq++,
                        result->appendvec.slot, body_off, body_sz, ctx->covered_until, assign_fork );
        ctx->io_watermark_dirty = 1;

        ctx->av_stats.cnt++;
        ctx->av_stats.bytes += body_sz;
        ctx->av_stats.max_sz = fd_ulong_max( ctx->av_stats.max_sz, body_sz );
        if( FD_UNLIKELY( body_sz>(64UL<<20) ) )  ctx->av_stats.over_64m_cnt++;
        if( FD_UNLIKELY( body_sz>(256UL<<20) ) ) ctx->av_stats.over_256m_bytes += body_sz;
        ctx->av_stats.log2_hist[ fd_ulong_min( (ulong)fd_ulong_find_msb( fd_ulong_max( body_sz, 1UL ) ), 47UL ) ]++;
        break;
      }
      case FD_SSPARSE_ADVANCE_REGION: {
        /* io_enabled only: publish coverage for a non-appendvec entry
           BEFORE parsing it, so workers can skip-release the (multi-GiB)
           manifest/status-cache frags while the coordinator parses. */
        ulong body_off = ctx->stream_cursor + result->bytes_consumed;
        ulong end = body_off + fd_ulong_align_up( result->region.data_sz, 512UL );
        ctx->covered_until = fd_ulong_max( ctx->covered_until, end );
        publish_watermarks( ctx, stem );
        break;
      }
      case FD_SSPARSE_ADVANCE_MANIFEST:
      case FD_SSPARSE_ADVANCE_MANIFEST_DONE: {
        if( FD_UNLIKELY( ctx->flags.manifest_done ) ) {
          FD_LOG_WARNING(( "excess data after manifest" ));
          transition_malformed( ctx, stem );
          return 0;
        }
        int parser_res = fd_ssmanifest_parser_consume( ctx->manifest_parser,
                                                       result->manifest.data,
                                                       result->manifest.data_sz );
        if( FD_UNLIKELY( parser_res==FD_SSMANIFEST_PARSER_ADVANCE_ERROR ) ) {
          FD_LOG_WARNING(( "error while parsing snapshot manifest" ));
          transition_malformed( ctx, stem );
          return 0;
        }
        if( res==FD_SSPARSE_ADVANCE_MANIFEST_DONE ) {
          if( FD_UNLIKELY( fd_ssmanifest_parser_fini( ctx->manifest_parser )!=FD_SSMANIFEST_PARSER_ADVANCE_DONE ) ) {
            FD_LOG_WARNING(( "manifest stream ended before parser was done" ));
            transition_malformed( ctx, stem );
            return 0;
          }
          ctx->flags.manifest_done = 1;
        }
        break;
      }
      case FD_SSPARSE_ADVANCE_STATUS_CACHE: {
        fd_slot_delta_parser_advance_result_t sd_result[1];
        ulong bytes_remaining = result->status_cache.data_sz;

        while( bytes_remaining ) {
          int res = fd_slot_delta_parser_consume( ctx->slot_delta_parser,
                                                  result->status_cache.data,
                                                  bytes_remaining,
                                                  sd_result );
          if( FD_UNLIKELY( res<0 ) ) {
            FD_LOG_WARNING(( "error while parsing slot deltas in status cache" ));
            transition_malformed( ctx, stem );
            return 0;
          } else if( FD_LIKELY( res==FD_SLOT_DELTA_PARSER_ADVANCE_SLOT ) ) {
            /* If we're parsing a new slot, add th new slot if we
               haven't parsed 151 slots yet.  Otherwise ignore or evict
               slots that are too old.  */
            ulong candidate_idx;
            if( FD_LIKELY( ctx->txncache_slots_len<FD_TXNCACHE_MAX_SLOT_DELTAS ) ) {
              candidate_idx = ctx->txncache_slots_len++;
            } else {
              candidate_idx = 0UL;
              for( ulong i=1UL; i<FD_TXNCACHE_MAX_SLOT_DELTAS; i++ ) {
                if( ctx->txncache_slots[ i ].slot<ctx->txncache_slots[ candidate_idx ].slot ) candidate_idx = i;
              }
              if( FD_UNLIKELY( sd_result->slot<ctx->txncache_slots[ candidate_idx ].slot ) ) candidate_idx = ULONG_MAX;
            }

            if( FD_LIKELY( candidate_idx!=ULONG_MAX ) ) {
              ctx->txncache_slots[ candidate_idx ].slot      = sd_result->slot;
              ctx->txncache_slots[ candidate_idx ].entry_cnt = 0UL;
            }
            ctx->txncache_current_slot_idx       = candidate_idx;
            ctx->txncache_current_slot_entry_cnt = 0UL;
          } else if( FD_LIKELY( res==FD_SLOT_DELTA_PARSER_ADVANCE_GROUP ) ) {
            if( FD_UNLIKELY( ctx->blockhash_groups_len>=FD_SNAPIN_MAX_SLOT_DELTA_GROUPS ) ) {
              FD_LOG_WARNING(( "blockhash groups overflow, max is %lu", FD_SNAPIN_MAX_SLOT_DELTA_GROUPS ));
              transition_malformed( ctx, stem );
              return 0;
            }

            blockhash_group_t * group = &ctx->blockhash_groups[ ctx->blockhash_groups_len++ ];
            memcpy( group->blockhash, sd_result->group.blockhash, 32UL );
            group->slot               = sd_result->group.slot;
            group->txnhash_offset     = sd_result->group.txnhash_offset;
            group->txncache_entry_cnt = 0UL;

            /* Ignore the group if its corresponding slot is too old.
               Otherwise record which entry to start looking at. */
            ulong slot_idx = ctx->txncache_current_slot_idx;
            if( FD_UNLIKELY( slot_idx==ULONG_MAX ) ) {
              group->txncache_entry_idx = ULONG_MAX;
            } else {
              FD_TEST( slot_idx<ctx->txncache_slots_len );
              FD_TEST( ctx->txncache_slots[ slot_idx ].slot==group->slot );
              group->txncache_entry_idx = slot_idx*FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT+ctx->txncache_slots[ slot_idx ].entry_cnt;
            }
          } else if( FD_LIKELY( res==FD_SLOT_DELTA_PARSER_ADVANCE_ENTRY ) ) {
            FD_TEST( ctx->blockhash_groups_len );
            blockhash_group_t * group = &ctx->blockhash_groups[ ctx->blockhash_groups_len-1UL ];
            FD_TEST( group->slot==sd_result->entry->slot );

            if( FD_UNLIKELY( ctx->txncache_current_slot_entry_cnt>=FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT ) ) {
              FD_LOG_WARNING(( "txncache entries overflow for slot %lu, max is %lu",
                               group->slot, FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT ));
              transition_malformed( ctx, stem );
              return 0;
            }
            ctx->txncache_current_slot_entry_cnt++;

            /* Record the entry iff it corresponds to a valid slot. */
            ulong slot_idx = ctx->txncache_current_slot_idx;
            if( FD_LIKELY( slot_idx!=ULONG_MAX ) ) {
              FD_TEST( slot_idx<ctx->txncache_slots_len );
              txncache_staging_slot_t * staging_slot = &ctx->txncache_slots[ slot_idx ];
              FD_TEST( staging_slot->slot==group->slot );
              FD_TEST( staging_slot->entry_cnt<FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT );
              ulong entry_idx = slot_idx*FD_PACK_MAX_TXNCACHE_TXN_PER_SLOT+staging_slot->entry_cnt;
              FD_TEST( entry_idx==group->txncache_entry_idx+group->txncache_entry_cnt );
              memcpy( ctx->txncache_entries[ entry_idx ].txnhash, sd_result->entry->txnhash, sizeof(fd_sstxncache_hash_t) );
              staging_slot->entry_cnt++;
              group->txncache_entry_cnt++;
            }
          }

          bytes_remaining           -= sd_result->bytes_consumed;
          result->status_cache.data += sd_result->bytes_consumed;
        }

        if( FD_UNLIKELY( result->status_cache.done ) ) {
          int fini_res = fd_slot_delta_parser_consume( ctx->slot_delta_parser, result->status_cache.data, 0UL, sd_result );
          if( FD_UNLIKELY( fini_res<0 ) ) {
            FD_LOG_WARNING(( "error while finalizing slot deltas in status cache" ));
            transition_malformed( ctx, stem );
            return 0;
          }
          ctx->flags.status_cache_done = fini_res==FD_SLOT_DELTA_PARSER_ADVANCE_DONE;
        }
        break;
      }
      case FD_SSPARSE_ADVANCE_ACCOUNT_HEADER:
        early_exit = process_account_header( ctx, result, stem );
        if( FD_UNLIKELY( early_exit<0 ) ) {
          transition_malformed( ctx, stem );
          return 0;
        }

        if( FD_UNLIKELY( ctx->gui_out.idx!=ULONG_MAX
                      && !memcmp( result->account_header.owner, fd_solana_config_program_id.key, sizeof(fd_hash_t) )
                      && result->account_header.data_len
                      && result->account_header.data_len<=FD_GUI_CONFIG_PARSE_MAX_VALID_ACCT_SZ ) ) {
          ctx->gui_config_acct_sz  = result->account_header.data_len;
          ctx->gui_config_acct_off = 0UL;
        } else {
          ctx->gui_config_acct_sz  = 0UL;
        }
        break;
      case FD_SSPARSE_ADVANCE_ACCOUNT_DATA:
        process_account_data( ctx, result );

        /* Account data may span multiple input chunks (when an account
           straddles a decompressed chunk boundary), so we copy each
           piece into the gui_out dcache and only publish once the full
           account has been received.

           We expect ConfigKeys Vec to be length 2 (checked via the
           first byte of the accumulated data).  We expect the size of
           ConfigProgram-owned accounts to be at most
           FD_GUI_CONFIG_PARSE_MAX_VALID_ACCT_SZ, since this is the
           size that the Solana CLI allocates for them. Although the
           ConfigProgram itself does not enforce these invariants, the
           vast majority of accounts (with a tiny number of exceptions
           on devnet) are maintained with the Solana CLI. */
        if( FD_UNLIKELY( ctx->gui_config_acct_sz ) ) {
          uchar * acct = fd_chunk_to_laddr( ctx->gui_out.mem, ctx->gui_out.chunk );
          fd_memcpy( acct + ctx->gui_config_acct_off, result->account_data.data, result->account_data.data_sz );
          ctx->gui_config_acct_off += result->account_data.data_sz;

          if( FD_LIKELY( ctx->gui_config_acct_off>=ctx->gui_config_acct_sz ) ) {
            ctx->gui_config_acct_sz = 0UL;
            if( FD_LIKELY( acct[ 0 ]==2UL ) ) {
              fd_stem_publish( stem, ctx->gui_out.idx, 0UL, ctx->gui_out.chunk, ctx->gui_config_acct_off, 0UL, 0UL, 0UL );
              ctx->gui_out.chunk = fd_dcache_compact_next( ctx->gui_out.chunk, ctx->gui_config_acct_off, ctx->gui_out.chunk0, ctx->gui_out.wmark );
              early_exit = 1;
            }
          }
        }
        break;
      case FD_SSPARSE_ADVANCE_ACCOUNT_BATCH:
        early_exit = process_account_batch( ctx, result, stem );
        if( FD_UNLIKELY( early_exit<0 ) ) {
          transition_malformed( ctx, stem );
          return 0;
        }
        break;
      case FD_SSPARSE_ADVANCE_DONE:
        if( FD_UNLIKELY( ctx->io_enabled ) ) {
          for( ulong worker_idx=0UL; worker_idx<ctx->worker_cnt; worker_idx++ ) {
            publish_io_job( ctx, stem, worker_idx, FD_SNAPIN_IO_KIND_EOS, 0UL, 0UL, 0UL, 0UL, 0UL, ULONG_MAX );
          }
        }
        ctx->state = FD_SNAPSHOT_STATE_FINISHING;
        break;
      default:
        FD_LOG_ERR(( "unexpected fd_ssparse_advance result %d", res ));
        break;
    }

    if( FD_UNLIKELY( !ctx->flags.manifest_processed && ctx->flags.manifest_done && ctx->flags.status_cache_done ) ) {
      process_manifest( ctx, stem );
      if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR ) ) break;
      ctx->flags.manifest_processed = 1;
    }

    ctx->in[ in_idx ].pos += result->bytes_consumed;
    ctx->stream_cursor    += result->bytes_consumed;
    if( FD_LIKELY( ctx->full ) ) ctx->metrics.full_bytes_read        += result->bytes_consumed;
    else                         ctx->metrics.incremental_bytes_read += result->bytes_consumed;

    if( FD_UNLIKELY( early_exit ) ) break;
  }

  int reprocess_frag = ctx->in[ in_idx ].pos<sz;
  if( FD_LIKELY( !reprocess_frag ) ) ctx->in[ in_idx ].pos = 0UL;
  return reprocess_frag;
}

static int
validate_capitalization( fd_snapin_tile_t * ctx ) {
  if( FD_UNLIKELY( ctx->capitalization!=ctx->manifest_capitalization ) ) {
    /* SnapshotError::MismatchedCapitalization
        https://github.com/anza-xyz/agave/blob/v4.0.0-beta.2/runtime/src/snapshot_bank_utils.rs#L217 */
    FD_LOG_WARNING(( "%s snapshot manifest capitalization %lu does not match computed capitalization %lu",
                     ctx->full?"full":"incr", ctx->manifest_capitalization, ctx->capitalization ));
    return 0; /* TEMPORARY (benchmark only): tolerate agave 4.3 capitalization mismatch; restore to -1 before merge */
  }
  return 0;
}

/* Worker role ********************************************************/

static void
worker_publish_ack( fd_snapin_tile_t *  ctx,
                    fd_stem_context_t * stem,
                    ulong               control,
                    int                 err ) {
  fd_snapin_io_ack_t * ack = fd_chunk_to_laddr( ctx->ack_out.mem, ctx->ack_out.chunk );
  ack->worker_idx            = ctx->worker_idx;
  ack->generation            = ctx->generation;
  ack->control               = control;
  ack->accounts_ignored      = ctx->metrics.accounts_ignored;
  ack->accounts_replaced     = ctx->metrics.accounts_replaced;
  ack->accounts_loaded       = ctx->metrics.accounts_loaded;
  ack->input_lamports        = ctx->worker.input_lamports;
  ack->replaced_lamports     = ctx->worker.replaced_lamports;
  ack->ignored_lamports      = ctx->worker.ignored_lamports;
  ack->bytes_written         = ctx->bytes_written;
  ack->eq_slot_dups          = ctx->worker_metrics->eq_slot_dups;
  ack->eq_slot_lamports_diff = ctx->worker_metrics->eq_slot_lamports_diff;
  ack->err                   = err;
  fd_stem_publish( stem, ctx->ack_out.idx, fd_snapin_io_ack_sig( ctx->generation, control ),
                   ctx->ack_out.chunk, sizeof(fd_snapin_io_ack_t), 0UL, 0UL, 0UL );
  ack_out_advance( ctx );
}

/* Enter the ERROR state and (unless silent, e.g. on a coordinator ABORT
   that already knows) send an unsolicited ERROR ack so the coordinator
   can fail the attempt and unblock the other workers. */

static void
worker_enter_error( fd_snapin_tile_t *  ctx,
                    fd_stem_context_t * stem,
                    int                 err,
                    int                 silent ) {
  if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR ) ) return;
  ctx->state        = FD_SNAPSHOT_STATE_ERROR;
  ctx->pending_fini = 0;
  if( FD_LIKELY( !silent ) ) {
    FD_LOG_WARNING(( "snapshot accdb worker %lu failed (%d-%s)", ctx->worker_idx, err, fd_io_strerror( err ) ));
    worker_publish_ack( ctx, stem, FD_SNAPSHOT_MSG_CTRL_ERROR, err );
  }
}

static void
worker_reset_write_engine( fd_snapin_tile_t * ctx ) {
  ctx->write_buf_used      = 0UL;
  ctx->flush_off           = 0UL;
  ctx->bytes_written       = 0UL;
  ctx->whead.val           = 0UL;
  ctx->whead.has_partition = 0;
  fd_memset( &ctx->open_acc, 0, sizeof(ctx->open_acc) );
  ctx->wb_run_sz  = 0UL;
  ctx->wb_pending = 0UL;
  ctx->wb_head    = 0UL;
  ctx->wb_tail    = 0UL;
  ctx->wb_kick_cnt = 0UL;
  ctx->wb_wait_cnt = 0UL;
}

static void
worker_reset_attempt( fd_snapin_tile_t * ctx ) {
  ctx->expected_frame = 0UL;
  for( ulong lane=0UL; lane<ctx->lane_cnt; lane++ ) ctx->in[ lane ].pos = 0UL;
  ctx->eos_seen       = 0;
  ctx->pending_fini   = 0;
  ctx->incr_fork      = ULONG_MAX;
  ctx->covered_until  = 0UL;
  ctx->cursor         = 0UL;
  ctx->fifo_head      = 0UL;
  ctx->fifo_tail      = 0UL;
  ctx->av_active      = 0;
  ctx->av_consumed    = 0UL;
  ctx->rec_idx        = 0UL;
  worker_reset_write_engine( ctx );
  fd_memset( &ctx->worker,    0, sizeof(ctx->worker)    );
  fd_memset( ctx->worker_metrics, 0, sizeof(ctx->worker_metrics) );
  fd_memset( &ctx->cov_stats, 0, sizeof(ctx->cov_stats) );
  ctx->cov_stats.lag_min = ULONG_MAX;
  ctx->metrics.accounts_loaded   = 0UL;
  ctx->metrics.accounts_replaced = 0UL;
  ctx->metrics.accounts_ignored  = 0UL;
  ctx->metrics.total_accounts_processed        = 0UL;
  ctx->metrics.total_account_batches_processed = 0UL;
  ctx->sh_reasm.capturing      = 0;
  ctx->feature_reasm.capturing = 0;
  ctx->stake_reasm.capturing   = 0;
  fd_ssparse_init( ctx->ssparse );
  fd_ssparse_batch_enable( ctx->ssparse, 1 );
  /* Reset the shared snoop staging owned by this worker */
  ctx->my_snoop->slot_history.captured = 0;
  fd_memset( &ctx->my_snoop->feature_snoop, 0, sizeof(fd_feature_snoop_t) );
  ctx->my_snoop->stake_cnt = 0UL;
}

/* Worker staging buffer: buffered pwrites into the worker's own accdb
   partitions (cloned from the deleted snapwr tile's buffer_write/flush,
   with an explicit flush_off because per-worker offsets are only
   sequential within a partition). */

/* Write-behind machinery.  worker_wb_disable turns it off for the rest
   of the run (unsupported filesystem); worker_wb_kick starts async
   writeback of the accumulated contiguous run; worker_wb_track records
   a flushed range and applies the smooth self-throttle backstop. */

static void
worker_wb_disable( fd_snapin_tile_t * ctx ) {
  FD_LOG_WARNING(( "sync_file_range failed (%d-%s); disabling worker write-behind", errno, fd_io_strerror( errno ) ));
  ctx->wb_window  = 0UL;
  ctx->wb_run_sz  = 0UL;
  ctx->wb_pending = 0UL;
  ctx->wb_head    = ctx->wb_tail;
}

static void
worker_wb_kick( fd_snapin_tile_t * ctx ) {
  if( FD_LIKELY( !ctx->wb_run_sz ) ) return;

  /* Ring full: retire the oldest range first. */
  if( FD_UNLIKELY( ctx->wb_tail-ctx->wb_head>=FD_SNAPIN_WB_RING_CNT ) ) {
    ulong idx = ctx->wb_head & (FD_SNAPIN_WB_RING_CNT-1UL);
    while( FD_UNLIKELY( -1==sync_file_range( FD_ACCDB_FD_RW, (long)ctx->wb_ring[ idx ].off, (long)ctx->wb_ring[ idx ].sz,
                                             SYNC_FILE_RANGE_WAIT_BEFORE|SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER ) ) ) {
      if( FD_LIKELY( errno==EINTR ) ) continue;
      worker_wb_disable( ctx );
      return;
    }
    ctx->wb_pending -= ctx->wb_ring[ idx ].sz;
    ctx->wb_head++;
    ctx->wb_wait_cnt++;
  }

  while( FD_UNLIKELY( -1==sync_file_range( FD_ACCDB_FD_RW, (long)ctx->wb_run_off, (long)ctx->wb_run_sz, SYNC_FILE_RANGE_WRITE ) ) ) {
    if( FD_LIKELY( errno==EINTR ) ) continue;
    worker_wb_disable( ctx );
    return;
  }
  ctx->wb_ring[ ctx->wb_tail & (FD_SNAPIN_WB_RING_CNT-1UL) ] = (__typeof__(ctx->wb_ring[0])){ .off = ctx->wb_run_off, .sz = ctx->wb_run_sz };
  ctx->wb_tail++;
  ctx->wb_pending += ctx->wb_run_sz;
  ctx->wb_run_sz   = 0UL;
  ctx->wb_kick_cnt++;
}

static void
worker_wb_track( fd_snapin_tile_t * ctx,
                 ulong              off,
                 ulong              sz ) {
  if( FD_LIKELY( !ctx->wb_window ) ) return;

  if( FD_UNLIKELY( ctx->wb_run_sz && off!=ctx->wb_run_off+ctx->wb_run_sz ) ) worker_wb_kick( ctx ); /* partition rotation */
  if( FD_UNLIKELY( !ctx->wb_run_sz ) ) ctx->wb_run_off = off;
  ctx->wb_run_sz += sz;
  if( FD_UNLIKELY( ctx->wb_run_sz>=ctx->wb_kick_sz ) ) worker_wb_kick( ctx );

  /* Backstop: wait on the oldest kicked range whenever the in-flight
     window is exceeded.  The range was kicked (window/kick ~ tens of
     ranges) earlier, so the wait is short and 64 MiB granular: the
     512 MiB lane runway rides through it where the kernel's coarse
     dirty-throttle sleeps would stall the pipe. */
  while( FD_UNLIKELY( ctx->wb_window && ctx->wb_pending>ctx->wb_window && ctx->wb_head!=ctx->wb_tail ) ) {
    ulong idx = ctx->wb_head & (FD_SNAPIN_WB_RING_CNT-1UL);
    while( FD_UNLIKELY( -1==sync_file_range( FD_ACCDB_FD_RW, (long)ctx->wb_ring[ idx ].off, (long)ctx->wb_ring[ idx ].sz,
                                             SYNC_FILE_RANGE_WAIT_BEFORE|SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER ) ) ) {
      if( FD_LIKELY( errno==EINTR ) ) continue;
      worker_wb_disable( ctx );
      return;
    }
    ctx->wb_pending -= ctx->wb_ring[ idx ].sz;
    ctx->wb_head++;
    ctx->wb_wait_cnt++;
  }
}

static void
worker_buffer_flush( fd_snapin_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->write_buf_used ) ) return;

  ulong sz  = ctx->write_buf_used;
  ulong off = ctx->flush_off;
  ulong bytes_written = 0UL;
  while( bytes_written<sz ) {
    long res = pwrite( FD_ACCDB_FD_RW, ctx->write_buf+bytes_written, sz-bytes_written, (long)(off+bytes_written) );
    if( FD_UNLIKELY( -1L==res ) ) {
      if( FD_LIKELY( errno==EINTR ) ) continue;
      FD_LOG_ERR(( "error writing to disk (%d-%s)", errno, fd_io_strerror( errno ) ));
    }
    bytes_written      += (ulong)res;
    ctx->bytes_written += (ulong)res;
  }
  ctx->flush_off      += sz;
  ctx->write_buf_used  = 0UL;

  worker_wb_track( ctx, off, sz );
}

static void
worker_buffer_write( fd_snapin_tile_t * ctx,
                     ulong              file_off,
                     uchar const *      data,
                     ulong              sz ) {
  /* Force a flush whenever the next offset is not the natural append
     point (first write, or the allocator rotated to a new partition). */
  if( FD_UNLIKELY( file_off!=ctx->flush_off+ctx->write_buf_used ) ) {
    worker_buffer_flush( ctx );
    ctx->flush_off = file_off;
  }
  while( sz ) {
    ulong avail = FD_SNAPIN_WRITE_BUF_SZ - ctx->write_buf_used;
    ulong n     = fd_ulong_min( sz, avail );
    fd_memcpy( ctx->write_buf + ctx->write_buf_used, data, n );
    ctx->write_buf_used += n;
    data += n;
    sz   -= n;
    if( FD_UNLIKELY( ctx->write_buf_used==FD_SNAPIN_WRITE_BUF_SZ ) ) worker_buffer_flush( ctx );
  }
}

static void
worker_stage_meta( fd_snapin_tile_t * ctx,
                   ulong              file_off,
                   uchar const *      pubkey,
                   uchar const *      owner,
                   ulong              data_len ) {
  fd_accdb_disk_meta_t meta;
  fd_memcpy( meta.pubkey, pubkey, 32UL );
  meta.size       = (uint)data_len;
  meta.generation = 0U;
  fd_memcpy( meta.owner, owner, 32UL );
  worker_buffer_write( ctx, file_off, meta.b, sizeof(fd_accdb_disk_meta_t) );
}

/* Snoop staging.  Precedence tags are (slot, appendvec_idx, record_idx)
   for slot history (slot>= replaces, matching the sequential loader's
   stream-order rule) and (appendvec_idx, record_idx) stream position
   for features/stakes. */

static inline int
worker_sh_wins( fd_snapio_worker_snoop_t const * ws,
                ulong slot, ulong av, ulong rec ) {
  if( !ws->slot_history.captured ) return 1;
  if( slot!=ws->slot_history.slot ) return slot>ws->slot_history.slot;
  if( av!=ws->slot_history.appendvec_idx ) return av>ws->slot_history.appendvec_idx;
  return rec>=ws->slot_history.record_idx;
}

static void
worker_snoop_slot_history_contig( fd_snapin_tile_t * ctx,
                                  ulong              slot,
                                  ulong              rec_idx,
                                  ulong              lamports,
                                  ulong              data_len,
                                  uchar const *      owner,
                                  int                executable,
                                  uchar const *      data ) {
  fd_snapio_worker_snoop_t * ws = ctx->my_snoop;
  if( FD_UNLIKELY( data_len>FD_SYSVAR_SLOT_HISTORY_BINCODE_SZ ) ) return;
  if( FD_UNLIKELY( !worker_sh_wins( ws, slot, ctx->cur_av.appendvec_idx, rec_idx ) ) ) return;
  ws->slot_history.slot          = slot;
  ws->slot_history.appendvec_idx = ctx->cur_av.appendvec_idx;
  ws->slot_history.record_idx    = rec_idx;
  ws->slot_history.lamports      = lamports;
  ws->slot_history.data_len      = data_len;
  ws->slot_history.executable    = executable;
  fd_memcpy( ws->slot_history.owner, owner, 32UL );
  fd_memcpy( ws->slot_history.buf, data, data_len );
  ws->slot_history.captured      = 1;
}

static void
worker_snoop_feature( fd_snapin_tile_t * ctx,
                      uchar const *      pubkey,
                      ulong              lamports,
                      uchar const *      owner,
                      uchar const *      data,
                      ulong              data_len,
                      ulong              av_idx,
                      ulong              rec_idx ) {
  if( FD_LIKELY( !lamports ) ) return;
  if( FD_LIKELY( memcmp( owner, fd_solana_feature_program_id.uc, 32UL ) ) ) return;
  fd_feature_id_t const * id = fd_feature_id_query( fd_ulong_load_8( pubkey ) );
  if( FD_UNLIKELY( !id || !fd_pubkey_eq( (fd_pubkey_t const *)pubkey, &id->id ) ) ) return;
  fd_feature_t feature[1];
  if( FD_UNLIKELY( data_len<sizeof(fd_feature_t) || !fd_feature_decode( feature, data, data_len ) ) ) return;

  fd_snapio_worker_snoop_t * ws = ctx->my_snoop;
  ws->feature_snoop.present        [ id->index ] = 1;
  ws->feature_snoop.is_active      [ id->index ] = feature->is_active;
  ws->feature_snoop.activation_slot[ id->index ] = feature->activation_slot;
  ws->feature_av [ id->index ] = av_idx;
  ws->feature_rec[ id->index ] = rec_idx;
}

static void
worker_snoop_stake( fd_snapin_tile_t * ctx,
                    uchar const *      pubkey,
                    ulong              lamports,
                    ulong              data_len,
                    uchar const *      data,
                    ulong              data_sz,
                    ulong              av_idx,
                    ulong              rec_idx ) {
  fd_stake_state_t const * stake_state = fd_stake_state_view( data, data_sz );
  if( FD_UNLIKELY( !stake_state || stake_state->stake_type!=FD_STAKE_STATE_STAKE ) ) return;

  fd_delegation_t const * delegation = &stake_state->stake.stake.delegation;
  if( FD_UNLIKELY( ( delegation->activation_epoch!=ULONG_MAX &&
                    delegation->activation_epoch>=(ulong)USHORT_MAX ) ||
                   ( delegation->deactivation_epoch!=ULONG_MAX &&
                    delegation->deactivation_epoch>=(ulong)USHORT_MAX ) ) ) return;

  fd_snapio_worker_snoop_t * ws = ctx->my_snoop;
  if( FD_UNLIKELY( ws->stake_cnt>=ctx->stake_log_max ) ) {
    FD_LOG_ERR(( "stake snoop log overflow (worker %lu, max %lu)", ctx->worker_idx, ctx->stake_log_max ));
  }
  fd_snapio_stake_ent_t * ent = &fd_snapio_worker_stake_log( ws )[ ws->stake_cnt ];
  fd_memcpy( ent->pubkey, pubkey, 32UL );
  fd_memcpy( ent->voter, delegation->voter_pubkey.uc, 32UL );
  ent->stake              = delegation->stake;
  ent->activation_epoch   = delegation->activation_epoch;
  ent->deactivation_epoch = delegation->deactivation_epoch;
  ent->credits_observed   = stake_state->stake.stake.credits_observed;
  ent->lamports           = lamports;
  ent->appendvec_idx      = av_idx;
  ent->record_idx         = rec_idx;
  ent->data_len           = (uint)data_len;
  FD_COMPILER_MFENCE();
  ws->stake_cnt++;
}

/* worker_cov_advance advances the worker's coverage watermark and
   samples the coverage lag (runway ahead of the scan cursor) for the
   day-1 throttling instrumentation. */

static inline void
worker_cov_advance( fd_snapin_tile_t * ctx,
                    ulong              until ) {
  if( FD_LIKELY( until<=ctx->covered_until ) ) return;
  ctx->covered_until = until;
  ulong lag = until - fd_ulong_min( ctx->cursor, until );
  ctx->cov_stats.lag_samples++;
  ctx->cov_stats.lag_sum += lag;
  ctx->cov_stats.lag_min  = fd_ulong_min( ctx->cov_stats.lag_min, lag );
  ctx->cov_stats.lag_max  = fd_ulong_max( ctx->cov_stats.lag_max, lag );
}

static int
worker_process_account_batch( fd_snapin_tile_t *            ctx,
                              fd_ssparse_advance_result_t * result ) {
  uchar const * const * entries    = result->account_batch.batch;
  ulong                 cnt        = result->account_batch.batch_cnt;
  ulong                 batch_slot = result->account_batch.slot;

  uchar const * pubkeys     [ FD_SSPARSE_ACC_BATCH_MAX ] = {0};
  ulong         lamports    [ FD_SSPARSE_ACC_BATCH_MAX ] = {0};
  ulong         data_lens   [ FD_SSPARSE_ACC_BATCH_MAX ] = {0};
  int           executables [ FD_SSPARSE_ACC_BATCH_MAX ] = {0};
  ulong         file_offsets[ FD_SSPARSE_ACC_BATCH_MAX ] = {0};

  for( ulong i=0UL; i<cnt; i++ ) {
    uchar const * e = entries[ i ];
    pubkeys[ i ]     = e + 16UL;
    lamports[ i ]    = fd_ulong_load_8_fast( e+48UL );
    data_lens[ i ]   = fd_ulong_load_8_fast( e+8UL );
    executables[ i ] = e[ 96UL ];

    /* Snoops (unconditional in stream order, matching the sequential
       loader's batch path). */
    ulong rec = ctx->rec_idx + i;
    if( FD_UNLIKELY( !memcmp( pubkeys[ i ], fd_sysvar_slot_history_id.uc, 32UL ) ) ) {
      worker_snoop_slot_history_contig( ctx, batch_slot, rec, lamports[ i ], data_lens[ i ], e+64UL, executables[ i ], e+136UL );
    }
    worker_snoop_feature( ctx, pubkeys[ i ], lamports[ i ], e+64UL, e+136UL, data_lens[ i ], ctx->cur_av.appendvec_idx, rec );
    if( FD_UNLIKELY( lamports[ i ] &&
                     !memcmp( e+64UL, &fd_solana_stake_program_id, sizeof(fd_pubkey_t) ) ) ) {
      worker_snoop_stake( ctx, pubkeys[ i ], lamports[ i ], data_lens[ i ], e+136UL, data_lens[ i ],
                          ctx->cur_av.appendvec_idx, rec );
    }
  }
  ctx->rec_idx += cnt;

  ulong accounts_ignored, accounts_replaced, accounts_loaded, replaced_lamports, ignored_lamports;
  fd_accdb_fork_id_t fork_id = { .val = ctx->full ? USHORT_MAX : (ushort)ctx->incr_fork };
  if( FD_UNLIKELY( 0!=fd_accdb_snapshot_write_batch_worker( ctx->accdb, fork_id, cnt, pubkeys, batch_slot, lamports,
                                                            data_lens, executables, &ctx->whead,
                                                            ctx->stripe_locks, FD_SNAPIO_STRIPE_MSK, ctx->worker_metrics,
                                                            file_offsets, &accounts_ignored, &accounts_replaced,
                                                            &accounts_loaded, &replaced_lamports, &ignored_lamports ) ) ) {
    return -1;
  }

  /* Stage the accepted disk records (72 byte header + data, packed at
     the allocated explicit offsets; ignored dups burn no space). */
  for( ulong i=0UL; i<cnt; i++ ) {
    if( FD_UNLIKELY( file_offsets[ i ]==ULONG_MAX ) ) continue;
    worker_stage_meta( ctx, file_offsets[ i ], pubkeys[ i ], entries[ i ]+64UL, data_lens[ i ] );
    if( FD_LIKELY( data_lens[ i ] ) ) {
      worker_buffer_write( ctx, file_offsets[ i ]+sizeof(fd_accdb_disk_meta_t), entries[ i ]+136UL, data_lens[ i ] );
    }
  }

  ctx->metrics.accounts_ignored  += accounts_ignored;
  ctx->metrics.accounts_replaced += accounts_replaced;
  ctx->metrics.accounts_loaded   += accounts_loaded;
  ctx->metrics.total_accounts_processed += cnt;
  ctx->metrics.total_account_batches_processed++;
  for( ulong i=0UL; i<cnt; i++ ) ctx->worker.input_lamports = fd_ulong_sat_add( ctx->worker.input_lamports, lamports[ i ] );
  ctx->worker.replaced_lamports = fd_ulong_sat_add( ctx->worker.replaced_lamports, replaced_lamports );
  ctx->worker.ignored_lamports  = fd_ulong_sat_add( ctx->worker.ignored_lamports,  ignored_lamports  );

  return 0;
}

static int
worker_process_account_header( fd_snapin_tile_t *            ctx,
                               fd_ssparse_advance_result_t * result ) {
  ulong slot     = result->account_header.slot;
  ulong lamports = result->account_header.lamports;
  ulong data_len = result->account_header.data_len;
  ulong rec      = ctx->rec_idx++;

  uchar const * pubkeys     [ 1 ] = { result->account_header.pubkey };
  ulong         lamports_a  [ 1 ] = { lamports };
  ulong         data_lens   [ 1 ] = { data_len };
  int           executables [ 1 ] = { result->account_header.executable };
  ulong         file_offsets[ 1 ];
  ulong accounts_ignored, accounts_replaced, accounts_loaded, replaced_lamports, ignored_lamports;
  fd_accdb_fork_id_t fork_id = { .val = ctx->full ? USHORT_MAX : (ushort)ctx->incr_fork };
  if( FD_UNLIKELY( 0!=fd_accdb_snapshot_write_batch_worker( ctx->accdb, fork_id, 1UL, pubkeys, slot, lamports_a,
                                                            data_lens, executables, &ctx->whead,
                                                            ctx->stripe_locks, FD_SNAPIO_STRIPE_MSK, ctx->worker_metrics,
                                                            file_offsets, &accounts_ignored, &accounts_replaced,
                                                            &accounts_loaded, &replaced_lamports, &ignored_lamports ) ) ) {
    return -1;
  }
  int ignored = file_offsets[ 0 ]==ULONG_MAX;

  ctx->open_acc.accepted = !ignored;
  ctx->open_acc.received = 0UL;
  ctx->open_acc.file_off = file_offsets[ 0 ];
  if( FD_LIKELY( !ignored ) ) {
    worker_stage_meta( ctx, file_offsets[ 0 ], result->account_header.pubkey, result->account_header.owner, data_len );
  }

  ctx->metrics.accounts_ignored  += accounts_ignored;
  ctx->metrics.accounts_replaced += accounts_replaced;
  ctx->metrics.accounts_loaded   += accounts_loaded;
  ctx->metrics.total_accounts_processed++;
  ctx->metrics.total_account_batches_processed++;
  ctx->worker.input_lamports    = fd_ulong_sat_add( ctx->worker.input_lamports,    lamports );
  ctx->worker.replaced_lamports = fd_ulong_sat_add( ctx->worker.replaced_lamports, replaced_lamports );
  ctx->worker.ignored_lamports  = fd_ulong_sat_add( ctx->worker.ignored_lamports,  ignored_lamports  );

  /* Streaming-path snoop arming (fragmented account bodies).  Same
     gating as the sequential loader's process_account_header. */
  ctx->reasm_av_idx  = ctx->cur_av.appendvec_idx;
  ctx->reasm_rec_idx = rec;

  ctx->sh_reasm.capturing = 0;
  if( FD_UNLIKELY( !memcmp( result->account_header.pubkey, fd_sysvar_slot_history_id.uc, 32UL ) ) &&
      data_len<=FD_SYSVAR_SLOT_HISTORY_BINCODE_SZ &&
      worker_sh_wins( ctx->my_snoop, slot, ctx->cur_av.appendvec_idx, rec ) ) {
    fd_snapio_worker_snoop_t * ws = ctx->my_snoop;
    ws->slot_history.slot          = slot;
    ws->slot_history.appendvec_idx = ctx->cur_av.appendvec_idx;
    ws->slot_history.record_idx    = rec;
    ws->slot_history.lamports      = lamports;
    ws->slot_history.data_len      = data_len;
    ws->slot_history.executable    = result->account_header.executable;
    fd_memcpy( ws->slot_history.owner, result->account_header.owner, 32UL );
    ws->slot_history.captured      = 0; /* committed when the capture completes */
    ctx->sh_reasm.capturing = 1;
    ctx->sh_reasm.write_pos = 0UL;
    ctx->sh_reasm.data_len  = data_len;
    if( FD_UNLIKELY( !data_len ) ) {
      ws->slot_history.captured = 1;
      ctx->sh_reasm.capturing   = 0;
    }
  }

  ctx->feature_reasm.capturing = 0;
  if( FD_UNLIKELY( !memcmp( result->account_header.owner, fd_solana_feature_program_id.uc, 32UL ) &&
                   lamports ) ) {
    memcpy( ctx->feature_reasm.pubkey.uc, result->account_header.pubkey, 32UL );
    memcpy( ctx->feature_reasm.owner,     result->account_header.owner,  32UL );
    ctx->feature_reasm.lamports  = lamports;
    ctx->feature_reasm.need      = fd_ulong_min( data_len, sizeof(ctx->feature_reasm.buf) );
    ctx->feature_reasm.write_pos = 0UL;
    ctx->feature_reasm.capturing = 1;
    if( FD_UNLIKELY( !ctx->feature_reasm.need ) ) {
      worker_snoop_feature( ctx, ctx->feature_reasm.pubkey.uc, lamports, ctx->feature_reasm.owner,
                            ctx->feature_reasm.buf, 0UL, ctx->reasm_av_idx, ctx->reasm_rec_idx );
      ctx->feature_reasm.capturing = 0;
    }
  }

  ctx->stake_reasm.capturing = 0;
  if( FD_UNLIKELY( !ignored &&
                   lamports &&
                   data_len>=sizeof(fd_stake_state_t) &&
                   !memcmp( result->account_header.owner, &fd_solana_stake_program_id, sizeof(fd_pubkey_t) ) ) ) {
    memcpy( ctx->stake_reasm.pubkey.uc, result->account_header.pubkey, sizeof(fd_pubkey_t) );
    ctx->stake_reasm.lamports  = lamports;
    ctx->stake_reasm.data_len  = data_len;
    ctx->stake_reasm.write_pos = 0UL;
    ctx->stake_reasm.capturing = 1;
  }

  return 0;
}

static void
worker_process_account_data( fd_snapin_tile_t *            ctx,
                             fd_ssparse_advance_result_t * result ) {
  if( FD_LIKELY( ctx->open_acc.accepted ) ) {
    worker_buffer_write( ctx, ctx->open_acc.file_off+sizeof(fd_accdb_disk_meta_t)+ctx->open_acc.received,
                         result->account_data.data, result->account_data.data_sz );
  }
  ctx->open_acc.received += result->account_data.data_sz;

  if( FD_UNLIKELY( ctx->sh_reasm.capturing ) ) {
    fd_snapio_worker_snoop_t * ws = ctx->my_snoop;
    ulong remaining = ctx->sh_reasm.data_len - ctx->sh_reasm.write_pos;
    ulong copy_sz   = fd_ulong_min( result->account_data.data_sz, remaining );
    fd_memcpy( ws->slot_history.buf + ctx->sh_reasm.write_pos, result->account_data.data, copy_sz );
    ctx->sh_reasm.write_pos += copy_sz;
    if( ctx->sh_reasm.write_pos==ctx->sh_reasm.data_len ) {
      ws->slot_history.captured = 1;
      ctx->sh_reasm.capturing   = 0;
    }
  }

  if( FD_UNLIKELY( ctx->feature_reasm.capturing ) ) {
    ulong remaining = ctx->feature_reasm.need - ctx->feature_reasm.write_pos;
    ulong copy_sz   = fd_ulong_min( result->account_data.data_sz, remaining );
    fd_memcpy( ctx->feature_reasm.buf + ctx->feature_reasm.write_pos, result->account_data.data, copy_sz );
    ctx->feature_reasm.write_pos += copy_sz;
    if( ctx->feature_reasm.write_pos==ctx->feature_reasm.need ) {
      worker_snoop_feature( ctx, ctx->feature_reasm.pubkey.uc, ctx->feature_reasm.lamports,
                            ctx->feature_reasm.owner, ctx->feature_reasm.buf, ctx->feature_reasm.need,
                            ctx->reasm_av_idx, ctx->reasm_rec_idx );
      ctx->feature_reasm.capturing = 0;
    }
  }

  if( FD_UNLIKELY( ctx->stake_reasm.capturing ) ) {
    ulong remaining = sizeof(ctx->stake_reasm.buf) - ctx->stake_reasm.write_pos;
    ulong copy_sz   = fd_ulong_min( result->account_data.data_sz, remaining );
    fd_memcpy( ctx->stake_reasm.buf + ctx->stake_reasm.write_pos, result->account_data.data, copy_sz );
    ctx->stake_reasm.write_pos += copy_sz;
    if( ctx->stake_reasm.write_pos==sizeof(ctx->stake_reasm.buf) ) {
      worker_snoop_stake( ctx, ctx->stake_reasm.pubkey.uc, ctx->stake_reasm.lamports,
                          ctx->stake_reasm.data_len, ctx->stake_reasm.buf, sizeof(ctx->stake_reasm.buf),
                          ctx->reasm_av_idx, ctx->reasm_rec_idx );
      ctx->stake_reasm.capturing = 0;
    }
  }
}

/* Consume one lane data frag, gated on byte coverage.  Returns 1 if the
   frag must be held (reprocessed later, in[].pos preserved). */

static int
worker_handle_data_frag( fd_snapin_tile_t *  ctx,
                         ulong               lane,
                         ulong               chunk,
                         ulong               sz,
                         fd_stem_context_t * stem ) {
  if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR ) ) return 0; /* drain */
  if( FD_UNLIKELY( ctx->state!=FD_SNAPSHOT_STATE_PROCESSING ) ) {
    FD_LOG_WARNING(( "worker received data frag during invalid state %s (%lu)",
                     fd_ssctrl_state_str( (ulong)ctx->state ), (ulong)ctx->state ));
    worker_enter_error( ctx, stem, EPROTO, 0 );
    return 0;
  }

  if( FD_UNLIKELY( chunk<ctx->in[ lane ].chunk0 || chunk>ctx->in[ lane ].wmark || sz>ctx->in[ lane ].mtu ) ) {
    FD_LOG_ERR(( "invalid data frag bounds (chunk=%lu chunk0=%lu wmark=%lu sz=%lu mtu=%lu)", chunk, ctx->in[ lane ].chunk0, ctx->in[ lane ].wmark, sz, ctx->in[ lane ].mtu ));
  }

  for(;;) {
    ulong rem = sz - ctx->in[ lane ].pos;
    if( FD_UNLIKELY( !rem ) ) break;

    ulong limit = ctx->eos_seen ? ULONG_MAX : ctx->covered_until;
    if( FD_UNLIKELY( ctx->cursor>=limit ) ) {
      ctx->cov_stats.hold_reprocess++;
      return 1; /* hold until coverage extends */
    }
    ulong avail = fd_ulong_min( rem, limit-ctx->cursor );

    uchar const * data = (uchar const *)fd_chunk_to_laddr_const( ctx->in[ lane ].wksp, chunk ) + ctx->in[ lane ].pos;

    if( FD_UNLIKELY( !ctx->av_active ) ) {
      if( FD_LIKELY( ctx->fifo_head==ctx->fifo_tail ) ) {
        /* No owned appendvec pending inside the covered region: skip */
        ctx->in[ lane ].pos += avail;
        ctx->cursor         += avail;
        continue;
      }
      fd_snapin_extent_t const * next = &ctx->fifo[ ctx->fifo_head & (FD_SNAPIN_FIFO_CNT-1UL) ];
      if( FD_LIKELY( ctx->cursor<next->body_off ) ) {
        ulong skip = fd_ulong_min( avail, next->body_off-ctx->cursor );
        ctx->in[ lane ].pos += skip;
        ctx->cursor         += skip;
        continue;
      }
      /* cursor at the start of an owned appendvec body: activate */
      FD_TEST( ctx->cursor==next->body_off );
      ctx->cur_av = *next;
      ctx->fifo_head++;
      ctx->av_active   = 1;
      ctx->av_consumed = 0UL;
      ctx->rec_idx     = 0UL;
      fd_ssparse_accv_init( ctx->ssparse, ctx->cur_av.slot, ctx->cur_av.body_sz );
      continue;
    }

    /* Active appendvec: feed the parser */
    ulong feed = fd_ulong_min( avail, ctx->cur_av.body_sz-ctx->av_consumed );
    fd_ssparse_advance_result_t result[1];
    int res = fd_ssparse_advance( ctx->ssparse, data, feed, result );
    int fail = 0;
    switch( res ) {
      case FD_SSPARSE_ADVANCE_ERROR:
        FD_LOG_WARNING(( "worker %lu: error while parsing appendvec (slot %lu)", ctx->worker_idx, ctx->cur_av.slot ));
        fail = 1;
        break;
      case FD_SSPARSE_ADVANCE_AGAIN:
        break;
      case FD_SSPARSE_ADVANCE_ACCOUNT_HEADER:
        fail = worker_process_account_header( ctx, result )<0;
        break;
      case FD_SSPARSE_ADVANCE_ACCOUNT_DATA:
        worker_process_account_data( ctx, result );
        break;
      case FD_SSPARSE_ADVANCE_ACCOUNT_BATCH:
        fail = worker_process_account_batch( ctx, result )<0;
        break;
      default:
        FD_LOG_WARNING(( "worker %lu: unexpected fd_ssparse_advance result %d", ctx->worker_idx, res ));
        fail = 1;
        break;
    }
    if( FD_UNLIKELY( fail ) ) {
      worker_enter_error( ctx, stem, EPROTO, 0 );
      return 0;
    }

    ctx->in[ lane ].pos += result->bytes_consumed;
    ctx->cursor         += result->bytes_consumed;
    ctx->av_consumed    += result->bytes_consumed;
    if( FD_UNLIKELY( ctx->av_consumed==ctx->cur_av.body_sz ) ) ctx->av_active = 0;
  }

  ctx->in[ lane ].pos = 0UL;
  return 0;
}

static inline int
worker_work_complete( fd_snapin_tile_t const * ctx ) {
  return ctx->eos_seen && !ctx->av_active && ctx->fifo_head==ctx->fifo_tail;
}

/* Complete a deferred FINI once EOS has been consumed from the job ring
   and all owned appendvecs drained. */

static void
worker_try_complete_fini( fd_snapin_tile_t *  ctx,
                          fd_stem_context_t * stem ) {
  if( FD_LIKELY( !ctx->pending_fini ) ) return;
  if( FD_UNLIKELY( !worker_work_complete( ctx ) ) ) return;

  /* Make every staged byte durable and hand off the final partition
     (stamps write_offset, books the tail slack) before acking: the
     coordinator's DONE-side readback and load_end run only after all
     FINI acks. */
  worker_buffer_flush( ctx );
  fd_accdb_snapshot_worker_close( ctx->accdb, &ctx->whead );
  fd_accdb_snapshot_writer_end( ctx->accdb );
  fd_accdb_snapshot_flush_worker_metrics( ctx->accdb, ctx->worker_metrics );
  ctx->pending_fini = 0;
  ctx->state        = FD_SNAPSHOT_STATE_FINISHING;

  /* Day-1 throttling instrumentation (D4-ghost watch) */
  double lag_avg = ctx->cov_stats.lag_samples ? (double)ctx->cov_stats.lag_sum/(double)ctx->cov_stats.lag_samples : 0.0;
  FD_LOG_NOTICE(( "worker %lu: coverage holds=%lu, coverage lag min/avg/max=%lu/%.0f/%lu bytes (%lu samples), bytes_written=%lu, wb kicks=%lu waits=%lu",
                  ctx->worker_idx, ctx->cov_stats.hold_reprocess,
                  ctx->cov_stats.lag_min==ULONG_MAX ? 0UL : ctx->cov_stats.lag_min, lag_avg, ctx->cov_stats.lag_max,
                  ctx->cov_stats.lag_samples, ctx->bytes_written, ctx->wb_kick_cnt, ctx->wb_wait_cnt ));

  FD_COMPILER_MFENCE(); /* snoop staging visible before the ack */
  worker_publish_ack( ctx, stem, FD_SNAPSHOT_MSG_CTRL_FINI, 0 );
}

/* Process one job from this worker's ring.  Returns 1 to hold
   (reprocess later). */

static int
worker_handle_job( fd_snapin_tile_t *  ctx,
                   fd_stem_context_t * stem,
                   ulong               chunk,
                   ulong               sz ) {
  if( FD_UNLIKELY( chunk<ctx->io_in[ 0 ].chunk0 || chunk>ctx->io_in[ 0 ].wmark || sz!=sizeof(fd_snapin_io_job_t) ) ) {
    FD_LOG_ERR(( "invalid job frag bounds (chunk=%lu sz=%lu)", chunk, sz ));
  }
  fd_snapin_io_job_t const * job = fd_chunk_to_laddr_const( ctx->io_in[ 0 ].wksp, chunk );

  /* Generation discipline across retries: jobs from a future attempt
     are held until this worker's own INIT barrier catches up; stale
     jobs from an aborted attempt are drained. */
  if( FD_UNLIKELY( job->generation>ctx->generation ) ) return 1;
  if( FD_UNLIKELY( job->generation<ctx->generation ) ) return 0;

  switch( job->kind ) {
    case FD_SNAPIN_IO_KIND_ABORT: {
      if( FD_LIKELY( ctx->state==FD_SNAPSHOT_STATE_PROCESSING ) ) worker_enter_error( ctx, stem, ECANCELED, 1 /* coordinator already knows */ );
      ctx->pending_fini = 0;
      return 0;
    }
    case FD_SNAPIN_IO_KIND_ASSIGN: {
      if( FD_UNLIKELY( ctx->state!=FD_SNAPSHOT_STATE_PROCESSING ) ) return 0; /* stale (post-FAIL) */
      if( FD_UNLIKELY( ctx->fifo_tail-ctx->fifo_head>=FD_SNAPIN_FIFO_CNT ) ) return 1; /* hold until drained */
      /* Adopt/validate the phase fork: USHORT_MAX in a full attempt, a
         single valid fork id across every ASSIGN of an incremental
         attempt (bound to the generation by the ring protocol). */
      if( FD_UNLIKELY( ctx->full ? job->fork_id!=(ulong)USHORT_MAX
                                 : ( job->fork_id>=(ulong)USHORT_MAX ||
                                     ( ctx->incr_fork!=ULONG_MAX && job->fork_id!=ctx->incr_fork ) ) ) ) {
        worker_enter_error( ctx, stem, EPROTO, 0 );
        return 0;
      }
      if( FD_UNLIKELY( !ctx->full ) ) ctx->incr_fork = job->fork_id;
      fd_snapin_extent_t * ext = &ctx->fifo[ ctx->fifo_tail & (FD_SNAPIN_FIFO_CNT-1UL) ];
      ext->body_off      = job->body_off;
      ext->body_sz       = job->body_sz;
      ext->slot          = job->slot;
      ext->appendvec_idx = job->appendvec_idx;
      ctx->fifo_tail++;
      ulong end = job->body_off + fd_ulong_align_up( job->body_sz, 512UL );
      worker_cov_advance( ctx, fd_ulong_max( end, job->covered_until ) );
      return 0;
    }
    case FD_SNAPIN_IO_KIND_WATERMARK: {
      if( FD_UNLIKELY( ctx->state!=FD_SNAPSHOT_STATE_PROCESSING ) ) return 0;
      worker_cov_advance( ctx, job->covered_until );
      return 0;
    }
    case FD_SNAPIN_IO_KIND_EOS: {
      if( FD_UNLIKELY( ctx->state!=FD_SNAPSHOT_STATE_PROCESSING ) ) return 0;
      ctx->eos_seen = 1;
      return 0;
    }
    default:
      FD_LOG_ERR(( "unexpected job kind %lu", job->kind ));
  }
  return 0;
}

static void
worker_handle_control_frag( fd_snapin_tile_t *  ctx,
                            fd_stem_context_t * stem,
                            ulong               sig ) {
  if( ctx->state==FD_SNAPSHOT_STATE_ERROR && sig!=FD_SNAPSHOT_MSG_CTRL_FAIL ) return;

  switch( sig ) {
    case FD_SNAPSHOT_MSG_CTRL_INIT_FULL: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
      /* generation was already bumped at the first INIT frag (see
         handle_control_barrier) */
      ctx->state = FD_SNAPSHOT_STATE_PROCESSING;
      ctx->full  = 1;
      worker_reset_attempt( ctx );
      fd_accdb_snapshot_writer_begin( ctx->accdb );
      worker_publish_ack( ctx, stem, sig, 0 );
      break;
    }
    case FD_SNAPSHOT_MSG_CTRL_INIT_INCR: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
      /* Same lifecycle as INIT_FULL (generation already bumped at the
         first INIT frag); the attempt's accdb fork arrives with the
         ASSIGN jobs.  Per-attempt counters reset exactly as at
         INIT_FULL: the coordinator re-folds every FINI ack, so acking
         stale full-phase counts would double-count them. */
      ctx->state = FD_SNAPSHOT_STATE_PROCESSING;
      ctx->full  = 0;
      worker_reset_attempt( ctx );
      fd_accdb_snapshot_writer_begin( ctx->accdb );
      worker_publish_ack( ctx, stem, sig, 0 );
      break;
    }
    case FD_SNAPSHOT_MSG_META: {
      /* No action, no ack (the coordinator does not gate on META). */
      break;
    }
    case FD_SNAPSHOT_MSG_CTRL_FINI: {
      if( FD_UNLIKELY( ctx->state!=FD_SNAPSHOT_STATE_PROCESSING ) ) {
        worker_enter_error( ctx, stem, EPROTO, 0 );
        break;
      }
      /* All lane data precedes FINI, so owned appendvecs are complete;
         only the EOS job may still be in flight on the ring.  Defer the
         ack (do NOT hold the lane: a FAIL may be queued behind this
         frag). */
      ctx->pending_fini = 1;
      worker_try_complete_fini( ctx, stem );
      break;
    }
    case FD_SNAPSHOT_MSG_CTRL_NEXT:
    case FD_SNAPSHOT_MSG_CTRL_DONE: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
      ctx->state = FD_SNAPSHOT_STATE_IDLE;
      worker_publish_ack( ctx, stem, sig, 0 );
      break;
    }
    case FD_SNAPSHOT_MSG_CTRL_ERROR: {
      FD_TEST( ctx->state!=FD_SNAPSHOT_STATE_SHUTDOWN );
      /* Not silent: the coordinator may be waiting on this worker's ack
         of an in-flight control that will now never come; the ERROR ack
         makes it abandon that wait. */
      worker_enter_error( ctx, stem, EPIPE, 0 );
      break;
    }
    case FD_SNAPSHOT_MSG_CTRL_FAIL: {
      FD_TEST( ctx->state!=FD_SNAPSHOT_STATE_SHUTDOWN );
      ctx->state = FD_SNAPSHOT_STATE_IDLE;
      /* Drop staging and forget the private write head: the partitions
         themselves are released by the coordinator's fd_accdb_reset,
         which it only runs after every worker acked. */
      worker_reset_attempt( ctx );
      fd_accdb_snapshot_writer_end( ctx->accdb );
      worker_publish_ack( ctx, stem, sig, 0 );
      break;
    }
    case FD_SNAPSHOT_MSG_CTRL_SHUTDOWN: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
      ctx->state = FD_SNAPSHOT_STATE_SHUTDOWN;
      worker_publish_ack( ctx, stem, sig, 0 );
      break;
    }
    default: {
      FD_LOG_ERR(( "unexpected control frag %s (%lu) in state %s (%lu)",
                   fd_ssctrl_msg_ctrl_str( sig ), sig,
                   fd_ssctrl_state_str( (ulong)ctx->state ), (ulong)ctx->state ));
      break;
    }
  }
}

/* Coordinator role ***************************************************/

/* Coordinator: merge the workers' snoop staging (valid only after all
   FINI acks).  Reproduces the sequential loader's stream-order
   semantics: slot history by (slot, appendvec_idx, record_idx)
   precedence, features by per-id stream position, stakes by a k-way
   stream-order merge of the per-worker logs (each already stream
   sorted). */

static void
walker_merge_snoops( fd_snapin_tile_t * ctx ) {
  if( FD_LIKELY( !ctx->io_enabled ) ) return;
  if( FD_UNLIKELY( ctx->snoop_merged ) ) return;
  ctx->snoop_merged = 1;

  /* Slot history */
  ulong best = ULONG_MAX;
  for( ulong w=0UL; w<ctx->worker_cnt; w++ ) {
    fd_snapio_worker_snoop_t const * ws = ctx->snoops[ w ];
    if( !ws->slot_history.captured ) continue;
    if( best==ULONG_MAX ) { best = w; continue; }
    fd_snapio_worker_snoop_t const * bs = ctx->snoops[ best ];
    ulong  a[3] = { ws->slot_history.slot, ws->slot_history.appendvec_idx, ws->slot_history.record_idx };
    ulong  b[3] = { bs->slot_history.slot, bs->slot_history.appendvec_idx, bs->slot_history.record_idx };
    int wins = a[0]!=b[0] ? a[0]>b[0] : ( a[1]!=b[1] ? a[1]>b[1] : a[2]>b[2] );
    if( wins ) best = w;
  }
  if( FD_LIKELY( best!=ULONG_MAX ) ) {
    fd_snapio_worker_snoop_t const * ws = ctx->snoops[ best ];
    ctx->slot_history.captured   = 1;
    ctx->slot_history.capturing  = 0;
    ctx->slot_history.slot       = ws->slot_history.slot;
    ctx->slot_history.lamports   = ws->slot_history.lamports;
    ctx->slot_history.data_len   = ws->slot_history.data_len;
    ctx->slot_history.executable = ws->slot_history.executable;
    fd_memcpy( ctx->slot_history.owner, ws->slot_history.owner, 32UL );
    fd_memcpy( ctx->slot_history.buf, ws->slot_history.buf, ws->slot_history.data_len );
  }

  /* Features */
  for( ulong id=0UL; id<FD_FEATURE_SNOOP_CNT; id++ ) {
    ulong fbest = ULONG_MAX;
    for( ulong w=0UL; w<ctx->worker_cnt; w++ ) {
      fd_snapio_worker_snoop_t const * ws = ctx->snoops[ w ];
      if( !ws->feature_snoop.present[ id ] ) continue;
      if( fbest==ULONG_MAX ) { fbest = w; continue; }
      fd_snapio_worker_snoop_t const * bs = ctx->snoops[ fbest ];
      int wins = ws->feature_av[ id ]!=bs->feature_av[ id ]
               ? ws->feature_av[ id ]>bs->feature_av[ id ]
               : ws->feature_rec[ id ]>bs->feature_rec[ id ];
      if( wins ) fbest = w;
    }
    if( fbest!=ULONG_MAX ) {
      fd_snapio_worker_snoop_t const * ws = ctx->snoops[ fbest ];
      ctx->feature_snoop->present        [ id ] = 1;
      ctx->feature_snoop->is_active      [ id ] = ws->feature_snoop.is_active[ id ];
      ctx->feature_snoop->activation_slot[ id ] = ws->feature_snoop.activation_slot[ id ];
    }
  }

  /* Stakes: k-way merge in stream order */
  fd_stake_delegations_t * sd = fd_banks_stake_delegations_root_query( ctx->banks );
  ulong idx[ FD_SNAPIO_WORKER_MAX ] = {0};
  ulong merged = 0UL;
  for(;;) {
    ulong sbest = ULONG_MAX;
    fd_snapio_stake_ent_t const * best_ent = NULL;
    for( ulong w=0UL; w<ctx->worker_cnt; w++ ) {
      fd_snapio_worker_snoop_t * ws = ctx->snoops[ w ];
      if( idx[ w ]>=ws->stake_cnt ) continue;
      fd_snapio_stake_ent_t const * ent = &fd_snapio_worker_stake_log( ws )[ idx[ w ] ];
      if( !best_ent ||
          ent->appendvec_idx<best_ent->appendvec_idx ||
          ( ent->appendvec_idx==best_ent->appendvec_idx && ent->record_idx<best_ent->record_idx ) ) {
        sbest = w; best_ent = ent;
      }
    }
    if( sbest==ULONG_MAX ) break;
    fd_stake_delegations_root_update(
        sd,
        (fd_pubkey_t const *)best_ent->pubkey,
        (fd_pubkey_t const *)best_ent->voter,
        best_ent->stake,
        best_ent->activation_epoch,
        best_ent->deactivation_epoch,
        best_ent->credits_observed,
        best_ent->lamports,
        best_ent->data_len,
        FD_STAKE_DELEGATIONS_WARMUP_COOLDOWN_RATE_ENUM_025 );
    idx[ sbest ]++;
    merged++;
  }
  FD_LOG_INFO(( "merged snoop staging: %lu stake delegation updates", merged ));
}

/* Diagnostics for the verification gates: order-independent checksums
   of the merged snoop state (compare against a D5/sequential run of the
   same snapshot), and the equal-slot duplicate instrumentation. */

static void
log_snoop_checksums( fd_snapin_tile_t * ctx ) {
  fd_stake_delegations_t * sd = fd_banks_stake_delegations_root_query( ctx->banks );
  fd_stake_delegation_t const * root_pool  = fd_type_pun_const( (uchar const *)sd + sd->pool_offset_ );
  fd_stake_delegation_t const * delta_pool = fd_type_pun_const( (uchar const *)sd + sd->delta_pool_offset_ );
  ulong stake_cs  = 0UL;
  ulong stake_cnt = 0UL;
  for( ulong i=0UL; i<sd->pool_idx_wmk_; i++ ) {
    fd_stake_delegation_t const * d = &root_pool[ i ];
    if( !d->in_use ) continue;
    if( d->delta_idx!=UINT_MAX ) d = &delta_pool[ d->delta_idx ];
    if( d->is_tombstone ) continue;
    ulong h = fd_hash( 0x57A4EUL, d->stake_account.uc, 32UL );
    h = fd_hash( h, d->vote_account.uc, 32UL );
    ulong nums[ 6 ] = { d->stake, d->lamports, d->credits_observed,
                        (ulong)d->activation_epoch, (ulong)d->deactivation_epoch, (ulong)d->acc_dlen };
    h = fd_hash( h, nums, sizeof(nums) );
    stake_cs += h; /* commutative: iteration order independent */
    stake_cnt++;
  }
  ulong feature_cs = fd_hash( 0xFEA7UL, ctx->feature_snoop, sizeof(fd_feature_snoop_t) );
  ulong sh_cs      = ctx->slot_history.captured ? fd_hash( 0x5107UL, ctx->slot_history.buf, ctx->slot_history.data_len ) : 0UL;
  FD_LOG_NOTICE(( "snoop A/B: stake_cnt=%lu stake_cs=%016lx feature_cs=%016lx slot_history_slot=%lu slot_history_cs=%016lx",
                  stake_cnt, stake_cs, feature_cs, ctx->slot_history.captured ? ctx->slot_history.slot : 0UL, sh_cs ));
}

/* Step-0 measurement: appendvec count + size distribution, feeds the
   giant-appendvec serialization kill-criterion. */

static void
log_appendvec_stats( fd_snapin_tile_t * ctx ) {
  if( FD_UNLIKELY( !ctx->av_stats.cnt ) ) return;

  /* Approximate percentiles from the log2 histogram (upper bucket
     bound). */
  ulong p50 = 0UL, p90 = 0UL;
  ulong seen = 0UL;
  for( ulong b=0UL; b<48UL; b++ ) {
    seen += ctx->av_stats.log2_hist[ b ];
    if( !p50 && seen*2UL >=      ctx->av_stats.cnt ) p50 = 2UL<<b;
    if( !p90 && seen*10UL>= 9UL*ctx->av_stats.cnt  ) p90 = 2UL<<b;
  }
  FD_LOG_NOTICE(( "appendvecs: cnt=%lu total=%.1f GiB avg=%.1f MiB p50<=%lu p90<=%lu max=%lu, >64MiB cnt=%lu, >256MiB bytes=%.1f GiB",
                  ctx->av_stats.cnt,
                  (double)ctx->av_stats.bytes/(double)(1UL<<30),
                  (double)ctx->av_stats.bytes/(double)ctx->av_stats.cnt/(double)(1UL<<20),
                  p50, p90, ctx->av_stats.max_sz,
                  ctx->av_stats.over_64m_cnt,
                  (double)ctx->av_stats.over_256m_bytes/(double)(1UL<<30) ));
}

/* Finish reverting a failed attempt only after every accdb worker has
   acknowledged FAIL.  fd_accdb_reset and the incremental purge/revert
   path require that no other joiner is still mutating shared state. */
static void
coordinator_finish_failed_attempt( fd_snapin_tile_t * ctx ) {
  if( FD_LIKELY( ctx->init_completed ) ) {
    if( ctx->full ) {
      fd_accdb_reset( ctx->accdb );
      ctx->accdb_root_fork_id = (fd_accdb_fork_id_t){ .val = USHORT_MAX };
      ctx->accdb_incr_fork_id = (fd_accdb_fork_id_t){ .val = USHORT_MAX };
    } else {
      fd_accdb_purge( ctx->accdb, ctx->accdb_incr_fork_id ); /* this fork and subsequent children */
      /* revert_whead is whead[0]-based; in io mode NEXT never saved it
         (recovery.accdb_metadata is still the boot-time zero) and
         reverting to partition_max=0 would release EVERY partition,
         destroying the loaded full snapshot.  Explicit-offset workers
         instead leak the failed attempt's partitions (purge unlinks the
         index entries, so the stale bytes are unreachable). */
      if( FD_LIKELY( !ctx->io_enabled ) ) fd_accdb_snapshot_revert_whead( ctx->accdb, &ctx->recovery.accdb_metadata );
      ctx->accdb_incr_fork_id = (fd_accdb_fork_id_t){ .val = USHORT_MAX };
      *ctx->feature_snoop = ctx->recovery.feature_snoop;
    }
  }
  ctx->init_completed = 0;
}

/* Process one worker ack.  Returns 1 to hold the ack frag (reprocess
   later): workers take controls from their OWN lane barriers, so a
   worker can legitimately complete a barrier and ack it (or even bump
   into the next attempt's generation) BEFORE the coordinator has
   consumed its own copies of the control frags.  Such "early" acks
   must be held until the coordinator's barrier catches up — dropping
   them wedges the ack mask (and thus the whole pipeline) forever.
   The race window is the skew between snapdc tiles' control forwards
   plus one stem poll rotation, and every worker is a lottery ticket. */

static int
coordinator_handle_ack( fd_snapin_tile_t *  ctx,
                        fd_stem_context_t * stem,
                        ulong               worker_idx,
                        ulong               sig,
                        ulong               chunk,
                        ulong               sz ) {
  if( FD_UNLIKELY( worker_idx>=ctx->worker_cnt ||
                   chunk<ctx->io_in[ worker_idx ].chunk0 || chunk>ctx->io_in[ worker_idx ].wmark ||
                   sz!=sizeof(fd_snapin_io_ack_t) ) ) {
    FD_LOG_ERR(( "malformed worker ack (worker=%lu chunk=%lu sz=%lu)", worker_idx, chunk, sz ));
  }
  fd_snapin_io_ack_t const * ack = fd_chunk_to_laddr_const( ctx->io_in[ worker_idx ].wksp, chunk );
  ulong generation = fd_snapin_io_ack_generation( sig );
  ulong control    = fd_snapin_io_ack_control( sig );
  if( FD_UNLIKELY( ack->worker_idx!=worker_idx || generation!=ack->generation || control!=ack->control ) ) {
    FD_LOG_ERR(( "inconsistent worker ack (worker=%lu sig=%lx)", worker_idx, sig ));
  }

  /* Future-attempt acks: the worker's INIT barrier (which bumps its
     generation) completed before ours.  Hold until our own INIT frags
     bump us into the same attempt.  Past-attempt acks are stale. */
  if( FD_UNLIKELY( generation>ctx->generation ) ) return 1;
  if( FD_UNLIKELY( generation<ctx->generation ) ) return 0;

  if( FD_UNLIKELY( control==FD_SNAPSHOT_MSG_CTRL_ERROR ) ) {
    FD_LOG_WARNING(( "snapshot accdb worker %lu reported error (%d-%s)", worker_idx, ack->err, fd_io_strerror( ack->err ) ));
    /* Abandon any in-flight ack wait (except FAIL, which every worker
       still acks) even if we already transitioned to ERROR. */
    if( ctx->pending_worker_control!=FD_SNAPSHOT_MSG_CTRL_FAIL ) {
      ctx->pending_worker_control  = ULONG_MAX;
      ctx->pending_worker_ack_mask = 0UL;
    }
    transition_malformed( ctx, stem );
    return 0;
  }

  if( FD_UNLIKELY( ctx->pending_worker_control==ULONG_MAX ) ) {
    /* Acks for a control the coordinator abandoned after an error are
       dropped while the FAIL flows (they are queued ahead of the FAIL
       ack on the same link and must not block it).  Anything else with
       no pending control is an early ack: hold it. */
    if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR && control!=FD_SNAPSHOT_MSG_CTRL_FAIL ) ) return 0;
    return 1;
  }
  if( FD_UNLIKELY( control!=ctx->pending_worker_control ) ) {
    /* Stale ack for a control abandoned after an error (e.g. a FINI
       ack queued ahead of the FAIL ack): drop.  A clean flow cannot
       produce this (the coordinator holds its lanes until the pending
       control's ack mask completes, so workers cannot run a barrier
       ahead). */
    return 0;
  }

  ulong worker_bit = 1UL<<worker_idx;
  if( FD_UNLIKELY( ctx->pending_worker_ack_mask & worker_bit ) ) {
    FD_LOG_ERR(( "duplicate worker ack (worker=%lu control=%lu)", worker_idx, control ));
  }

  if( FD_UNLIKELY( control==FD_SNAPSHOT_MSG_CTRL_FINI ) ) {
    /* Fold the worker's end-of-load counters */
    ctx->metrics.accounts_ignored  += ack->accounts_ignored;
    ctx->metrics.accounts_replaced += ack->accounts_replaced;
    ctx->metrics.accounts_loaded   += ack->accounts_loaded;
    ctx->capitalization     = fd_ulong_sat_add( ctx->capitalization, ack->input_lamports );
    ctx->capitalization     = fd_ulong_sat_sub( ctx->capitalization, ack->ignored_lamports );
    ctx->dup_capitalization = fd_ulong_sat_add( ctx->dup_capitalization, ack->replaced_lamports );
    ctx->worker_fold.eq_slot_dups          += ack->eq_slot_dups;
    ctx->worker_fold.eq_slot_lamports_diff += ack->eq_slot_lamports_diff;
    ctx->worker_fold.bytes_written         += ack->bytes_written;
  }

  ctx->pending_worker_ack_mask |= worker_bit;
  ulong all_worker_mask = (1UL<<ctx->worker_cnt)-1UL;
  if( FD_LIKELY( ctx->pending_worker_ack_mask!=all_worker_mask ) ) return 0;

  ulong completed = ctx->pending_worker_control;
  ctx->pending_worker_control  = ULONG_MAX;
  ctx->pending_worker_ack_mask = 0UL;

  if( FD_UNLIKELY( completed==FD_SNAPSHOT_MSG_CTRL_FINI && ctx->worker_fold.eq_slot_dups ) ) {
    /* Equal-slot cross-appendvec duplicates were ignored without a
       stream-order tiebreak; the load result would be
       schedule-dependent, so hard-fail the snapshot. */
    FD_LOG_WARNING(( "parallel loader: %lu equal-slot cross-appendvec duplicates (lamports-diff=%lu) cannot be tiebroken; flagging snapshot malformed",
                     ctx->worker_fold.eq_slot_dups, ctx->worker_fold.eq_slot_lamports_diff ));
    transition_malformed( ctx, stem );
    return 0;
  }

  if( FD_UNLIKELY( completed==FD_SNAPSHOT_MSG_CTRL_FAIL ) ) coordinator_finish_failed_attempt( ctx );
  fd_stem_publish( stem, ctx->ct_out.idx, completed, 0UL, 0UL, 0UL, 0UL, 0UL );
  return 0;
}

static void
handle_control_frag( fd_snapin_tile_t *  ctx,
                     fd_stem_context_t * stem,
                     ulong               in_idx,
                     ulong               sig,
                     ulong               chunk,
                     ulong               sz ) {
  if( ctx->state==FD_SNAPSHOT_STATE_ERROR && sig!=FD_SNAPSHOT_MSG_CTRL_FAIL ) {
    /* Control messages move along the snapshot load pipeline.  Since
       error conditions can be triggered by any tile in the pipeline,
       it is possible to be in error state and still receive otherwise
       valid messages.  Only a fail message can revert this. */
    return;
  };

  int forward_msg = 1;

  switch( sig ) {
    case FD_SNAPSHOT_MSG_CTRL_INIT_FULL:
    case FD_SNAPSHOT_MSG_CTRL_INIT_INCR: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_IDLE );
      ctx->state = FD_SNAPSHOT_STATE_PROCESSING;
      ctx->full = sig==FD_SNAPSHOT_MSG_CTRL_INIT_FULL;
      ctx->expected_frame = 0UL;
      for( ulong i=0UL; i<ctx->lane_cnt; i++ ) {
        ctx->in[ i ].pos = 0UL;
      }
      ctx->blockhash_groups_len    = 0UL;
      ctx->manifest_capitalization = 0UL;

      ctx->txncache_slots_len = 0UL;

      if( FD_UNLIKELY( ctx->io_enabled ) ) {
        /* generation was already bumped at the first INIT frag (see
           handle_control_barrier) */
        ctx->stream_cursor            = 0UL;
        ctx->appendvec_seq            = 0UL;
        ctx->covered_until            = 0UL;
        ctx->io_watermark_dirty       = 0;
        ctx->io_frags_since_watermark = 0UL;
        ctx->abort_published          = 0;
        ctx->snoop_merged             = 0;
        fd_memset( ctx->worker_assigned_bytes, 0, sizeof(ctx->worker_assigned_bytes) );
        fd_memset( &ctx->worker_fold, 0, sizeof(ctx->worker_fold) );
        fd_memset( &ctx->av_stats, 0, sizeof(ctx->av_stats) );
      }

      fd_txncache_reset( ctx->txncache );
      fd_ssparse_init( ctx->ssparse );
      fd_ssparse_batch_enable( ctx->ssparse, 1 );
      fd_ssparse_appendvec_passthrough_enable( ctx->ssparse, ctx->io_enabled );
      fd_ssmanifest_parser_init( ctx->manifest_parser, fd_chunk_to_laddr( ctx->manifest_out.mem, ctx->manifest_out.chunk ) );
      fd_slot_delta_parser_init( ctx->slot_delta_parser );
      fd_memset( &ctx->flags,    0, sizeof(ctx->flags)    );

      /* Rewind metric counters (no-op unless recovering from a fail) */
      if( sig==FD_SNAPSHOT_MSG_CTRL_INIT_FULL ) {
        ctx->metrics.accounts_loaded   = ctx->metrics.full_accounts_loaded   = 0;
        ctx->metrics.accounts_replaced = ctx->metrics.full_accounts_replaced = 0;
        ctx->metrics.accounts_ignored  = ctx->metrics.full_accounts_ignored  = 0;
        ctx->metrics.full_bytes_read   = 0UL;
        ctx->metrics.incremental_bytes_read = 0UL;
        ctx->full_genesis_creation_time_seconds = 0UL;
        ctx->capitalization                     = 0UL;
        ctx->dup_capitalization                 = 0UL;
        ctx->recovery.capitalization            = 0UL;

        fd_stake_delegations_reset( fd_banks_stake_delegations_root_query( ctx->banks ) );
        fd_accdb_reset( ctx->accdb );
        fd_accdb_fork_id_t null_fork_id = (fd_accdb_fork_id_t){ .val = USHORT_MAX };
        ctx->accdb_root_fork_id = fd_accdb_attach_child( ctx->accdb, null_fork_id );

        if( FD_UNLIKELY( ctx->io_enabled ) ) fd_accdb_snapshot_load_begin_with_writers( ctx->accdb, ctx->worker_cnt );
        else                                 fd_accdb_snapshot_load_begin( ctx->accdb );

        ctx->slot_history.captured  = 0;
        ctx->slot_history.capturing = 0;

        fd_memset( ctx->feature_snoop, 0, sizeof(ctx->feature_snoop) );
        ctx->feature_reasm.capturing = 0;
        ctx->stake_reasm.capturing   = 0;
      } else {
        ctx->metrics.accounts_loaded   = ctx->metrics.full_accounts_loaded;
        ctx->metrics.accounts_replaced = ctx->metrics.full_accounts_replaced;
        ctx->metrics.accounts_ignored  = ctx->metrics.full_accounts_ignored;
        ctx->metrics.incremental_bytes_read = 0UL;

        ctx->capitalization     = ctx->recovery.capitalization;
        ctx->dup_capitalization = 0UL;

        /* Discard stale capture so the retry's sysvar is snooped fresh */
        ctx->slot_history.captured  = 0;
        ctx->slot_history.capturing = 0;
        ctx->feature_reasm.capturing = 0;
        ctx->stake_reasm.capturing   = 0;

        /* Create a child fork for incremental writes.  On failure,
           fd_accdb_purge(child) reverts just the incremental changes.
           On success, fd_accdb_advance_root(child) promotes them. */
        ctx->accdb_incr_fork_id = fd_accdb_attach_child( ctx->accdb, ctx->accdb_root_fork_id );
      }

      /* Save the slot advertised by the snapshot peer and verify it
         against the slot in the snapshot manifest.  For redirect-based
         HTTP downloads, these are initial estimates from gossip and
         will be updated by the META message below once the redirect
         resolves to a concrete snapshot filename. */
      fd_ssctrl_init_t const * msg = fd_chunk_to_laddr_const( ctx->in[ in_idx ].wksp, chunk );
      ctx->advertised_slot = msg->slot;
      fd_memcpy( ctx->advertised_hash, msg->snapshot_hash, FD_HASH_FOOTPRINT );
      ctx->init_completed = 1;
      break;
    }

    case FD_SNAPSHOT_MSG_META: {
      /* For redirect-based HTTP downloads, the META message carries
         the resolved slot and hash from the actual snapshot filename
         the server redirected to.  Update the advertised values so
         that process_manifest can verify the manifest against them. */
      FD_TEST( sz==sizeof(fd_ssctrl_meta_t) );
      fd_ssctrl_meta_t const * meta = fd_chunk_to_laddr_const( ctx->in[ in_idx ].wksp, chunk );
      if( meta->resolved_slot!=ULONG_MAX ) {
        ctx->advertised_slot = meta->resolved_slot;
        fd_memcpy( ctx->advertised_hash, meta->resolved_hash, FD_HASH_FOOTPRINT );
      }
      forward_msg = 0; /* snapct already receives META directly from snapld */
      break;
    }

    case FD_SNAPSHOT_MSG_CTRL_FINI: {
      /* This is a special case: handle_data_frag must have already
         processed FD_SSPARSE_ADVANCE_DONE and moved the state into
         FD_SNAPSHOT_STATE_FINISHING.  Otherwise, treat this as a
         malformed snapshot so that the pipeline can retry. */
      if( FD_UNLIKELY( ctx->state!=FD_SNAPSHOT_STATE_FINISHING ) ) {
        FD_LOG_WARNING(( "received FINI while in state %s (%lu), expected FINISHING (possibly truncated tar stream)",
                         fd_ssctrl_state_str( (ulong)ctx->state ), (ulong)ctx->state ));
        transition_malformed( ctx, stem );
        forward_msg = 0;
        break;
      }
      if( FD_UNLIKELY( ctx->io_enabled ) ) log_appendvec_stats( ctx );
      break;
    }

    case FD_SNAPSHOT_MSG_CTRL_NEXT: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
      ctx->state = FD_SNAPSHOT_STATE_IDLE;

      /* Workers have all acked FINI by the time NEXT arrives (the
         coordinator only forwarded FINI to snapct after the ack mask
         completed), so their snoop staging is complete and quiescent. */
      walker_merge_snoops( ctx );

      if( FD_UNLIKELY( verify_slot_deltas_with_slot_history( ctx ) ) ) {
        FD_LOG_WARNING(( "slot deltas verification failed for full snapshot" ));
        transition_malformed( ctx, stem );
        forward_msg = 0;
        break;
      }

      ctx->capitalization = fd_ulong_sat_sub( ctx->capitalization, ctx->dup_capitalization );
      if( FD_UNLIKELY( validate_capitalization( ctx )!=0 ) ) {
        transition_malformed( ctx, stem );
        forward_msg = 0;
        break;
      }

      ctx->recovery.capitalization = ctx->capitalization;
      /* save_whead is whead[0]-based and meaningless under explicit
         per-worker offsets; INIT_INCR hard-errors in io mode anyway. */
      if( FD_LIKELY( !ctx->io_enabled ) ) fd_accdb_snapshot_save_whead( ctx->accdb, &ctx->recovery.accdb_metadata );
      ctx->recovery.feature_snoop = *ctx->feature_snoop;

      /* Backup metric counters */
      ctx->metrics.full_accounts_loaded   = ctx->metrics.accounts_loaded;
      ctx->metrics.full_accounts_replaced = ctx->metrics.accounts_replaced;
      ctx->metrics.full_accounts_ignored  = ctx->metrics.accounts_ignored;
      ctx->init_completed = 0;
      break;
    }

    case FD_SNAPSHOT_MSG_CTRL_DONE: {
      FD_TEST( ctx->state==FD_SNAPSHOT_STATE_FINISHING );
      ctx->state = FD_SNAPSHOT_STATE_IDLE;

      walker_merge_snoops( ctx );

      if( FD_UNLIKELY( verify_slot_deltas_with_slot_history( ctx ) ) ) {
        if( ctx->full ) FD_LOG_WARNING(( "slot deltas verification failed for full snapshot" ));
        else            FD_LOG_WARNING(( "slot deltas verification failed for incremental snapshot" ));
        transition_malformed( ctx, stem );
        forward_msg = 0;
        break;
      }

      ctx->capitalization = fd_ulong_sat_sub( ctx->capitalization, ctx->dup_capitalization );
      if( FD_UNLIKELY( validate_capitalization( ctx )!=0 ) ) {
        transition_malformed( ctx, stem );
        forward_msg = 0;
        break;
      }

      /* Multi-writer layout is not stream-ordered: gate on a sampled
         index->file readback (workers flushed + closed their partitions
         before acking FINI, so the bytes are visible here).  Must run
         BEFORE advance_root below: the promotion is asynchronous and
         background_advance_root concurrently unlinks + defer-frees
         shadowed full entries, which can be recycled under our chain
         walk (this joiner publishes no epoch).  Pre-promotion, both the
         old and new versions are chain-linked with valid on-disk
         records, so the readback covers both phases. */
      if( FD_UNLIKELY( ctx->io_enabled ) ) fd_accdb_snapshot_verify_readback( ctx->accdb, 100000UL );

      if( !ctx->full ) {
        fd_accdb_snapshot_recover_delta( ctx->accdb, ctx->accdb_incr_fork_id );
        /* ensure that snapin tile sees all delta changes before rooting */
        __atomic_thread_fence( __ATOMIC_SEQ_CST );
        fd_accdb_advance_root( ctx->accdb, ctx->accdb_incr_fork_id );
        ctx->accdb_root_fork_id = ctx->accdb_incr_fork_id;
        ctx->accdb_incr_fork_id = (fd_accdb_fork_id_t){ .val = USHORT_MAX };
      }

      fd_accdb_snapshot_load_end( ctx->accdb );

      fd_feature_snoop_finalize( &ctx->bank->f.features, ctx->bank_slot, &ctx->epoch_schedule, ctx->feature_snoop );

      if( FD_UNLIKELY( ctx->io_enabled ) ) {
        FD_LOG_NOTICE(( "parallel loader: equal-slot cross-appendvec dups=%lu (lamports-diff=%lu), worker bytes written=%lu",
                        ctx->worker_fold.eq_slot_dups, ctx->worker_fold.eq_slot_lamports_diff,
                        ctx->worker_fold.bytes_written ));
      }
      log_snoop_checksums( ctx );

      /* Notify replay when snapshot is fully loaded and verified. */
      fd_stem_publish( stem, ctx->manifest_out.idx, fd_ssmsg_sig( FD_SSMSG_DONE ), 0UL, 0UL, 0UL, 0UL, 0UL );
      ctx->init_completed = 0;
      break;
    }

    case FD_SNAPSHOT_MSG_CTRL_ERROR: {
      FD_TEST( ctx->state!=FD_SNAPSHOT_STATE_SHUTDOWN );
      ctx->state = FD_SNAPSHOT_STATE_ERROR;
      /* Unblock workers holding on byte coverage so the upcoming FAIL
         can flow through the lanes. */
      publish_abort( ctx, stem );
      if( ctx->io_enabled && ctx->pending_worker_control!=FD_SNAPSHOT_MSG_CTRL_FAIL ) {
        ctx->pending_worker_control  = ULONG_MAX;
        ctx->pending_worker_ack_mask = 0UL;
      }
      break;
    }

    case FD_SNAPSHOT_MSG_CTRL_FAIL: {
      FD_TEST( ctx->state!=FD_SNAPSHOT_STATE_SHUTDOWN );
      ctx->state = FD_SNAPSHOT_STATE_IDLE;
      if( FD_LIKELY( !ctx->io_enabled ) ) coordinator_finish_failed_attempt( ctx );
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

  /* Forward the control message down the pipeline.  With workers
     attached, every barriered control (except ERROR, which propagates
     immediately) is only forwarded to snapct once all workers have
     acknowledged it on their own lane barriers; lane consumption is
     held (before_frag) until then. */
  if( FD_LIKELY( forward_msg ) ) {
    if( FD_UNLIKELY( ctx->io_enabled && sig!=FD_SNAPSHOT_MSG_CTRL_ERROR ) ) {
      FD_TEST( ctx->pending_worker_control==ULONG_MAX );
      ctx->pending_worker_control  = sig;
      ctx->pending_worker_ack_mask = 0UL;
    } else {
      fd_stem_publish( stem, ctx->ct_out.idx, sig, 0UL, 0UL, 0UL, 0UL, 0UL );
    }
  }
}

static inline int
all_controls_seen( fd_snapin_tile_t const * ctx ) {
  int all_seen = 1;
  for( ulong i=0UL; i<ctx->lane_cnt; i++ ) {
    all_seen &= !!ctx->control_seen[ i ];
  }
  return all_seen;
}

static inline ulong
ack_worker_idx( fd_snapin_tile_t const * ctx,
                ulong                    in_idx ) {
  for( ulong worker_idx=0UL; worker_idx<ctx->worker_cnt; worker_idx++ ) {
    if( ctx->ack_in_idx[ worker_idx ]==in_idx ) return worker_idx;
  }
  return ULONG_MAX;
}

static inline int
before_frag( fd_snapin_tile_t * ctx,
             ulong              in_idx,
             ulong              seq    FD_PARAM_UNUSED,
             ulong              sig ) {
  if( FD_UNLIKELY( is_accdb_worker( ctx ) ) ) {
    /* Ring jobs are always processed (worker_handle_job may hold them).
       Lane frags share the coordinator's barrier/rotation discipline;
       coverage holds happen in returnable_frag so the scan position is
       preserved. */
    if( FD_LIKELY( in_idx==ctx->job_in_idx ) ) return 0;
    ulong lane = ctx->in_lane[ in_idx ];
    if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR ) ) {
      return sig!=FD_SNAPSHOT_MSG_CTRL_FAIL;
    }
    if( FD_UNLIKELY( sig==FD_SNAPSHOT_MSG_CTRL_ERROR ) ) return 0;
    if( FD_UNLIKELY( ctx->pending_control!=ULONG_MAX && ctx->control_seen[ lane ] ) ) {
      FD_TEST( sig!=ctx->pending_control );
      return -1;
    }
    if( FD_UNLIKELY( sig==FD_SNAPSHOT_MSG_DATA && lane!=ctx->expected_frame%ctx->lane_cnt ) ) {
      return -1;
    }
    return 0;
  }

  if( FD_UNLIKELY( ctx->io_enabled && ack_worker_idx( ctx, in_idx )!=ULONG_MAX ) ) return 0;

  /* Do not let the next stream phase start until every worker has
     acknowledged the in-flight control (acks arrive on the ack links,
     which stay polled). */
  if( FD_UNLIKELY( ctx->io_enabled && ctx->pending_worker_control!=ULONG_MAX ) ) return -1;

  /* If we're currently in ERROR state we should only process FAIL
     control frags.  Workers were flipped to ERROR-drain by the ABORT
     ring job, so dropping lane frags here cannot wedge them. */
  if( FD_UNLIKELY( ctx->state==FD_SNAPSHOT_STATE_ERROR ) ) {
    return sig!=FD_SNAPSHOT_MSG_CTRL_FAIL;
  }

  if( FD_UNLIKELY( sig==FD_SNAPSHOT_MSG_CTRL_ERROR ) ) {
    return 0;
  }

  /* Once this lane sends the pending control, hold its later frags
     until all snapdc lanes send the same control. */
  if( FD_UNLIKELY( ctx->pending_control!=ULONG_MAX && ctx->control_seen[ in_idx ] ) ) {
    FD_TEST( sig!=ctx->pending_control );
    return -1;
  }

  /* Only accept DATA frags from the expected lane */
  if( FD_UNLIKELY( sig==FD_SNAPSHOT_MSG_DATA && in_idx!=ctx->expected_frame%ctx->lane_cnt ) ) {
    return -1;
  }

  return 0;
}

static inline int
handle_lane_data_frag( fd_snapin_tile_t *  ctx,
                       fd_stem_context_t * stem,
                       ulong               lane,
                       ulong               chunk,
                       ulong               sz,
                       ulong               ctl ) {
  /* EOM marks the end of a frame */
  int eom = !!fd_frag_meta_ctl_eom( ctl );

  if( FD_UNLIKELY( is_accdb_worker( ctx ) ) ) {
    /* Zero-size frags (e.g. the trailing EOM after tar EOF) carry no
       payload; the scan loop is a no-op for them. */
    if( FD_UNLIKELY( sz && worker_handle_data_frag( ctx, lane, chunk, sz, stem ) ) ) {
      return 1;
    }
    if( FD_UNLIKELY( eom ) ) ctx->expected_frame++;
    return 0;
  }

  /* The tar parser can reach EOF before snapdc reports the end of the
     zstd frame.  Only the empty EOM is valid (any payload after EOF is
     malformed). */
  int trailing_eom = ctx->state==FD_SNAPSHOT_STATE_FINISHING && eom && !sz;
  if( FD_UNLIKELY( !trailing_eom && handle_data_frag( ctx, lane, chunk, sz, stem ) ) ) {
    return 1;
  }

  if( FD_UNLIKELY( eom ) ) {
    ctx->expected_frame++;
  }

  return 0;
}

static inline void
handle_control_barrier( fd_snapin_tile_t *  ctx,
                        fd_stem_context_t * stem,
                        ulong               lane,
                        ulong               sig,
                        ulong               chunk,
                        ulong               sz ) {
  /* Error control frags must be immediately handled. */
  if( FD_UNLIKELY( sig==FD_SNAPSHOT_MSG_CTRL_ERROR ) ) {
    if( FD_UNLIKELY( is_accdb_worker( ctx ) ) ) worker_handle_control_frag( ctx, stem, sig );
    else                                        handle_control_frag( ctx, stem, lane, sig, chunk, sz );
    return;
  }

  /* Coordinator: at the first sight of FAIL on any lane, unblock
     workers that may be holding lane frags on byte coverage, so FAIL
     can flow through their lanes too. */
  if( FD_UNLIKELY( !is_accdb_worker( ctx ) && sig==FD_SNAPSHOT_MSG_CTRL_FAIL ) ) publish_abort( ctx, stem );

  if( FD_UNLIKELY( sig!=ctx->pending_control ) ) {
    FD_TEST( ctx->pending_control==ULONG_MAX || sig==FD_SNAPSHOT_MSG_CTRL_FAIL );
    clear_control_barrier( ctx );
    ctx->pending_control = sig;

    /* Bump the attempt generation at the FIRST INIT frag (barrier
       start), not at barrier completion: an ERROR processed while the
       INIT barrier is partially complete aborts the barrier (the
       remaining INIT frags are drained by the ERROR-state filter), but
       every tile is guaranteed to observe at least one INIT frag per
       attempt (INIT is only ever sent after the previous FAIL fully
       flushed, and the aborting ERROR is itself queued behind INIT on
       its own lane).  Bumping here keeps the coordinator's and workers'
       generations in lockstep even across aborted barriers. */
    if( FD_UNLIKELY( ( is_accdb_worker( ctx ) || ctx->io_enabled ) &&
                     ( sig==FD_SNAPSHOT_MSG_CTRL_INIT_FULL || sig==FD_SNAPSHOT_MSG_CTRL_INIT_INCR ) ) ) {
      ctx->generation++;
    }
  }

  /* Only process the control frag when all upstream tiles have sent
     the same control message. */
  FD_TEST( !ctx->control_seen[ lane ] );
  ctx->control_seen[ lane ] = 1U;
  if( FD_LIKELY( !all_controls_seen( ctx ) ) ) {
    return;
  }

  /* All controls received, process the control frag. */
  clear_control_barrier( ctx );
  if( FD_UNLIKELY( is_accdb_worker( ctx ) ) ) worker_handle_control_frag( ctx, stem, sig );
  else                                        handle_control_frag( ctx, stem, lane, sig, chunk, sz );
}

static inline int
returnable_frag( fd_snapin_tile_t *  ctx,
                 ulong               in_idx,
                 ulong               seq    FD_PARAM_UNUSED,
                 ulong               sig,
                 ulong               chunk,
                 ulong               sz,
                 ulong               ctl,
                 ulong               tsorig FD_PARAM_UNUSED,
                 ulong               tspub  FD_PARAM_UNUSED,
                 fd_stem_context_t * stem ) {
  if( FD_UNLIKELY( is_accdb_worker( ctx ) ) ) {
    FD_TEST( ctx->state!=FD_SNAPSHOT_STATE_SHUTDOWN );
    if( FD_UNLIKELY( in_idx==ctx->job_in_idx ) ) {
      return worker_handle_job( ctx, stem, chunk, sz );
    }
    ulong lane = ctx->in_lane[ in_idx ];
    if( FD_UNLIKELY( sig==FD_SNAPSHOT_MSG_DATA ) ) return handle_lane_data_frag( ctx, stem, lane, chunk, sz, ctl );
    handle_control_barrier( ctx, stem, lane, sig, chunk, sz );
    return 0;
  }

  ulong worker_idx = ctx->io_enabled ? ack_worker_idx( ctx, in_idx ) : ULONG_MAX;
  if( FD_UNLIKELY( worker_idx!=ULONG_MAX ) ) {
    return coordinator_handle_ack( ctx, stem, worker_idx, sig, chunk, sz );
  }

  FD_TEST( ctx->state!=FD_SNAPSHOT_STATE_SHUTDOWN );

  int reprocess = 0;
  if( FD_UNLIKELY( sig==FD_SNAPSHOT_MSG_DATA ) ) reprocess = handle_lane_data_frag( ctx, stem, in_idx, chunk, sz, ctl );
  else                                           handle_control_barrier( ctx, stem, in_idx, sig, chunk, sz );

  /* Watermark cadence: broadcast the coverage watermark every
     FD_SNAPIN_IO_WATERMARK_INTERVAL consumed lane frags; the idle
     after_credit path covers assignment-less stretches. */
  if( FD_UNLIKELY( ctx->io_enabled && !reprocess && in_idx<ctx->lane_cnt &&
                   ctx->state==FD_SNAPSHOT_STATE_PROCESSING ) ) {
    ctx->io_frags_since_watermark++;
    if( FD_UNLIKELY( ctx->io_watermark_dirty &&
                     ctx->io_frags_since_watermark>=FD_SNAPIN_IO_WATERMARK_INTERVAL &&
                     ctx->pending_worker_control==ULONG_MAX ) ) {
      publish_watermarks( ctx, stem );
    }
  }

  return reprocess;
}

static inline void
after_credit( fd_snapin_tile_t *  ctx,
              fd_stem_context_t * stem,
              int *               opt_poll_in FD_PARAM_UNUSED,
              int *               charge_busy ) {
  if( FD_UNLIKELY( is_accdb_worker( ctx ) ) ) {
    /* The EOS job may arrive on the ring after the FINI lane barrier
       completed: finish the deferred FINI from here. */
    if( FD_UNLIKELY( ctx->pending_fini ) ) worker_try_complete_fini( ctx, stem );
    return;
  }
  if( FD_LIKELY( !ctx->io_enabled ) ) return;

  /* Idle-deadlock fix: when the coordinator consumed lane frags without
     publishing (manifest body / status cache stretches), the workers
     never see the advanced watermark, keep holding their lane heads,
     and snapdc stalls on their fseqs.  Broadcast the watermark from
     here whenever it advanced but no ring publish went out. */
  int idle = !ctx->io_jobs_since_credit;
  ctx->io_jobs_since_credit = 0UL;
  if( FD_UNLIKELY( ctx->io_watermark_dirty && idle &&
                   ctx->state==FD_SNAPSHOT_STATE_PROCESSING &&
                   ctx->pending_worker_control==ULONG_MAX ) ) {
    publish_watermarks( ctx, stem );
    *charge_busy = 1;
  }
}

static ulong
populate_allowed_fds( fd_topo_t      const * topo FD_PARAM_UNUSED,
                      fd_topo_tile_t const * tile FD_PARAM_UNUSED,
                      ulong                  out_fds_cnt,
                      int *                  out_fds ) {
  if( FD_UNLIKELY( out_fds_cnt<3UL ) ) FD_LOG_ERR(( "invalid out_fds_cnt %lu", out_fds_cnt ));

  ulong out_cnt = 0;
  out_fds[ out_cnt++ ] = 2UL; /* stderr */
  if( FD_LIKELY( -1!=fd_log_private_logfile_fd() ) ) {
    out_fds[ out_cnt++ ] = fd_log_private_logfile_fd(); /* logfile */
  }
  out_fds[ out_cnt++ ] = FD_ACCDB_FD_RW; /* accounts db */

  return out_cnt;
}

static ulong
populate_allowed_seccomp( fd_topo_t const *      topo,
                          fd_topo_tile_t const * tile,
                          ulong                  out_cnt,
                          struct sock_filter *   out ) {
  (void)topo; (void)tile;
  populate_sock_filter_policy_fd_snapin_tile( out_cnt, out, (uint)fd_log_private_logfile_fd(), FD_ACCDB_FD_RW );
  return sock_filter_policy_fd_snapin_tile_instr_cnt;
}

static void
privileged_init( fd_topo_t const *      topo,
                 fd_topo_tile_t const * tile ) {
  fd_snapin_tile_t * ctx = fd_topo_obj_laddr( topo, tile->tile_obj_id );
  memset( ctx, 0, sizeof(fd_snapin_tile_t) );
  FD_TEST( fd_rng_secure( &ctx->seed, 8UL ) );
}

static inline fd_snapin_out_link_t
out1( fd_topo_t const *      topo,
      fd_topo_tile_t const * tile,
      char const *           name,
      ulong                  kind_id ) {
  ulong idx = fd_topo_find_tile_out_link( topo, tile, name, kind_id );

  if( FD_UNLIKELY( idx==ULONG_MAX ) ) return (fd_snapin_out_link_t){ .idx = ULONG_MAX, .mem = NULL, .chunk0 = 0, .wmark = 0, .chunk = 0, .mtu = 0 };

  ulong mtu = topo->links[ tile->out_link_id[ idx ] ].mtu;
  if( FD_UNLIKELY( mtu==0UL ) ) return (fd_snapin_out_link_t){ .idx = idx, .mem = NULL, .chunk0 = ULONG_MAX, .wmark = ULONG_MAX, .chunk = ULONG_MAX, .mtu = mtu };

  void * mem   = topo->workspaces[ topo->objs[ topo->links[ tile->out_link_id[ idx ] ].dcache_obj_id ].wksp_id ].wksp;
  ulong chunk0 = fd_dcache_compact_chunk0( mem, topo->links[ tile->out_link_id[ idx ] ].dcache );
  ulong wmark  = fd_dcache_compact_wmark ( mem, topo->links[ tile->out_link_id[ idx ] ].dcache, mtu );
  return (fd_snapin_out_link_t){ .idx = idx, .mem = mem, .chunk0 = chunk0, .wmark = wmark, .chunk = chunk0, .mtu = mtu };
}

static void
unprivileged_init( fd_topo_t const *      topo,
                   fd_topo_tile_t const * tile ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_snapin_tile_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_snapin_tile_t), sizeof(fd_snapin_tile_t) );

  ctx->role = (int)tile->kind_id;
  if( FD_UNLIKELY( ctx->role<FD_SNAPIN_ROLE_COORDINATOR || (ulong)ctx->role>=FD_SNAPIN_TILE_MAX ) ) {
    FD_LOG_ERR(( "tile `" NAME "` has unsupported kind id %lu", tile->kind_id ));
  }
  ctx->io_enabled      = 0;
  ctx->full            = 1;
  ctx->init_completed  = 0;
  ctx->state           = FD_SNAPSHOT_STATE_IDLE;
  ctx->lane_cnt        = 0UL;
  ctx->worker_idx      = is_accdb_worker( ctx ) ? tile->kind_id-1UL : ULONG_MAX;
  ctx->worker_cnt      = 0UL;
  for( ulong worker_idx=0UL; worker_idx<FD_SNAPIN_WORKER_MAX; worker_idx++ ) ctx->ack_in_idx[ worker_idx ] = ULONG_MAX;
  ctx->generation      = 0UL;
  ctx->pending_worker_control = ULONG_MAX;
  ctx->pending_worker_ack_mask = 0UL;
  ctx->expected_frame  = 0UL;
  clear_control_barrier( ctx );
  fd_memset( &ctx->metrics, 0, sizeof(ctx->metrics) );

  if( FD_UNLIKELY( is_accdb_worker( ctx ) ) ) {
    void * _accdb     = FD_SCRATCH_ALLOC_APPEND( l, fd_accdb_align(),            fd_accdb_footprint( tile->snapin.max_live_slots ) );
    ctx->fifo         = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_snapin_extent_t), FD_SNAPIN_FIFO_CNT*sizeof(fd_snapin_extent_t)     );
    void * _write_buf = FD_SCRATCH_ALLOC_APPEND( l, 4096UL,                      FD_SNAPIN_WRITE_BUF_SZ                            );
    void * _accdb_shmem = fd_topo_obj_laddr( topo, tile->snapin.accdb_obj_id );
    fd_accdb_shmem_t * accdb_shmem = fd_accdb_shmem_join( _accdb_shmem );
    FD_TEST( accdb_shmem );
    ctx->accdb = fd_accdb_join( fd_accdb_new( _accdb, accdb_shmem, FD_ACCDB_FD_RW, 0UL, NULL ) );
    FD_TEST( ctx->accdb );

    ctx->write_buf = _write_buf;

    fd_snapio_snoop_hdr_t * snoop_hdr = fd_snapio_snoop_join( fd_topo_obj_laddr( topo, tile->snapin.snoop_obj_id ) );
    FD_TEST( snoop_hdr );
    FD_TEST( ctx->worker_idx<snoop_hdr->worker_cnt );
    ctx->stripe_locks  = fd_snapio_snoop_stripes( snoop_hdr );
    ctx->my_snoop      = fd_snapio_snoop_worker( snoop_hdr, ctx->worker_idx );
    ctx->stake_log_max = snoop_hdr->stake_log_max;

    ctx->wb_kick_sz = FD_SNAPIN_WB_KICK_SZ;
    /* Write-behind only pays for itself when aggregate pwrite intake can
       outrun the array's multi-stream writeback and trip the kernel's
       dirty-page throttler (measured: collapses at 8 workers, while 4
       workers ride the page cache ~12 s faster without it).  Engage it
       only at high worker counts. */
    ctx->wb_window  = snoop_hdr->worker_cnt>=8UL ? FD_SNAPIN_WB_TOTAL_WINDOW/snoop_hdr->worker_cnt : 0UL;

    /* One snapin_io job ring, plus every snapdc_in lane (full reliable
       consumer, coverage-gated scan). */
    ctx->job_in_idx = ULONG_MAX;
    for( ulong i=0UL; i<tile->in_cnt; i++ ) {
      fd_topo_link_t const * in_link = &topo->links[ tile->in_link_id[ i ] ];
      fd_topo_wksp_t const * in_wksp = &topo->workspaces[ topo->objs[ in_link->dcache_obj_id ].wksp_id ];
      if( FD_UNLIKELY( !strcmp( in_link->name, "snapin_io" ) ) ) {
        FD_TEST( in_link->kind_id==ctx->worker_idx && ctx->job_in_idx==ULONG_MAX );
        ctx->job_in_idx        = i;
        ctx->in_lane[ i ]      = ULONG_MAX;
        ctx->io_in[ 0 ].wksp   = in_wksp->wksp;
        ctx->io_in[ 0 ].chunk0 = fd_dcache_compact_chunk0( ctx->io_in[ 0 ].wksp, in_link->dcache );
        ctx->io_in[ 0 ].wmark  = fd_dcache_compact_wmark( ctx->io_in[ 0 ].wksp, in_link->dcache, in_link->mtu );
        ctx->io_in[ 0 ].mtu    = in_link->mtu;
        FD_TEST( ctx->io_in[ 0 ].mtu==FD_SNAPIN_IO_JOB_SLOT_SZ );
        continue;
      }
      FD_TEST( !strcmp( in_link->name, "snapdc_in" ) );
      ulong lane = in_link->kind_id;
      FD_TEST( lane<FD_SNAPIN_IO_LANE_MAX && !ctx->in[ lane ].wksp );
      ctx->in_lane[ i ]      = lane;
      ctx->in[ lane ].wksp   = in_wksp->wksp;
      ctx->in[ lane ].chunk0 = fd_dcache_compact_chunk0( ctx->in[ lane ].wksp, in_link->dcache );
      ctx->in[ lane ].wmark  = fd_dcache_compact_wmark( ctx->in[ lane ].wksp, in_link->dcache, in_link->mtu );
      ctx->in[ lane ].mtu    = in_link->mtu;
      ctx->in[ lane ].pos    = 0UL;
      ctx->lane_cnt++;
    }
    if( FD_UNLIKELY( ctx->job_in_idx==ULONG_MAX || !ctx->lane_cnt || tile->in_cnt!=ctx->lane_cnt+1UL ) ) {
      FD_LOG_ERR(( "tile `" NAME ":%lu` has %lu ins, expected one snapin_io link plus 1..%lu snapdc_in lanes",
                   tile->kind_id, tile->in_cnt, FD_SNAPIN_IO_LANE_MAX ));
    }
    for( ulong lane=0UL; lane<ctx->lane_cnt; lane++ ) {
      FD_TEST( ctx->in[ lane ].wksp );
    }

    ctx->ack_out = out1( topo, tile, "snapio_ack", ctx->worker_idx );
    if( FD_UNLIKELY( ctx->ack_out.idx==ULONG_MAX || ctx->ack_out.mtu!=FD_SNAPIN_IO_ACK_SLOT_SZ ) ) {
      FD_LOG_ERR(( "tile `" NAME ":%lu` missing valid snapio_ack output", tile->kind_id ));
    }

    worker_reset_attempt( ctx );
    ctx->boot_timestamp = fd_log_wallclock();
    return;
  }

  void * _txncache        = FD_SCRATCH_ALLOC_APPEND( l, fd_txncache_align(),           fd_txncache_footprint( tile->snapin.max_live_slots )        );
  void * _accdb           = FD_SCRATCH_ALLOC_APPEND( l, fd_accdb_align(),              fd_accdb_footprint( tile->snapin.max_live_slots )           );
  void * _manifest_parser = FD_SCRATCH_ALLOC_APPEND( l, fd_ssmanifest_parser_align(),  fd_ssmanifest_parser_footprint()                            );
  void * _sd_parser       = FD_SCRATCH_ALLOC_APPEND( l, fd_slot_delta_parser_align(),  fd_slot_delta_parser_footprint()                            );
  ctx->blockhash_groups   = FD_SCRATCH_ALLOC_APPEND( l, alignof(blockhash_group_t),    sizeof(blockhash_group_t)*FD_SNAPIN_MAX_SLOT_DELTA_GROUPS   );
  ctx->txncache_entries   = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_sstxncache_hash_t), sizeof(fd_sstxncache_hash_t)*FD_SNAPIN_TXNCACHE_MAX_ENTRIES );

  void * _accdb_shmem = fd_topo_obj_laddr( topo, tile->snapin.accdb_obj_id );
  fd_accdb_shmem_t * accdb_shmem = fd_accdb_shmem_join( _accdb_shmem );
  FD_TEST( accdb_shmem );
  ctx->accdb = fd_accdb_join( fd_accdb_new( _accdb, accdb_shmem, FD_ACCDB_FD_RW, 0UL, NULL ) );
  FD_TEST( ctx->accdb );

  void * _txncache_shmem = fd_topo_obj_laddr( topo, tile->snapin.txncache_obj_id );
  fd_txncache_shmem_t * txncache_shmem = fd_txncache_shmem_join( _txncache_shmem );
  FD_TEST( txncache_shmem );
  ctx->txncache = fd_txncache_join( fd_txncache_new( _txncache, txncache_shmem ) );
  FD_TEST( ctx->txncache );

  ctx->alpenglow = tile->snapin.alpenglow;

  ctx->banks = fd_banks_join( fd_topo_obj_laddr( topo, tile->snapin.banks_obj_id ) );
  FD_TEST( ctx->banks );
  ctx->bank = fd_banks_init_bank( ctx->banks );
  FD_TEST( ctx->bank );
  FD_TEST( ctx->bank->idx==0UL );

  ctx->blockhash_groups_len = 0UL;

  ctx->manifest_parser = fd_ssmanifest_parser_join( fd_ssmanifest_parser_new( _manifest_parser ) );
  FD_TEST( ctx->manifest_parser );

  ctx->slot_delta_parser = fd_slot_delta_parser_join( fd_slot_delta_parser_new( _sd_parser ) );
  FD_TEST( ctx->slot_delta_parser );

  ctx->ct_out       = out1( topo, tile, "snapin_ct",    0UL );
  ctx->manifest_out = out1( topo, tile, "snapin_manif", 0UL );
  ctx->gui_out      = out1( topo, tile, "snapin_gui",   0UL );
  for( ulong worker_idx=0UL; worker_idx<FD_SNAPIN_WORKER_MAX; worker_idx++ ) {
    fd_snapin_out_link_t out = out1( topo, tile, "snapin_io", worker_idx );
    if( out.idx==ULONG_MAX ) break;
    FD_TEST( out.mtu==FD_SNAPIN_IO_JOB_SLOT_SZ );
    ctx->io_out[ worker_idx ] = out;
    ctx->worker_cnt++;
  }
  if( FD_UNLIKELY( ctx->worker_cnt ) ) {
    FD_TEST( fd_ulong_is_pow2( ctx->worker_cnt ) );
    ctx->io_enabled = 1;

    fd_snapio_snoop_hdr_t * snoop_hdr = fd_snapio_snoop_join( fd_topo_obj_laddr( topo, tile->snapin.snoop_obj_id ) );
    FD_TEST( snoop_hdr );
    FD_TEST( snoop_hdr->worker_cnt==ctx->worker_cnt );
    ctx->stripe_locks = fd_snapio_snoop_stripes( snoop_hdr );
    for( ulong w=0UL; w<ctx->worker_cnt; w++ ) ctx->snoops[ w ] = fd_snapio_snoop_worker( snoop_hdr, w );
  }

  if( FD_UNLIKELY( ctx->ct_out.idx==ULONG_MAX ) ) FD_LOG_ERR(( "tile `" NAME "` missing required out link `snapin_ct`" ));
  if( FD_UNLIKELY( ctx->manifest_out.idx==ULONG_MAX ) ) FD_LOG_ERR(( "tile `" NAME "` missing required out link `snapin_manif`" ));

  fd_ssparse_init( ctx->ssparse );
  fd_ssparse_appendvec_passthrough_enable( ctx->ssparse, ctx->io_enabled );
  fd_ssmanifest_parser_init( ctx->manifest_parser, fd_chunk_to_laddr( ctx->manifest_out.mem, ctx->manifest_out.chunk ) );
  fd_slot_delta_parser_init( ctx->slot_delta_parser );

  for( ulong i=0UL; i<tile->in_cnt; i++ ) {
    fd_topo_link_t const * in_link = &topo->links[ tile->in_link_id[ i ] ];
    if( FD_UNLIKELY( !strcmp( in_link->name, "snapio_ack" ) ) ) {
      ulong worker_idx = in_link->kind_id;
      FD_TEST( ctx->io_enabled && worker_idx<ctx->worker_cnt && ctx->ack_in_idx[ worker_idx ]==ULONG_MAX );
      ctx->ack_in_idx[ worker_idx ] = i;
      fd_topo_wksp_t const * in_wksp = &topo->workspaces[ topo->objs[ in_link->dcache_obj_id ].wksp_id ];
      ctx->io_in[ worker_idx ].wksp   = in_wksp->wksp;
      ctx->io_in[ worker_idx ].chunk0 = fd_dcache_compact_chunk0( ctx->io_in[ worker_idx ].wksp, in_link->dcache );
      ctx->io_in[ worker_idx ].wmark  = fd_dcache_compact_wmark( ctx->io_in[ worker_idx ].wksp, in_link->dcache, in_link->mtu );
      ctx->io_in[ worker_idx ].mtu    = in_link->mtu;
      FD_TEST( ctx->io_in[ worker_idx ].mtu==FD_SNAPIN_IO_ACK_SLOT_SZ );
      continue;
    }

    ulong lane = ctx->lane_cnt++;
    FD_TEST( !strcmp( in_link->name, "snapdc_in" ) );
    FD_TEST( in_link->kind_id==lane && lane<FD_TOPO_MAX_TILE_IN_LINKS );
    fd_topo_wksp_t const * in_wksp = &topo->workspaces[ topo->objs[ in_link->dcache_obj_id ].wksp_id ];
    ctx->in[ lane ].wksp   = in_wksp->wksp;
    ctx->in[ lane ].chunk0 = fd_dcache_compact_chunk0( ctx->in[ lane ].wksp, in_link->dcache );
    ctx->in[ lane ].wmark  = fd_dcache_compact_wmark( ctx->in[ lane ].wksp, in_link->dcache, in_link->mtu );
    ctx->in[ lane ].mtu    = in_link->mtu;
    ctx->in[ lane ].pos    = 0UL;
  }

  if( FD_UNLIKELY( !ctx->lane_cnt || ctx->lane_cnt>FD_TOPO_MAX_TILE_IN_LINKS ) ) {
    FD_LOG_ERR(( "tile `" NAME "` has %lu snapshot data lanes, expected 1..%lu", ctx->lane_cnt, FD_TOPO_MAX_TILE_IN_LINKS ));
  }
  if( FD_UNLIKELY( ctx->io_enabled ) ) {
    for( ulong worker_idx=0UL; worker_idx<ctx->worker_cnt; worker_idx++ ) FD_TEST( ctx->ack_in_idx[ worker_idx ]!=ULONG_MAX );
    FD_TEST( tile->in_cnt==ctx->lane_cnt+ctx->worker_cnt );
    FD_TEST( ctx->lane_cnt<=FD_SNAPIN_IO_LANE_MAX );
  } else {
    FD_TEST( tile->in_cnt==ctx->lane_cnt );
  }

  ctx->stream_cursor            = 0UL;
  ctx->appendvec_seq            = 0UL;
  ctx->covered_until            = 0UL;
  ctx->io_watermark_dirty       = 0;
  ctx->io_frags_since_watermark = 0UL;
  ctx->io_jobs_since_credit     = 0UL;
  ctx->abort_published          = 0;
  ctx->snoop_merged             = 0;
  fd_memset( ctx->worker_assigned_bytes, 0, sizeof(ctx->worker_assigned_bytes) );
  fd_memset( &ctx->worker_fold, 0, sizeof(ctx->worker_fold) );
  fd_memset( &ctx->av_stats,    0, sizeof(ctx->av_stats)    );

  ctx->gui_config_acct_sz  = 0UL;
  ctx->gui_config_acct_off = 0UL;

  ctx->advertised_slot = 0UL;
  ctx->bank_slot       = 0UL;
  ctx->epoch           = 0UL;

  ctx->full_genesis_creation_time_seconds = 0UL;
  ctx->manifest_capitalization            = 0UL;
  ctx->capitalization                     = 0UL;
  ctx->dup_capitalization                 = 0UL;
  ctx->recovery.capitalization = 0UL;
  memset( &ctx->recovery.accdb_metadata, 0, sizeof(ctx->recovery.accdb_metadata) );

  ctx->accdb_root_fork_id = (fd_accdb_fork_id_t){ .val = USHORT_MAX };
  ctx->accdb_incr_fork_id = (fd_accdb_fork_id_t){ .val = USHORT_MAX };

  fd_memset( &ctx->flags, 0, sizeof(ctx->flags) );
  ctx->boot_timestamp = fd_log_wallclock();
}

/* A full input fragment can contain at most 61 eight-account jobs. */
#define STEM_BURST FD_SNAPIN_IO_BURST

/* Account-batch jobs arrive at roughly two million messages per second.
   Refresh flow-control credits before a burst drains so the coordinator
   and accdb worker do not alternate between backpressure and empty polls. */
#define STEM_LAZY  (128L*250L)

#define STEM_CALLBACK_CONTEXT_TYPE  fd_snapin_tile_t
#define STEM_CALLBACK_CONTEXT_ALIGN alignof(fd_snapin_tile_t)

#define STEM_CALLBACK_SHOULD_SHUTDOWN should_shutdown
#define STEM_CALLBACK_METRICS_WRITE   metrics_write
#define STEM_CALLBACK_BEFORE_FRAG     before_frag
#define STEM_CALLBACK_RETURNABLE_FRAG returnable_frag
#define STEM_CALLBACK_AFTER_CREDIT    after_credit

#include "../../disco/stem/fd_stem.c"

static ulong
max_event_sz( fd_topo_tile_t const * tile FD_PARAM_UNUSED ) {
  return sizeof(fd_event_accdb_partition_added_t);
}

fd_topo_run_tile_t fd_tile_snapin = {
  .name                     = NAME,
  .populate_allowed_fds     = populate_allowed_fds,
  .populate_allowed_seccomp = populate_allowed_seccomp,
  .scratch_align            = scratch_align,
  .scratch_footprint        = scratch_footprint,
  .privileged_init          = privileged_init,
  .unprivileged_init        = unprivileged_init,
  .max_event_sz             = max_event_sz,
  .run                      = stem_run,
};

#undef NAME
