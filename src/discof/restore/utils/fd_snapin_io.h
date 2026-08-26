#ifndef HEADER_fd_src_discof_restore_utils_fd_snapin_io_h
#define HEADER_fd_src_discof_restore_utils_fd_snapin_io_h

/* fd_snapin_io defines the coordination protocol between the snapshot
   coordinator (snapin kind 0) and the fused parse+insert+write accdb
   workers (snapin kind 1..N) in the tar-boundary-sharded snapshot-load
   topology:

     - per-worker "snapin_io" job rings carrying whole-appendvec
       assignments (published as soon as the 512 byte tar header
       parses), stream-offset coverage watermarks, and EOS/ABORT control
       markers.  Data controls (INIT/FINI/...) do NOT ride the ring:
       workers are full lane consumers and take them from their own lane
       barriers;
     - per-worker "snapio_ack" links carrying control acknowledgements
       and end-of-load counters back to the coordinator;
     - a shared-memory staging object holding the striped accdb chain
       locks and the per-worker snoop staging (slot history sysvar,
       feature gate accounts, stake delegations log) that the
       coordinator merges at end of load. */

#include "fd_ssctrl.h"
#include "../../../flamenco/features/fd_feature_snoop.h"
#include "../../../flamenco/runtime/sysvar/fd_sysvar_base.h"

/* Ring parameters.  Jobs are per tar entry (~one per 2,600 accounts)
   plus periodic watermarks; depth 2048 spans the flow-control credit
   refresh interval with lots of slack. */

#define FD_SNAPIN_IO_DEPTH       (2048UL)
#define FD_SNAPIN_IO_BURST       (64UL)
#define FD_SNAPIN_IO_JOB_SLOT_SZ (128UL)
#define FD_SNAPIN_IO_ACK_SLOT_SZ (128UL)

/* Maximum number of snapdc lanes a snapin coordinator/worker tracks. */

#define FD_SNAPIN_IO_LANE_MAX    (16UL)

/* Coordinator publishes a WATERMARK to every worker ring after this
   many consumed data frags (in addition to the idle after_credit path
   and the inline covered_until on every ASSIGN). */

#define FD_SNAPIN_IO_WATERMARK_INTERVAL (64UL)

/* Job kinds (also used as the frag sig on the job ring) */

#define FD_SNAPIN_IO_KIND_ASSIGN    (0UL) /* whole appendvec owned by this worker */
#define FD_SNAPIN_IO_KIND_WATERMARK (1UL) /* assignments complete below covered_until */
#define FD_SNAPIN_IO_KIND_EOS       (2UL) /* tar end of stream: coverage extends to infinity */
#define FD_SNAPIN_IO_KIND_ABORT     (3UL) /* attempt aborted: drain data until FAIL */

/* fd_snapin_io_job_t is one slot of a worker's job ring.  For ASSIGN,
   [body_off, body_off+body_sz) is the appendvec body in
   decompressed-stream byte offsets; byte coverage extends through
   covered_until = body_off + align512(body_sz) (tar entries are 512
   aligned).  The ASSIGN-before-watermark invariant: an ASSIGN is
   published on the owning worker's ring before any watermark whose
   covered_until exceeds its body_off, so ring FIFO order guarantees the
   assignment is in the worker's queue by the time its scan cursor
   reaches the body.  EOS/ABORT carry only generation. */

struct __attribute__((aligned(64))) fd_snapin_io_job {
  ulong kind;
  ulong generation;
  ulong appendvec_idx; /* ASSIGN: stream sequence number (snoop precedence) */
  ulong slot;          /* ASSIGN */
  ulong body_off;      /* ASSIGN */
  ulong body_sz;       /* ASSIGN */
  ulong covered_until; /* ASSIGN / WATERMARK */
};

typedef struct fd_snapin_io_job fd_snapin_io_job_t;

/* fd_snapin_io_ack_t is one slot of a worker's ack link.  Counters are
   only meaningful on the FINI ack (cumulative for the attempt); err is
   only meaningful on an (unsolicited) FD_SNAPSHOT_MSG_CTRL_ERROR ack. */

struct __attribute__((aligned(64))) fd_snapin_io_ack {
  ulong worker_idx;
  ulong generation;
  ulong control;
  ulong accounts_ignored;
  ulong accounts_replaced;
  ulong accounts_loaded;
  ulong input_lamports;
  ulong replaced_lamports;
  ulong ignored_lamports;
  ulong bytes_written;
  ulong eq_slot_dups;
  ulong eq_slot_lamports_diff;
  int   err;
  int   _pad;
};

typedef struct fd_snapin_io_ack fd_snapin_io_ack_t;

FD_STATIC_ASSERT( sizeof(fd_snapin_io_job_t)<=FD_SNAPIN_IO_JOB_SLOT_SZ, snapin_io_job_fits );
FD_STATIC_ASSERT( sizeof(fd_snapin_io_ack_t)<=FD_SNAPIN_IO_ACK_SLOT_SZ, snapin_io_ack_fits );

static inline ulong
fd_snapin_io_ack_sig( ulong generation,
                      ulong control ) {
  return (generation<<8) | (control & 255UL);
}

static inline ulong
fd_snapin_io_ack_generation( ulong sig ) {
  return sig>>8;
}

static inline ulong
fd_snapin_io_ack_control( ulong sig ) {
  return sig & 255UL;
}

/* Shared staging object ("snapio_snoop" topo obj) ******************/

/* Striped spin locks serializing accdb hash-chain access across the
   parallel writers.  4 MiB. */

#define FD_SNAPIO_STRIPE_CNT (1UL<<20)
#define FD_SNAPIO_STRIPE_MSK (FD_SNAPIO_STRIPE_CNT-1UL)

#define FD_SNAPIO_WORKER_MAX (8UL)

/* One record of a worker's stake-delegation snoop log.  Entries are
   appended in worker-stream order, so each per-worker log is sorted by
   (appendvec_idx, record_idx); the coordinator k-way merges the logs at
   end of load to reproduce the sequential loader's stream-order
   fd_stake_delegations_root_update sequence exactly. */

struct fd_snapio_stake_ent {
  uchar pubkey[ 32UL ];
  uchar voter [ 32UL ];
  ulong stake;
  ulong activation_epoch;
  ulong deactivation_epoch;
  ulong credits_observed;
  ulong lamports;
  ulong appendvec_idx;
  ulong record_idx;
  uint  data_len;
  uint  _pad;
};

typedef struct fd_snapio_stake_ent fd_snapio_stake_ent_t;

FD_STATIC_ASSERT( sizeof(fd_snapio_stake_ent_t)==128UL, snapio_stake_ent_sz );

/* Per-worker snoop staging.  The stake log (stake_log_max entries)
   immediately follows the struct.  Written lock-free by the owning
   worker only; read by the coordinator after the worker's FINI ack. */

struct fd_snapio_worker_snoop {
  struct {
    int   captured;
    int   executable;
    ulong slot;
    ulong appendvec_idx;
    ulong record_idx;
    ulong lamports;
    ulong data_len;
    uchar owner[ 32UL ];
    uchar buf[ FD_SYSVAR_SLOT_HISTORY_BINCODE_SZ ];
  } slot_history;

  fd_feature_snoop_t feature_snoop;
  ulong feature_av [ FD_FEATURE_SNOOP_CNT ]; /* stream position of last observation per id */
  ulong feature_rec[ FD_FEATURE_SNOOP_CNT ];

  ulong stake_cnt;
  /* fd_snapio_stake_ent_t stake_log[ stake_log_max ] follows */
};

typedef struct fd_snapio_worker_snoop fd_snapio_worker_snoop_t;

struct fd_snapio_snoop_hdr {
  ulong magic;
  ulong worker_cnt;
  ulong stake_log_max;
};

typedef struct fd_snapio_snoop_hdr fd_snapio_snoop_hdr_t;

#define FD_SNAPIO_SNOOP_MAGIC (0xF17EDA2C0501A910UL)

/* Default per-worker stake log capacity.  Mainnet has ~1.5M stake
   accounts; 2M entries per worker tolerates heavy skew plus duplicates.
   128 B/entry -> 256 MiB per worker (2 GiB at 8 workers). */
#define FD_SNAPIO_STAKE_LOG_MAX (1UL<<21)

static inline ulong
fd_snapio_snoop_align( void ) {
  return 4096UL;
}

static inline ulong
fd_snapio_worker_snoop_footprint( ulong stake_log_max ) {
  return fd_ulong_align_up( sizeof(fd_snapio_worker_snoop_t) + stake_log_max*sizeof(fd_snapio_stake_ent_t), 4096UL );
}

static inline ulong
fd_snapio_snoop_footprint( ulong worker_cnt,
                           ulong stake_log_max ) {
  return fd_ulong_align_up( sizeof(fd_snapio_snoop_hdr_t), 4096UL )
       + fd_ulong_align_up( FD_SNAPIO_STRIPE_CNT*sizeof(uint), 4096UL )
       + worker_cnt*fd_snapio_worker_snoop_footprint( stake_log_max );
}

static inline void *
fd_snapio_snoop_new( void * mem,
                     ulong  worker_cnt,
                     ulong  stake_log_max ) {
  fd_snapio_snoop_hdr_t * hdr = (fd_snapio_snoop_hdr_t *)mem;
  hdr->worker_cnt    = worker_cnt;
  hdr->stake_log_max = stake_log_max;
  /* stripe locks and per-worker regions rely on fresh shmem being
     zeroed; workers additionally reset their own regions on INIT */
  FD_COMPILER_MFENCE();
  hdr->magic = FD_SNAPIO_SNOOP_MAGIC;
  return mem;
}

static inline fd_snapio_snoop_hdr_t *
fd_snapio_snoop_join( void * mem ) {
  fd_snapio_snoop_hdr_t * hdr = (fd_snapio_snoop_hdr_t *)mem;
  if( FD_UNLIKELY( hdr->magic!=FD_SNAPIO_SNOOP_MAGIC ) ) return NULL;
  return hdr;
}

static inline uint *
fd_snapio_snoop_stripes( fd_snapio_snoop_hdr_t * hdr ) {
  return (uint *)( (uchar *)hdr + fd_ulong_align_up( sizeof(fd_snapio_snoop_hdr_t), 4096UL ) );
}

static inline fd_snapio_worker_snoop_t *
fd_snapio_snoop_worker( fd_snapio_snoop_hdr_t * hdr,
                        ulong                   worker_idx ) {
  uchar * base = (uchar *)fd_snapio_snoop_stripes( hdr ) + fd_ulong_align_up( FD_SNAPIO_STRIPE_CNT*sizeof(uint), 4096UL );
  return (fd_snapio_worker_snoop_t *)( base + worker_idx*fd_snapio_worker_snoop_footprint( hdr->stake_log_max ) );
}

static inline fd_snapio_stake_ent_t *
fd_snapio_worker_stake_log( fd_snapio_worker_snoop_t * ws ) {
  return (fd_snapio_stake_ent_t *)( (uchar *)ws + sizeof(fd_snapio_worker_snoop_t) );
}

#endif /* HEADER_fd_src_discof_restore_utils_fd_snapin_io_h */
