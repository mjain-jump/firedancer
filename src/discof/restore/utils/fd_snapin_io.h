#ifndef HEADER_fd_src_discof_restore_utils_fd_snapin_io_h
#define HEADER_fd_src_discof_restore_utils_fd_snapin_io_h

#include "fd_ssctrl.h"

/* One parser callback can emit at most 61 full eight-account batches from
   a 65408-byte snapshot fragment.  The ring also has to span the stem's
   flow-control credit refresh interval: at mainnet insertion rates, a
   depth-128 ring drains long before the default ~384 us housekeeping
   cadence and artificially caps throughput.  Depth 2048 costs only 2 MiB
   and leaves enough runway between credit refreshes. */

#define FD_SNAPIN_IO_DEPTH       (2048UL)
#define FD_SNAPIN_IO_BURST       (64UL)
#define FD_SNAPIN_IO_JOB_SLOT_SZ (1024UL)
#define FD_SNAPIN_IO_ACK_SLOT_SZ (128UL)

/* Maximum number of snapdc lanes a snapin coordinator/worker tracks.
   Bounds the per-lane frontier array carried in FRONTIER/CTRL jobs. */

#define FD_SNAPIN_IO_LANE_MAX    (16UL)

/* Coordinator emits a FRONTIER job to every worker ring after this many
   consumed data frags (in addition to the idle after_credit path and
   every CTRL job). */

#define FD_SNAPIN_IO_FRONTIER_INTERVAL (64UL)

#define FD_SNAPIN_IO_KIND_BATCH    (0UL)
#define FD_SNAPIN_IO_KIND_CTRL     (1UL)
#define FD_SNAPIN_IO_KIND_HEADER   (2UL)
#define FD_SNAPIN_IO_KIND_DATA     (3UL)
#define FD_SNAPIN_IO_KIND_FRONTIER (4UL)

/* fd_ssparse account batch entry layout (fd_ssparse.c): each entry is a
   136-byte header followed by the account data, unfragmented within one
   decompressed frag.  Workers read account fields directly from the
   held frag bytes via these offsets. */

#define FD_SNAPIN_IO_ENT_DATA_LEN_OFF  (8UL)
#define FD_SNAPIN_IO_ENT_PUBKEY_OFF   (16UL)
#define FD_SNAPIN_IO_ENT_LAMPORTS_OFF (48UL)
#define FD_SNAPIN_IO_ENT_OWNER_OFF    (64UL)
#define FD_SNAPIN_IO_ENT_EXEC_OFF     (96UL)
#define FD_SNAPIN_IO_ENT_DATA_OFF    (136UL)

/* Jobs carry (lane,seq,chunk,off) references into the snapdc_in dcache
   instead of by-value account fields.  This is safe because every
   worker is a reliable consumer of every snapdc_in lane and defers
   (holds) any lane frag newer than its released frontier: the producer
   cannot recycle referenced bytes until the worker has processed every
   ring job that precedes the FRONTIER/CTRL job releasing them, and the
   coordinator only snapshots the frontier at frag boundaries, after all
   pending jobs pinned to a consumed frag have been published. */

struct __attribute__((aligned(64))) fd_snapin_io_job {
  ulong kind;
  ulong worker_idx;
  ulong generation;
  ulong control;      /* CTRL only */
  ulong cnt;          /* BATCH: number of accounts in this job */
  ulong slot;         /* BATCH / HEADER */
  ulong fork_id;      /* BATCH / HEADER */

  /* Held-frag reference (BATCH / DATA).  All BATCH entries live in one
     decompressed frag; DATA references one contiguous byte run. */
  ulong lane;
  ulong seq;
  ulong chunk;
  uint  off;          /* DATA: run start within the frag */
  uint  sz;           /* DATA: run length in bytes */

  /* BATCH: per-account entry offsets within the referenced frag. */
  uint  ent_off     [ 8 ];
  uint  data_len    [ 8 ];
  uint  chain_idx_lo[ 8 ];
  uchar chain_idx_hi[ 8 ];

  /* HEADER: slow-path account.  The account header may straddle input
     frags (the parser reassembles it in private memory), so the decoded
     fields travel by value; the data bytes follow as DATA refs. */
  struct {
    uchar pubkey[ 32 ];
    uchar owner [ 32 ];
    ulong lamports;
    ulong data_len;
    ulong chain_idx;
    int   executable;
  } hdr;

  /* FRONTIER / CTRL: per-lane last-consumed frag seq on the coordinator
     (ULONG_MAX means nothing consumed yet).  Workers may consume (and
     thus release) lane frags up to and including these seqs.  Only
     snapshotted at frag boundaries. */
  ulong frontier[ FD_SNAPIN_IO_LANE_MAX ];
};

typedef struct fd_snapin_io_job fd_snapin_io_job_t;

/* accdb chain indices need at most 34 bits because max_accounts is bounded
   below UINT_MAX and chain_cnt is pow2_up(4*max_accounts).  Keep the indices
   packed to 40 bits on the coordinator-to-worker link. */

static inline void
fd_snapin_io_job_set_chain_idx( fd_snapin_io_job_t * job,
                                ulong                idx,
                                ulong                chain_idx ) {
  job->chain_idx_lo[ idx ] = (uint)chain_idx;
  job->chain_idx_hi[ idx ] = (uchar)(chain_idx>>32);
}

static inline ulong
fd_snapin_io_job_chain_idx( fd_snapin_io_job_t const * job,
                            ulong                      idx ) {
  return (ulong)job->chain_idx_lo[ idx ] | ((ulong)job->chain_idx_hi[ idx ]<<32);
}

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
  int   err;
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

#endif /* HEADER_fd_src_discof_restore_utils_fd_snapin_io_h */
