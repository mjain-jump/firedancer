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

#define FD_SNAPIN_IO_KIND_BATCH (0UL)
#define FD_SNAPIN_IO_KIND_CTRL  (1UL)

struct __attribute__((aligned(64))) fd_snapin_io_job {
  ulong kind;
  ulong worker_idx;
  ulong generation;
  ulong control;
  ulong cnt;
  ulong slot;
  ulong fork_id;
  uchar pubkeys    [ 8 ][ 32 ];
  uint  chain_idx_lo [ 8 ];
  uchar chain_idx_hi [ 8 ];
  ulong lamports   [ 8 ];
  ulong data_lens  [ 8 ];
  ulong file_offsets[ 8 ];
  int   executables[ 8 ];
};

typedef struct fd_snapin_io_job fd_snapin_io_job_t;

/* accdb chain indices need at most 34 bits because max_accounts is bounded
   below UINT_MAX and chain_cnt is pow2_up(4*max_accounts).  Keep the indices
   packed to 40 bits on the coordinator-to-worker link so adding precomputed
   hashes does not grow the 576-byte job by another cache line. */

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

FD_STATIC_ASSERT( sizeof(fd_snapin_io_job_t)<=FD_SNAPIN_IO_JOB_SLOT_SZ, snapin_io_job_fits    );
FD_STATIC_ASSERT( sizeof(fd_snapin_io_job_t)==576UL,                    snapin_io_job_compact );
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
