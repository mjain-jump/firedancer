#ifndef HEADER_fd_src_discof_restore_utils_fd_snapin_shared_h
#define HEADER_fd_src_discof_restore_utils_fd_snapin_shared_h

/* Shared coordination state for parallel snapshot loaders. */

#include "fd_ssctrl.h"
#include "../../../flamenco/features/fd_feature_snoop.h"
#include "../../../flamenco/runtime/sysvar/fd_sysvar_base.h"

#define FD_SNAPIN_LANE_MAX (16UL)

/* 4096 stripes kept lock contention below 0.4% with 8 workers. */
#define FD_SNAPIN_SHARED_STRIPE_CNT (1UL<<12)
#define FD_SNAPIN_SHARED_STRIPE_MSK (FD_SNAPIN_SHARED_STRIPE_CNT-1UL)

/* One entry per partition in the current accdb topologies. */
#define FD_SNAPIN_SHARED_PARTITION_MAX (8192UL)

/* Workers add their attempt totals before ACKing FINI. */
struct __attribute__((aligned(64))) fd_snapin_shared_totals {
  ulong accounts_loaded;
  ulong accounts_replaced;
  ulong accounts_ignored;
  ulong input_lamports;
  ulong replaced_lamports;
  ulong ignored_lamports;
  ulong eq_slot_dups;
  ulong appendvecs_processed;
};

typedef struct fd_snapin_shared_totals fd_snapin_shared_totals_t;

/* Partitions owned by one failed worker.  Tile 0 releases them during
   the next INIT. */
struct __attribute__((aligned(64))) fd_snapin_shared_worker {
  ulong fail_partition_cnt;
  uint  fail_partitions[ FD_SNAPIN_SHARED_PARTITION_MAX ];
};

typedef struct fd_snapin_shared_worker fd_snapin_shared_worker_t;

/* Tile 0 publishes the attempt after setup.  Workers hold data until
   the generation matches, then claim appendvecs from next_appendvec. */
struct fd_snapin_shared {
  ulong magic;
  ulong worker_cnt;

  struct __attribute__((aligned(64))) {
    ulong generation;
    ulong fork_id;
  } attempt;

  /* Isolate the hot claim counter on its own cache line. */
  ulong next_appendvec __attribute__((aligned(128)));

  fd_snapin_shared_totals_t totals;

  /* Winner callbacks update these captures while holding the account
     stripe. */
  struct __attribute__((aligned(64))) {
    int   captured;
    int   executable;
    ulong slot;
    ulong lamports;
    ulong data_len;
    uchar owner[ 32UL ];
    uchar buf[ FD_SYSVAR_SLOT_HISTORY_BINCODE_SZ ];
  } slot_history;

  fd_feature_snoop_t feature_snoop;
};

typedef struct fd_snapin_shared fd_snapin_shared_t;

#define FD_SNAPIN_SHARED_MAGIC (0xF17EDA2C0501A910UL)

static inline ulong
fd_snapin_shared_align( void ) {
  return 4096UL;
}

static inline ulong
fd_snapin_shared_footprint( ulong worker_cnt ) {
  return fd_ulong_align_up( sizeof(fd_snapin_shared_t), 4096UL )
       + fd_ulong_align_up( FD_SNAPIN_SHARED_STRIPE_CNT*sizeof(int), 4096UL )
       + worker_cnt*sizeof(fd_snapin_shared_worker_t);
}

static inline void *
fd_snapin_shared_new( void * mem,
                      ulong  worker_cnt ) {
  fd_snapin_shared_t * shared = (fd_snapin_shared_t *)mem;
  shared->worker_cnt = worker_cnt;
  /* Workspace memory may survive a crash, so clear stale shared state. */
  shared->attempt.generation = 0UL;
  shared->attempt.fork_id    = 0UL;
  shared->next_appendvec     = 0UL;
  fd_memset( &shared->totals,        0, sizeof(fd_snapin_shared_totals_t) );
  fd_memset( &shared->slot_history,  0, sizeof(shared->slot_history)      );
  fd_memset( &shared->feature_snoop, 0, sizeof(fd_feature_snoop_t)        );
  uchar * stripes = (uchar *)shared + fd_ulong_align_up( sizeof(fd_snapin_shared_t), 4096UL );
  fd_memset( stripes, 0, FD_SNAPIN_SHARED_STRIPE_CNT*sizeof(int) );
  uchar * workers = stripes + fd_ulong_align_up( FD_SNAPIN_SHARED_STRIPE_CNT*sizeof(int), 4096UL );
  for( ulong w=0UL; w<worker_cnt; w++ ) {
    fd_memset( workers + w*sizeof(fd_snapin_shared_worker_t), 0, sizeof(fd_snapin_shared_worker_t) );
  }
  FD_COMPILER_MFENCE();
  shared->magic = FD_SNAPIN_SHARED_MAGIC;
  return mem;
}

static inline fd_snapin_shared_t *
fd_snapin_shared_join( void * mem ) {
  fd_snapin_shared_t * shared = (fd_snapin_shared_t *)mem;
  if( FD_UNLIKELY( shared->magic!=FD_SNAPIN_SHARED_MAGIC ) ) return NULL;
  return shared;
}

static inline int *
fd_snapin_shared_stripes( fd_snapin_shared_t * shared ) {
  return (int *)( (uchar *)shared + fd_ulong_align_up( sizeof(fd_snapin_shared_t), 4096UL ) );
}

static inline fd_snapin_shared_worker_t *
fd_snapin_shared_worker( fd_snapin_shared_t * shared,
                         ulong                worker_idx ) {
  uchar * base = (uchar *)fd_snapin_shared_stripes( shared ) + fd_ulong_align_up( FD_SNAPIN_SHARED_STRIPE_CNT*sizeof(int), 4096UL );
  return (fd_snapin_shared_worker_t *)( base + worker_idx*sizeof(fd_snapin_shared_worker_t) );
}

#endif /* HEADER_fd_src_discof_restore_utils_fd_snapin_shared_h */
