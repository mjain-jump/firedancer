#ifndef HEADER_fd_src_discof_restore_utils_fd_snapin_shmem_h
#define HEADER_fd_src_discof_restore_utils_fd_snapin_shmem_h

/* Shared memory state for parallel snapshot loaders. */

#include "../../../flamenco/features/fd_feature_snoop.h"
#include "../../../flamenco/runtime/sysvar/fd_sysvar_base.h"

/* 4096 stripes kept lock contention below 0.4% with 8 workers. */
#define FD_SNAPIN_SHMEM_STRIPE_CNT (1UL<<12)
#define FD_SNAPIN_SHMEM_STRIPE_MSK (FD_SNAPIN_SHMEM_STRIPE_CNT-1UL)

/* Workers add capitalization totals before ACKing FINI. */
struct fd_snapin_shmem_totals {
  ulong input_lamports;
  ulong replaced_lamports;
  ulong ignored_lamports;
};

typedef struct fd_snapin_shmem_totals fd_snapin_shmem_totals_t;

/* Tile 0 publishes the attempt after setup.  Workers hold data until
   the generation matches, then claim appendvecs from next_appendvec. */
struct fd_snapin_shmem {
  ulong magic;
  ulong worker_cnt;

  struct __attribute__((aligned(64))) {
    ulong generation;
    ulong fork_id;
  } attempt;

  /* Isolate the hot claim counter on its own cache line. */
  ulong next_appendvec __attribute__((aligned(128)));

  fd_snapin_shmem_totals_t totals;

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

typedef struct fd_snapin_shmem fd_snapin_shmem_t;

#define FD_SNAPIN_SHMEM_MAGIC (0xF17EDA2C0501A910UL)

static inline ulong
fd_snapin_shmem_align( void ) {
  return alignof(fd_snapin_shmem_t);
}

static inline ulong
fd_snapin_shmem_footprint( void ) {
  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof(fd_snapin_shmem_t), sizeof(fd_snapin_shmem_t)              );
  l = FD_LAYOUT_APPEND( l, alignof(int),               FD_SNAPIN_SHMEM_STRIPE_CNT*sizeof(int) );
  return FD_LAYOUT_FINI( l, fd_snapin_shmem_align() );
}

static inline void *
fd_snapin_shmem_new( void * mem,
                     ulong  worker_cnt ) {
  FD_SCRATCH_ALLOC_INIT( l, mem );
  fd_snapin_shmem_t * shmem   = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_snapin_shmem_t), sizeof(fd_snapin_shmem_t)              );
  int *               stripes = FD_SCRATCH_ALLOC_APPEND( l, alignof(int),               FD_SNAPIN_SHMEM_STRIPE_CNT*sizeof(int) );
  shmem->worker_cnt = worker_cnt;
  /* Workspace memory may survive a crash, so clear stale shared state. */
  shmem->attempt.generation = 0UL;
  shmem->attempt.fork_id    = 0UL;
  shmem->next_appendvec     = 0UL;
  fd_memset( &shmem->totals,        0, sizeof(fd_snapin_shmem_totals_t) );
  fd_memset( &shmem->slot_history,  0, sizeof(shmem->slot_history)      );
  fd_memset( &shmem->feature_snoop, 0, sizeof(fd_feature_snoop_t)       );
  fd_memset( stripes, 0, FD_SNAPIN_SHMEM_STRIPE_CNT*sizeof(int) );
  FD_COMPILER_MFENCE();
  shmem->magic = FD_SNAPIN_SHMEM_MAGIC;
  return mem;
}

static inline fd_snapin_shmem_t *
fd_snapin_shmem_join( void * mem ) {
  fd_snapin_shmem_t * shmem = (fd_snapin_shmem_t *)mem;
  if( FD_UNLIKELY( shmem->magic!=FD_SNAPIN_SHMEM_MAGIC ) ) return NULL;
  return shmem;
}

static inline int *
fd_snapin_shmem_stripes( fd_snapin_shmem_t * shmem ) {
  FD_SCRATCH_ALLOC_INIT( l, shmem );
  FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_snapin_shmem_t), sizeof(fd_snapin_shmem_t) );
  return (int *)FD_SCRATCH_ALLOC_APPEND( l, alignof(int), FD_SNAPIN_SHMEM_STRIPE_CNT*sizeof(int) );
}

#endif /* HEADER_fd_src_discof_restore_utils_fd_snapin_shmem_h */
