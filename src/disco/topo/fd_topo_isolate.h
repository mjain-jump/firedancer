#ifndef HEADER_fd_src_disco_topo_fd_topo_isolate_h
#define HEADER_fd_src_disco_topo_fd_topo_isolate_h

/* Cache-domain isolation for the snapshot load pipeline.

   fd_topob_auto_layout assigns tiles to CPUs in NUMA-sequential order.  The
   snapshot tiles are created consecutively, so on a single-NUMA machine every
   one of them lands in a single L3 domain and they evict each other.  snapin is
   the binding tile on a page-cache-resident load and joins the accounts-database
   map, so it is the one tile whose isolation pays: measured -11.6 s of an 80.1 s
   load of a 480 GiB mainnet archive on a 256-thread EPYC 9754.

   Two domains have to be avoided, not one.  L3 domains come in pairs that share
   a link to the IO die, and placing snapin on the partner of the domain holding
   the rest of the pipeline recovers only about half the win.  With the pipeline
   in block 0, snapin measured 79.8 s in that same block, 74.4 s in block 1 --
   the partner -- and 68.0 to 68.5 s in each of the other fourteen.  Moving the
   pipeline to block 3 moves the penalty to block 2, and block 1 becomes as good
   as anywhere else, so the rule follows the pairing and not the CPU index.

   The partner of the block starting at home_lo is the block starting at
   home_lo ^ block_sz.  This keys off physical CPU-index structure; the sysfs L3
   ids are scrambled on this part (cpu 0 -> 0, cpu 8 -> 1, cpu 16 -> 8) and
   predict nothing.

   Costs ~1.6 s on a compressed archive, where snapdc rather than snapin is the
   constraint.  Giving snapdc its own domain does not recover that (+4.7 s), so
   there is no placement that is good in both regimes. */

#include "fd_topo.h"

#include <stdio.h>
#include <limits.h>

FD_PROTOTYPES_BEGIN

FD_FN_UNUSED static ulong
fd_topo_cpu_l3_id( ulong cpu_idx ) {
  char path[ PATH_MAX ];
  FD_TEST( fd_cstr_printf_check( path, sizeof(path), NULL,
           "/sys/devices/system/cpu/cpu%lu/cache/index3/id", cpu_idx ) );
  FILE * fp = fopen( path, "r" );
  if( FD_UNLIKELY( !fp ) ) return ULONG_MAX;
  uint v = 0U;
  int  ok = ( 1==fscanf( fp, "%u\n", &v ) );
  fclose( fp );
  return ok ? (ulong)v : ULONG_MAX;
}

/* 1 if cpu_idx is the lowest-numbered thread of its physical core, so that a
   tile moved by fd_topo_isolate_snapin does not take an SMT sibling of a CPU
   another tile is using. */

FD_FN_UNUSED static int
fd_topo_cpu_is_first_thread( ulong cpu_idx ) {
  char path[ PATH_MAX ];
  FD_TEST( fd_cstr_printf_check( path, sizeof(path), NULL,
           "/sys/devices/system/cpu/cpu%lu/topology/thread_siblings_list", cpu_idx ) );
  FILE * fp = fopen( path, "r" );
  if( FD_UNLIKELY( !fp ) ) return 1; /* unknown: do not exclude */
  uint first = 0U;
  int  ok = ( 1==fscanf( fp, "%u", &first ) );
  fclose( fp );
  return ok ? ( (ulong)first==cpu_idx ) : 1;
}

/* The L3 domain sharing an IO-die link with home, or ULONG_MAX if it cannot be
   identified.  home is assumed to cover a contiguous, aligned run of CPU
   indices, which is how x86 parts expose it; if it does not, this returns
   ULONG_MAX and the caller simply skips the partner check. */

FD_FN_UNUSED static ulong
fd_topo_l3_partner( ulong home ) {
  ulong lo = ULONG_MAX;
  for( ulong cpu=0UL; cpu<FD_TILE_MAX; cpu++ ) {
    if( fd_topo_cpu_l3_id( cpu )==home ) { lo = cpu; break; }
  }
  if( FD_UNLIKELY( lo==ULONG_MAX ) ) return ULONG_MAX;

  ulong sz = 0UL;
  while( lo+sz<FD_TILE_MAX && fd_topo_cpu_l3_id( lo+sz )==home ) sz++;
  if( FD_UNLIKELY( !sz || !fd_ulong_is_pow2( sz ) || (lo & (sz-1UL)) ) ) return ULONG_MAX;

  ulong partner_lo = lo ^ sz;
  if( FD_UNLIKELY( partner_lo>=FD_TILE_MAX ) ) return ULONG_MAX;
  ulong partner = fd_topo_cpu_l3_id( partner_lo );
  return fd_ulong_if( partner==home, ULONG_MAX, partner );
}

/* Move snapin out of the L3 domain holding the rest of the snapshot pipeline,
   and out of that domain's pair-partner.  No-op if the cache topology is
   unavailable, if snapin is not pinned, or if no free CPU exists outside its
   domain; uses the partner if that is the only alternative. */

FD_FN_UNUSED static void
fd_topo_isolate_snapin( fd_topo_t * topo ) {
  ulong tile_idx = fd_topo_find_tile( topo, "snapin", 0UL );
  if( FD_UNLIKELY( tile_idx==ULONG_MAX ) ) return;
  fd_topo_tile_t * snapin = &topo->tiles[ tile_idx ];
  if( FD_UNLIKELY( snapin->cpu_idx==ULONG_MAX ) ) return;

  ulong home = fd_topo_cpu_l3_id( snapin->cpu_idx );
  if( FD_UNLIKELY( home==ULONG_MAX ) ) return;
  ulong partner = fd_topo_l3_partner( home );

  ulong fallback = ULONG_MAX;   /* a CPU on the partner, if nothing better exists */

  for( ulong cpu=0UL; cpu<FD_TILE_MAX; cpu++ ) {
    ulong id = fd_topo_cpu_l3_id( cpu );
    if( id==ULONG_MAX || id==home ) continue;
    if( FD_UNLIKELY( !fd_topo_cpu_is_first_thread( cpu ) ) ) continue;

    int taken = 0;
    for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
      if( topo->tiles[ i ].cpu_idx==cpu ) { taken = 1; break; }
    }
    if( FD_UNLIKELY( taken ) ) continue;

    if( FD_UNLIKELY( id==partner ) ) {
      if( fallback==ULONG_MAX ) fallback = cpu;
      continue;
    }

    snapin->cpu_idx = cpu;
    return;
  }

  if( fallback!=ULONG_MAX ) snapin->cpu_idx = fallback;
}

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_disco_topo_fd_topo_isolate_h */
