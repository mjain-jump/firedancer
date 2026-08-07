#ifndef HEADER_fd_src_disco_topo_fd_cpu_topo_h
#define HEADER_fd_src_disco_topo_fd_cpu_topo_h

#include "../fd_disco_base.h"
#include "../../util/tile/fd_tile.h"

struct fd_topo_cpu {
  ulong idx;
  int   online;
  ulong numa_node;
  ulong sibling;

  /* Index of the last-level (L3) cache this CPU shares, as reported by
     /sys/devices/system/cpu/cpuN/cache/index3/id.  ULONG_MAX if the
     system does not report one.  Used by the automatic layout to spread
     tiles across cache domains rather than packing them into one. */
  ulong l3_id;
};

typedef struct fd_topo_cpu fd_topo_cpu_t;

struct fd_topo_cpus {
  ulong         numa_node_cnt;

  ulong         cpu_cnt;
  fd_topo_cpu_t cpu[ FD_TILE_MAX ];
};

typedef struct fd_topo_cpus fd_topo_cpus_t;

FD_PROTOTYPES_BEGIN

/* Initialize the CPU topology structure by reading information from the
   operating system.  If the CPU topology cannot be determined, logs an
   error and exits the process. */

void
fd_topo_cpus_init( fd_topo_cpus_t * cpus );

void
fd_topo_cpus_printf( fd_topo_cpus_t * cpus );

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_disco_topo_fd_cpu_topo_h */
