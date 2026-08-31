#ifndef HEADER_fd_src_discof_restore_utils_fd_snapfs_h
#define HEADER_fd_src_discof_restore_utils_fd_snapfs_h

/* fd_snapfs locates structure inside a `.tar.zst` snapshot file at
   arbitrary byte offsets, so that N workers can each take a disjoint
   byte range of the compressed file and start parsing there without any
   pre-built index and without any handshake.

   Two independent discovery problems:

   1. ZSTD FRAME STARTS (compressed domain).  Agave chunks the
      uncompressed archive at a fixed 32 MiB and compresses each chunk
      as its own zstd frame, so the file is a concatenation of ~15k
      independently decodable frames.  The frame header is a fixed
      10 bytes -- magic, a frame header descriptor of 0x80 (4 byte
      Frame_Content_Size, no dictionary, no content checksum), a window
      descriptor, and the FCS itself -- so a worker can find the next
      real frame start by searching forward for that byte pattern and
      confirming it with a block walk: from a true frame start, walking
      the 3 byte block headers lands exactly on the next frame start (or
      on EOF).  Three consecutive successful walks make an accidental
      match negligible; measured on mainnet this reconstructs a
      byte-identical index to a serial walk with zero false positives.

   2. TAR ENTRY HEADERS (decompressed domain).  Frame boundaries are
      512 byte aligned (32 MiB / 512 = 65536) so a tar header is never
      split by one, but a frame boundary lands mid-entry essentially
      always: a worker's range starts 0.07-7.6 MiB before the first tar
      header it owns.  A header is recognised by the ustar magic, a
      valid octal checksum, an `accounts/<slot>.<id>` name and a
      forward chain h -> h + 512 + roundup(size,512) that keeps landing
      on more valid headers.

   Both discoveries are heuristics over attacker-controlled bytes, so
   both MUST be confirmed after the fact:

     - a discovered frame start is confirmed by the worker that owns the
       range ENDING there: it walks real frames from its own (confirmed)
       start and must land exactly on that offset;
     - a discovered first-header offset is confirmed by the preceding
       worker, which parses past its range end until its last entry
       closes and must stop at exactly that offset.

   fd_snapin_tile.c performs both confirmations at FINI and hard-fails
   the load on any mismatch. */

#include "../../../util/fd_util.h"
#include "../../../util/archive/fd_tar.h"

#include <unistd.h>
#include <errno.h>

/* Frame header: 28 B5 2F FD | FHD | WD | FCS[4].  FHD 0x80 means
   FCS_flag=0b10 (4 byte FCS), Single_Segment=0, Content_Checksum=0,
   Dictionary_ID=0. */

#define FD_SNAPFS_FRAME_HDR_SZ (10UL)

/* Block header: 3 bytes, [last:1][type:2][size:21]. */

#define FD_SNAPFS_BLOCK_RAW        (0U)
#define FD_SNAPFS_BLOCK_RLE        (1U)
#define FD_SNAPFS_BLOCK_COMPRESSED (2U)

/* How deep the block-walk confirmation chain runs on a candidate frame
   start, and the tar header chain on a candidate tar header. */

#define FD_SNAPFS_FRAME_CHAIN_DEPTH (3UL)
#define FD_SNAPFS_TAR_CHAIN_DEPTH   (4UL)

/* Maximum compressed bytes searched forward from an anchor before
   giving up.  The largest compressed frame measured on mainnet is
   23.5 MiB, so one frame is always found well inside this. */

#define FD_SNAPFS_FRAME_SEARCH_MAX (256UL<<20)

/* Buffer sizes for the pread-backed readers. */

#define FD_SNAPFS_SCAN_BUF_SZ (4UL<<20)
#define FD_SNAPFS_WALK_BUF_SZ (256UL<<10)

/* fd_snapfs_rdr_t is a trivially small pread window over the snapshot
   file: fd_snapfs_peek hands back a pointer to `need` contiguous bytes
   at file offset `off`, refilling when the window does not cover them.
   Returns NULL past end of file. */

struct fd_snapfs_rdr {
  int     fd;
  ulong   file_sz;
  uchar * buf;
  ulong   buf_cap;
  ulong   buf_off;
  ulong   buf_len;
};

typedef struct fd_snapfs_rdr fd_snapfs_rdr_t;

static inline void
fd_snapfs_rdr_init( fd_snapfs_rdr_t * rdr,
                    int               fd,
                    ulong             file_sz,
                    uchar *           buf,
                    ulong             buf_cap ) {
  rdr->fd      = fd;
  rdr->file_sz = file_sz;
  rdr->buf     = buf;
  rdr->buf_cap = buf_cap;
  rdr->buf_off = 0UL;
  rdr->buf_len = 0UL;
}

/* fd_snapfs_pread_all reads sz bytes at off, retrying short reads.
   Returns the number of bytes read (< sz only at end of file), or
   ULONG_MAX on error. */

static inline ulong
fd_snapfs_pread_all( int     fd,
                     uchar * dst,
                     ulong   sz,
                     ulong   off ) {
  ulong got = 0UL;
  while( got<sz ) {
    long r = pread( fd, dst+got, sz-got, (long)(off+got) );
    if( FD_UNLIKELY( r<0L ) ) {
      if( FD_LIKELY( errno==EINTR ) ) continue;
      return ULONG_MAX;
    }
    if( FD_UNLIKELY( !r ) ) break; /* EOF */
    got += (ulong)r;
  }
  return got;
}

static inline uchar const *
fd_snapfs_peek( fd_snapfs_rdr_t * rdr,
                ulong             off,
                ulong             need ) {
  if( FD_UNLIKELY( need>rdr->buf_cap ) ) return NULL;
  if( FD_UNLIKELY( off+need>rdr->file_sz ) ) return NULL;
  if( FD_LIKELY( off>=rdr->buf_off && off+need<=rdr->buf_off+rdr->buf_len ) ) {
    return rdr->buf + ( off - rdr->buf_off );
  }
  ulong want = fd_ulong_min( rdr->buf_cap, rdr->file_sz-off );
  ulong got  = fd_snapfs_pread_all( rdr->fd, rdr->buf, want, off );
  if( FD_UNLIKELY( got==ULONG_MAX || got<need ) ) return NULL;
  rdr->buf_off = off;
  rdr->buf_len = got;
  return rdr->buf;
}

/* fd_snapfs_frame_hdr_ok returns 1 if a zstd frame header of the shape
   Agave's snapshot writer emits starts at off. */

static inline int
fd_snapfs_frame_hdr_ok( fd_snapfs_rdr_t * rdr,
                        ulong             off ) {
  uchar const * p = fd_snapfs_peek( rdr, off, FD_SNAPFS_FRAME_HDR_SZ );
  if( FD_UNLIKELY( !p ) ) return 0;
  return p[0]==0x28 && p[1]==0xB5 && p[2]==0x2F && p[3]==0xFD && p[4]==0x80;
}

/* fd_snapfs_frame_next walks the block headers of the frame starting at
   off and returns the offset one past its last block, i.e. the next
   frame start (or file_sz at end of file).  Returns ULONG_MAX if the
   walk hits a malformed block header or runs off the end. */

static inline ulong
fd_snapfs_frame_next( fd_snapfs_rdr_t * rdr,
                      ulong             off ) {
  if( FD_UNLIKELY( !fd_snapfs_frame_hdr_ok( rdr, off ) ) ) return ULONG_MAX;
  ulong p = off + FD_SNAPFS_FRAME_HDR_SZ;
  for(;;) {
    uchar const * b = fd_snapfs_peek( rdr, p, 3UL );
    if( FD_UNLIKELY( !b ) ) return ULONG_MAX;
    uint  hdr  = (uint)b[0] | ((uint)b[1]<<8) | ((uint)b[2]<<16);
    uint  last = hdr & 1U;
    uint  type = (hdr>>1) & 3U;
    ulong sz   = (ulong)(hdr>>3);
    if( FD_UNLIKELY( type==3U ) ) return ULONG_MAX; /* reserved */
    p += 3UL + ( type==FD_SNAPFS_BLOCK_RLE ? 1UL : sz );
    if( FD_UNLIKELY( p>rdr->file_sz ) ) return ULONG_MAX;
    if( last ) return p;
  }
}

/* fd_snapfs_frame_content_sz returns the Frame_Content_Size declared in
   the header of the frame at off, or ULONG_MAX if off does not carry a
   frame header of the shape Agave's snapshot writer emits.  Because that
   writer always sets FCS_flag=0b10 (see FD_SNAPFS_FRAME_HDR_SZ above),
   every frame declares its decompressed size in its header and the
   decompressed layout of the archive is readable without decompressing
   anything. */

static inline ulong
fd_snapfs_frame_content_sz( fd_snapfs_rdr_t * rdr,
                            ulong             off ) {
  uchar const * p = fd_snapfs_peek( rdr, off, FD_SNAPFS_FRAME_HDR_SZ );
  if( FD_UNLIKELY( !p ) ) return ULONG_MAX;
  if( FD_UNLIKELY( !( p[0]==0x28 && p[1]==0xB5 && p[2]==0x2F && p[3]==0xFD && p[4]==0x80 ) ) ) return ULONG_MAX;
  return (ulong)p[6] | ((ulong)p[7]<<8) | ((ulong)p[8]<<16) | ((ulong)p[9]<<24);
}

/* fd_snapfs_frame_start_ok confirms a candidate frame start with a
   depth-deep block walk: every walk must land on another frame header
   (or exactly on end of file). */

static inline int
fd_snapfs_frame_start_ok( fd_snapfs_rdr_t * rdr,
                          ulong             off,
                          ulong             depth ) {
  ulong p = off;
  for( ulong i=0UL; i<depth; i++ ) {
    ulong nxt = fd_snapfs_frame_next( rdr, p );
    if( FD_UNLIKELY( nxt==ULONG_MAX ) ) return 0;
    if( FD_UNLIKELY( nxt==rdr->file_sz ) ) return 1; /* clean end of archive */
    if( FD_UNLIKELY( !fd_snapfs_frame_hdr_ok( rdr, nxt ) ) ) return 0;
    p = nxt;
  }
  return 1;
}

/* fd_snapfs_find_frame_start searches forward from `from` for the first
   confirmed zstd frame start, using scan_buf (>= FD_SNAPFS_SCAN_BUF_SZ)
   for the pattern search and walk_buf (>= FD_SNAPFS_WALK_BUF_SZ) for
   the confirmation walk.  Returns ULONG_MAX if none is found within
   FD_SNAPFS_FRAME_SEARCH_MAX bytes.

   Deterministic given the file, so two workers anchored at the same
   offset always agree without exchanging anything. */

static inline ulong
fd_snapfs_find_frame_start( int     fd,
                            ulong   file_sz,
                            ulong   from,
                            uchar * scan_buf,
                            ulong   scan_buf_sz,
                            uchar * walk_buf,
                            ulong   walk_buf_sz ) {
  if( FD_UNLIKELY( from>=file_sz ) ) return ULONG_MAX;

  fd_snapfs_rdr_t walk[1];
  fd_snapfs_rdr_init( walk, fd, file_sz, walk_buf, walk_buf_sz );

  ulong limit = fd_ulong_min( file_sz, from+FD_SNAPFS_FRAME_SEARCH_MAX );
  ulong pos   = from;
  while( pos<limit ) {
    ulong want = fd_ulong_min( scan_buf_sz, file_sz-pos );
    ulong got  = fd_snapfs_pread_all( fd, scan_buf, want, pos );
    if( FD_UNLIKELY( got==ULONG_MAX ) ) return ULONG_MAX;
    if( FD_UNLIKELY( got<FD_SNAPFS_FRAME_HDR_SZ ) ) return ULONG_MAX;

    ulong scan_end = got - 5UL; /* need 5 bytes of pattern past the first */
    ulong i        = 0UL;
    while( i<scan_end ) {
      uchar const * hit = (uchar const *)memchr( scan_buf+i, 0x28, scan_end-i );
      if( FD_UNLIKELY( !hit ) ) break;
      ulong o = (ulong)( hit - scan_buf );
      if( hit[1]==0xB5 && hit[2]==0x2F && hit[3]==0xFD && hit[4]==0x80 ) {
        ulong cand = pos + o;
        if( cand>=limit ) return ULONG_MAX;
        if( fd_snapfs_frame_start_ok( walk, cand, FD_SNAPFS_FRAME_CHAIN_DEPTH ) ) return cand;
      }
      i = o + 1UL;
    }

    if( FD_UNLIKELY( got<scan_buf_sz ) ) break; /* hit end of file */
    pos += got - 5UL;
  }
  return ULONG_MAX;
}

/* fd_snapfs_tar_hdr_ok returns 1 if a plausible tar entry header for an
   `accounts/<slot>.<id>` appendvec sits at hdr[0,512), and stores its
   entry size (excluding the header, excluding trailing padding). */

static inline int
fd_snapfs_tar_hdr_ok( uchar const * hdr,
                      ulong *       opt_size ) {
  fd_tar_meta_t const * meta = (fd_tar_meta_t const *)hdr;

  if( FD_UNLIKELY( memcmp( meta->magic, FD_TAR_MAGIC, FD_TAR_MAGIC_SZ ) ) ) return 0;
  if( FD_UNLIKELY( !fd_tar_meta_is_reg( meta ) ) ) return 0;

  /* Octal header checksum over all 512 bytes with the checksum field
     itself read as spaces. */
  ulong sum = 0UL;
  for( ulong i=0UL;   i<148UL; i++ ) sum += (ulong)hdr[ i ];
  for( ulong i=148UL; i<156UL; i++ ) sum += 0x20UL;
  for( ulong i=156UL; i<512UL; i++ ) sum += (ulong)hdr[ i ];

  ulong want = 0UL;
  char const * p = meta->chksum;
  while( p<meta->chksum+8 && *p==' ' ) p++;
  int digits = 0;
  for( ; p<meta->chksum+8; p++ ) {
    if( *p=='\0' || *p==' ' ) break;
    if( FD_UNLIKELY( *p<'0' || *p>'7' ) ) return 0;
    want = (want<<3) + (ulong)(*p-'0');
    digits++;
  }
  if( FD_UNLIKELY( !digits || want!=sum ) ) return 0;

  /* A mid-stream worker only ever starts inside the accounts region:
     everything before it (version, status cache, manifest) lives in the
     first ~217 MiB of the archive, i.e. inside worker 0's range. */
  if( FD_UNLIKELY( strncmp( meta->name, "accounts/", 9UL ) ) ) return 0;

  ulong sz = fd_tar_meta_get_size( meta );
  if( FD_UNLIKELY( sz==ULONG_MAX || !sz ) ) return 0;
  if( FD_UNLIKELY( sz>(1UL<<40) ) ) return 0;

  if( opt_size ) *opt_size = sz;
  return 1;
}

/* fd_snapfs_find_tar_header scans buf[0,len) for the first 512 byte
   aligned offset carrying a confirmed tar entry header.  Confirmation
   is a forward chain of up to FD_SNAPFS_TAR_CHAIN_DEPTH further
   headers, as deep as the buffer allows.  Returns ULONG_MAX if none is
   found. */

static inline ulong
fd_snapfs_find_tar_header( uchar const * buf,
                           ulong         len ) {
  for( ulong c=0UL; c+512UL<=len; c+=512UL ) {
    ulong sz;
    if( !fd_snapfs_tar_hdr_ok( buf+c, &sz ) ) continue;

    ulong p  = c;
    ulong ok = 1UL;
    for( ulong d=0UL; d<FD_SNAPFS_TAR_CHAIN_DEPTH; d++ ) {
      ulong nxt = p + 512UL + fd_ulong_align_up( sz, 512UL );
      if( nxt+512UL>len ) break; /* as deep as the buffer allows */
      if( !fd_snapfs_tar_hdr_ok( buf+nxt, &sz ) ) { ok = 0UL; break; }
      p = nxt;
    }
    if( FD_LIKELY( ok ) ) return c;
  }
  return ULONG_MAX;
}

/* The archive's HEADER REGION -- `version`, `snapshots/status_cache`
   and `snapshots/<slot>/<slot>` (the manifest) -- precedes every
   `accounts/` entry, and only ONE worker can own it: it is the worker
   that feeds the manifest and status cache parsers, and a worker whose
   range starts inside the manifest finds no tar entry header at all
   (fd_snapfs_find_tar_header only accepts `accounts/` names, by
   design -- see the comment on fd_snapfs_tar_hdr_ok).  An even byte
   split of a ~112 GB full archive gives worker 0 GiBs and the ~217 MiB
   header region is never a constraint, but an even split of a ~1 GB
   incremental gives worker 0 tens of MB against a ~200 MiB header
   region, and it is.  The two helpers below let every worker derive the
   end of that region locally, so the split stays a pure function of the
   file and no handshake is needed.

   Both walks are bounded: the header region is at most a handful of
   entries and, decompressed, a few hundred MiB. */

#define FD_SNAPFS_HEAD_ENTRY_MAX (16UL)
#define FD_SNAPFS_HEAD_FRAME_MAX (64UL)

/* fd_snapfs_head_region_end walks the tar entry header chain from the
   start of the archive's DECOMPRESSED stream -- buf[0,len) must begin at
   decompressed offset 0 -- and returns the decompressed offset at which
   the header region ends, i.e. one past the last entry whose name is not
   `accounts/`.  Returns ULONG_MAX if the stream does not look like tar
   at all, or if the chain runs longer than FD_SNAPFS_HEAD_ENTRY_MAX
   entries without leaving the buffer.

   The chain leaving the buffer is the NORMAL exit, not a failure: only
   the headers have to be readable, and the manifest -- the last
   non-`accounts/` entry Agave writes -- has a ~190 MB body that no
   single-frame buffer can hold.  So the walk reads five headers out of
   the first 32 MiB frame (version, snapshots/, status_cache,
   snapshots/<slot>, manifest) and returns where the manifest's body
   ends.

   The result is therefore a LOWER BOUND on the offset of the first
   `accounts/` entry, exact whenever the manifest is the last
   header-region entry.  If it were ever short, the worker whose range
   started there would find no tar entry header and the load would fail
   loudly; nothing silently loads the wrong bytes. */

static inline ulong
fd_snapfs_head_region_end( uchar const * buf,
                           ulong         len ) {
  ulong off = 0UL;
  for( ulong i=0UL; i<FD_SNAPFS_HEAD_ENTRY_MAX; i++ ) {
    if( FD_UNLIKELY( off+FD_TAR_BLOCK_SZ>len ) ) return i ? off : ULONG_MAX;
    fd_tar_meta_t const * meta = (fd_tar_meta_t const *)( buf+off );
    if( FD_UNLIKELY( memcmp( meta->magic, FD_TAR_MAGIC, FD_TAR_MAGIC_SZ ) ) ) return ULONG_MAX;
    if( FD_UNLIKELY( !strncmp( meta->name, "accounts/", 9UL ) ) ) return off;
    ulong sz = fd_tar_meta_get_size( meta );
    if( FD_UNLIKELY( sz==ULONG_MAX || sz>(1UL<<40) ) ) return ULONG_MAX;
    if( FD_UNLIKELY( !fd_tar_meta_is_reg( meta ) ) ) sz = 0UL; /* directory entries carry no body */
    off += FD_TAR_BLOCK_SZ + fd_ulong_align_up( sz, FD_TAR_BLOCK_SZ );
  }
  return ULONG_MAX;
}

/* fd_snapfs_frame_at_or_after walks the archive's frames from byte 0 --
   in the compressed domain only, reading the 10 byte frame headers and
   3 byte block headers, decompressing nothing -- and returns the
   compressed offset of the first frame whose DECOMPRESSED start is at
   or after dprod.  Returns ULONG_MAX if the walk hits a malformed
   header, reaches end of file, or needs more than
   FD_SNAPFS_HEAD_FRAME_MAX frames.

   Every frame it lands on is confirmed by construction (the block walk
   that found it validated every block header in the previous frame), so
   the returned offset is a real frame start and a valid decode entry
   point. */

static inline ulong
fd_snapfs_frame_at_or_after( int     fd,
                             ulong   file_sz,
                             ulong   dprod,
                             uchar * walk_buf,
                             ulong   walk_buf_sz ) {
  fd_snapfs_rdr_t rdr[1];
  fd_snapfs_rdr_init( rdr, fd, file_sz, walk_buf, walk_buf_sz );

  ulong comp = 0UL, decomp = 0UL;
  for( ulong i=0UL; i<FD_SNAPFS_HEAD_FRAME_MAX; i++ ) {
    if( FD_LIKELY( decomp>=dprod ) ) return comp;
    ulong fcs = fd_snapfs_frame_content_sz( rdr, comp );
    if( FD_UNLIKELY( fcs==ULONG_MAX || !fcs ) ) return ULONG_MAX;
    ulong nxt = fd_snapfs_frame_next( rdr, comp );
    if( FD_UNLIKELY( nxt==ULONG_MAX || nxt<=comp || nxt>=file_sz ) ) return ULONG_MAX;
    comp    = nxt;
    decomp += fcs;
  }
  return ULONG_MAX;
}

#endif /* HEADER_fd_src_discof_restore_utils_fd_snapfs_h */
