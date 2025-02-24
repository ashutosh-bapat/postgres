/*-------------------------------------------------------------------------
 *
 * pg_shmem.h
 *	  Platform-independent API for shared memory support.
 *
 * Every port is expected to support shared memory with approximately
 * SysV-ish semantics; in particular, a memory block is not anonymous
 * but has an ID, and we must be able to tell whether there are any
 * remaining processes attached to a block of a specified ID.
 *
 * To simplify life for the SysV implementation, the ID is assumed to
 * consist of two unsigned long values (these are key and ID in SysV
 * terms).  Other platforms may ignore the second value if they need
 * only one ID number.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pg_shmem.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SHMEM_H
#define PG_SHMEM_H

#include "storage/dsm_impl.h"
#include "storage/spin.h"

typedef struct PGShmemHeader	/* standard header for all Postgres shmem */
{
	int32		magic;			/* magic # to identify Postgres segments */
#define PGShmemMagic  679834894
	pid_t		creatorPID;		/* PID of creating process (set but unread) */
	Size		totalsize;		/* total size of segment */
	Size		freeoffset;		/* offset to first free space */
	dsm_handle	dsm_control;	/* ID of dynamic shared memory control seg */
	void	   *index;			/* pointer to ShmemIndex table */
#ifndef WIN32					/* Windows doesn't have useful inode#s */
	dev_t		device;			/* device data directory is on */
	ino_t		inode;			/* inode number of data directory */
#endif
} PGShmemHeader;

typedef struct ShmemSegment
{
	PGShmemHeader *ShmemSegHdr; 	/* shared mem segment header */
	void *ShmemBase; 				/* start address of shared memory */
	void *ShmemEnd; 				/* end+1 address of shared memory */
	slock_t    *ShmemLock; 			/* spinlock for shared memory and LWLock
									 * allocation */
} ShmemSegment;

/* Number of available segments for anonymous memory mappings */
#define ANON_MAPPINGS 6

extern PGDLLIMPORT ShmemSegment Segments[ANON_MAPPINGS];

/* GUC variables */
extern PGDLLIMPORT int shared_memory_type;
extern PGDLLIMPORT int huge_pages;
extern PGDLLIMPORT int huge_page_size;

/* Possible values for huge_pages and huge_pages_status */
typedef enum
{
	HUGE_PAGES_OFF,
	HUGE_PAGES_ON,
	HUGE_PAGES_TRY,				/* only for huge_pages */
	HUGE_PAGES_UNKNOWN,			/* only for huge_pages_status */
}			HugePagesType;

/* Possible values for shared_memory_type */
typedef enum
{
	SHMEM_TYPE_WINDOWS,
	SHMEM_TYPE_SYSV,
	SHMEM_TYPE_MMAP,
}			PGShmemType;

#ifndef WIN32
extern PGDLLIMPORT unsigned long UsedShmemSegID;
#else
extern PGDLLIMPORT HANDLE UsedShmemSegID;
extern PGDLLIMPORT void *ShmemProtectiveRegion;
#endif
extern PGDLLIMPORT void *UsedShmemSegAddr;

#if !defined(WIN32) && !defined(EXEC_BACKEND)
#define DEFAULT_SHARED_MEMORY_TYPE SHMEM_TYPE_MMAP
#elif !defined(WIN32)
#define DEFAULT_SHARED_MEMORY_TYPE SHMEM_TYPE_SYSV
#else
#define DEFAULT_SHARED_MEMORY_TYPE SHMEM_TYPE_WINDOWS
#endif

#ifdef EXEC_BACKEND
extern void PGSharedMemoryReAttach(void);
extern void PGSharedMemoryNoReAttach(void);
#endif

extern PGShmemHeader *PGSharedMemoryCreate(Size size,
										   PGShmemHeader **shim);
extern bool PGSharedMemoryIsInUse(unsigned long id1, unsigned long id2);
extern void PGSharedMemoryDetach(void);
extern void GetHugePageSize(Size *hugepagesize, int *mmap_flags);

/*
 * To be able to dynamically resize largest parts of the data stored in shared
 * memory, we split it into multiple shared memory mappings segments. Each
 * segment contains only certain part of the data, which size depends on
 * NBuffers.
 */

/* The main segment, contains everything except buffer blocks and related data. */
#define MAIN_SHMEM_SEGMENT 0

/* Buffer blocks */
#define BUFFERS_SHMEM_SEGMENT 1

/* Buffer descriptors */
#define BUFFER_DESCRIPTORS_SHMEM_SEGMENT 2

/* Condition variables for buffers */
#define BUFFER_IOCV_SHMEM_SEGMENT 3

/* Checkpoint BufferIds */
#define CHECKPOINT_BUFFERS_SHMEM_SEGMENT 4

/* Buffer strategy status */
#define STRATEGY_SHMEM_SEGMENT 5

#endif							/* PG_SHMEM_H */
