/*-------------------------------------------------------------------------
 *
 * shmem.c
 *	  create shared memory and initialize shared memory data structures.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/shmem.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * POSTGRES processes share one or more regions of shared memory.
 * The shared memory is created by a postmaster and is inherited
 * by each backend via fork() (or, in some ports, via other OS-specific
 * methods).  The routines in this file are used for allocating and
 * binding to shared memory data structures.
 *
 * NOTES:
 *		(a) There are three kinds of shared memory data structures
 *	available to POSTGRES: fixed-size structures, queues and hash
 *	tables.  Fixed-size structures contain things like global variables
 *	for a module and should never be allocated after the shared memory
 *	initialization phase.  Hash tables have a fixed maximum size, but
 *	their actual size can vary dynamically.  When entries are added
 *	to the table, more space is allocated.  Queues link data structures
 *	that have been allocated either within fixed-size structures or as hash
 *	buckets.  Each shared data structure has a string name to identify
 *	it (assigned in the module that declares it).
 *
 *		(b) During initialization, each module looks for its
 *	shared data structures in a hash table called the "Shmem Index".
 *	If the data structure is not present, the caller can allocate
 *	a new one and initialize it.  If the data structure is present,
 *	the caller "attaches" to the structure by initializing a pointer
 *	in the local address space.
 *		The shmem index has two purposes: first, it gives us
 *	a simple model of how the world looks when a backend process
 *	initializes.  If something is present in the shmem index,
 *	it is initialized.  If it is not, it is uninitialized.  Second,
 *	the shmem index allows us to allocate shared memory on demand
 *	instead of trying to preallocate structures and hard-wire the
 *	sizes and locations in header files.  If you are using a lot
 *	of shared memory in a lot of different places (and changing
 *	things during development), this is important.
 *
 *		(c) In standard Unix-ish environments, individual backends do not
 *	need to re-establish their local pointers into shared memory, because
 *	they inherit correct values of those variables via fork() from the
 *	postmaster.  However, this does not work in the EXEC_BACKEND case.
 *	In ports using EXEC_BACKEND, new backends have to set up their local
 *	pointers using the method described in (b) above.
 *
 *		(d) memory allocation model: shared memory can never be
 *	freed, once allocated.   Each hash table has its own free list,
 *	so hash buckets can be reused when an item is deleted.  However,
 *	if one hash table grows very large and then shrinks, its space
 *	cannot be redistributed to other tables.  We could build a simple
 *	hash bucket garbage collector if need be.  Right now, it seems
 *	unnecessary.
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"

static void *ShmemAllocRaw(Size size, Size *allocated_size);
static void *ShmemAllocRawInSegment(Size size, Size *allocated_size,
								 int shmem_segment);

/* shared memory global variables */

ShmemSegment Segments[ANON_MAPPINGS];

/*
 * Primary index hashtable for shmem, for simplicity we use a single for all
 * shared memory segments. There can be performance consequences of that, and
 * an alternative option would be to have one index per shared memory segments.
 */
static HTAB *ShmemIndex = NULL;


/*
 *	InitShmemAccess() --- set up basic pointers to shared memory.
 */
void
InitShmemAccess(PGShmemHeader *seghdr)
{
	InitShmemAccessInSegment(seghdr, MAIN_SHMEM_SEGMENT);
}

void
InitShmemAccessInSegment(PGShmemHeader *seghdr, int shmem_segment)
{
	PGShmemHeader *shmhdr = (PGShmemHeader *) seghdr;
	ShmemSegment *seg = &Segments[shmem_segment];
	seg->ShmemSegHdr = shmhdr;
	seg->ShmemBase = (void *) shmhdr;
	seg->ShmemEnd = (char *) seg->ShmemBase + shmhdr->totalsize;
}

/*
 *	InitShmemAllocation() --- set up shared-memory space allocation.
 *
 * This should be called only in the postmaster or a standalone backend.
 */
void
InitShmemAllocation(void)
{
	InitShmemAllocationInSegment(MAIN_SHMEM_SEGMENT);
}

void
InitShmemAllocationInSegment(int shmem_segment)
{
	PGShmemHeader *shmhdr = Segments[shmem_segment].ShmemSegHdr;
	char	   *aligned;

	Assert(shmhdr != NULL);

	/*
	 * Initialize the spinlock used by ShmemAlloc.  We must use
	 * ShmemAllocUnlocked, since obviously ShmemAlloc can't be called yet.
	 */
	Segments[shmem_segment].ShmemLock = (slock_t *) ShmemAllocUnlockedInSegment(sizeof(slock_t), shmem_segment);

	SpinLockInit(Segments[shmem_segment].ShmemLock);

	/*
	 * Allocations after this point should go through ShmemAlloc, which
	 * expects to allocate everything on cache line boundaries.  Make sure the
	 * first allocation begins on a cache line boundary.
	 */
	aligned = (char *)
		(CACHELINEALIGN((((char *) shmhdr) + shmhdr->freeoffset)));
	shmhdr->freeoffset = aligned - (char *) shmhdr;

	/* ShmemIndex can't be set up yet (need LWLocks first) */
	shmhdr->index = NULL;
	ShmemIndex = (HTAB *) NULL;
}

/*
 * ShmemAlloc -- allocate max-aligned chunk from shared memory
 *
 * Throws error if request cannot be satisfied.
 *
 * Assumes ShmemLock and ShmemSegHdr are initialized.
 */
void *
ShmemAlloc(Size size)
{
	return ShmemAllocInSegment(size, MAIN_SHMEM_SEGMENT);
}

void *
ShmemAllocInSegment(Size size, int shmem_segment)
{
	void	   *newSpace;
	Size		allocated_size;

	newSpace = ShmemAllocRawInSegment(size, &allocated_size, shmem_segment);
	if (!newSpace)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory (%zu bytes requested)",
						size)));
	return newSpace;
}

/*
 * ShmemAllocNoError -- allocate max-aligned chunk from shared memory
 *
 * As ShmemAlloc, but returns NULL if out of space, rather than erroring.
 */
void *
ShmemAllocNoError(Size size)
{
	Size		allocated_size;

	return ShmemAllocRaw(size, &allocated_size);
}

/*
 * ShmemAllocRaw -- allocate align chunk and return allocated size
 *
 * Also sets *allocated_size to the number of bytes allocated, which will
 * be equal to the number requested plus any padding we choose to add.
 */
static void *
ShmemAllocRaw(Size size, Size *allocated_size)
{
	return ShmemAllocRawInSegment(size, allocated_size, MAIN_SHMEM_SEGMENT);
}

static void *
ShmemAllocRawInSegment(Size size, Size *allocated_size, int shmem_segment)
{
	Size		newStart;
	Size		newFree;
	void	   *newSpace;

	/*
	 * Ensure all space is adequately aligned.  We used to only MAXALIGN this
	 * space but experience has proved that on modern systems that is not good
	 * enough.  Many parts of the system are very sensitive to critical data
	 * structures getting split across cache line boundaries.  To avoid that,
	 * attempt to align the beginning of the allocation to a cache line
	 * boundary.  The calling code will still need to be careful about how it
	 * uses the allocated space - e.g. by padding each element in an array of
	 * structures out to a power-of-two size - but without this, even that
	 * won't be sufficient.
	 */
	size = CACHELINEALIGN(size);
	*allocated_size = size;

	Assert(Segments[shmem_segment].ShmemSegHdr != NULL);

	SpinLockAcquire(Segments[shmem_segment].ShmemLock);

	newStart = Segments[shmem_segment].ShmemSegHdr->freeoffset;

	newFree = newStart + size;
	if (newFree <= Segments[shmem_segment].ShmemSegHdr->totalsize)
	{
		newSpace = (char *) Segments[shmem_segment].ShmemBase + newStart;
		Segments[shmem_segment].ShmemSegHdr->freeoffset = newFree;
	}
	else
		newSpace = NULL;

	SpinLockRelease(Segments[shmem_segment].ShmemLock);

	/* note this assert is okay with newSpace == NULL */
	Assert(newSpace == (void *) CACHELINEALIGN(newSpace));

	return newSpace;
}

/*
 * ShmemAllocUnlocked -- allocate max-aligned chunk from shared memory
 *
 * Allocate space without locking ShmemLock.  This should be used for,
 * and only for, allocations that must happen before ShmemLock is ready.
 *
 * We consider maxalign, rather than cachealign, sufficient here.
 */
void *
ShmemAllocUnlocked(Size size)
{
	return ShmemAllocUnlockedInSegment(size, MAIN_SHMEM_SEGMENT);
}

void *
ShmemAllocUnlockedInSegment(Size size, int shmem_segment)
{
	Size		newStart;
	Size		newFree;
	void	   *newSpace;

	/*
	 * Ensure allocated space is adequately aligned.
	 */
	size = MAXALIGN(size);

	Assert(Segments[shmem_segment].ShmemSegHdr != NULL);

	newStart = Segments[shmem_segment].ShmemSegHdr->freeoffset;

	newFree = newStart + size;
	if (newFree > Segments[shmem_segment].ShmemSegHdr->totalsize)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory (%zu bytes requested)",
						size)));
	Segments[shmem_segment].ShmemSegHdr->freeoffset = newFree;

	newSpace = (char *) Segments[shmem_segment].ShmemBase + newStart;

	Assert(newSpace == (void *) MAXALIGN(newSpace));

	return newSpace;
}

/*
 * ShmemAddrIsValid -- test if an address refers to shared memory
 *
 * Returns true if the pointer points within the shared memory segment.
 */
bool
ShmemAddrIsValid(const void *addr)
{
	return ShmemAddrIsValidInSegment(addr, MAIN_SHMEM_SEGMENT);
}

bool
ShmemAddrIsValidInSegment(const void *addr, int shmem_segment)
{
	return (addr >= Segments[shmem_segment].ShmemBase) && (addr < Segments[shmem_segment].ShmemEnd);
}

/*
 *	InitShmemIndex() --- set up or attach to shmem index table.
 */
void
InitShmemIndex(void)
{
	HASHCTL		info;

	/*
	 * Create the shared memory shmem index.
	 *
	 * Since ShmemInitHash calls ShmemInitStruct, which expects the ShmemIndex
	 * hashtable to exist already, we have a bit of a circularity problem in
	 * initializing the ShmemIndex itself.  The special "ShmemIndex" hash
	 * table name will tell ShmemInitStruct to fake it.
	 */
	info.keysize = SHMEM_INDEX_KEYSIZE;
	info.entrysize = sizeof(ShmemIndexEnt);

	ShmemIndex = ShmemInitHash("ShmemIndex",
							   SHMEM_INDEX_SIZE, SHMEM_INDEX_SIZE,
							   &info,
							   HASH_ELEM | HASH_STRINGS);
}

/*
 * ShmemInitHash -- Create and initialize, or attach to, a
 *		shared memory hash table.
 *
 * We assume caller is doing some kind of synchronization
 * so that two processes don't try to create/initialize the same
 * table at once.  (In practice, all creations are done in the postmaster
 * process; child processes should always be attaching to existing tables.)
 *
 * max_size is the estimated maximum number of hashtable entries.  This is
 * not a hard limit, but the access efficiency will degrade if it is
 * exceeded substantially (since it's used to compute directory size and
 * the hash table buckets will get overfull).
 *
 * init_size is the number of hashtable entries to preallocate.  For a table
 * whose maximum size is certain, this should be equal to max_size; that
 * ensures that no run-time out-of-shared-memory failures can occur.
 *
 * *infoP and hash_flags must specify at least the entry sizes and key
 * comparison semantics (see hash_create()).  Flag bits and values specific
 * to shared-memory hash tables are added here, except that callers may
 * choose to specify HASH_PARTITION and/or HASH_FIXED_SIZE.
 *
 * Note: before Postgres 9.0, this function returned NULL for some failure
 * cases.  Now, it always throws error instead, so callers need not check
 * for NULL.
 */
HTAB *
ShmemInitHash(const char *name,		/* table string name for shmem index */
			  long init_size,	/* initial table size */
			  long max_size,	/* max size of the table */
			  HASHCTL *infoP,	/* info about key and bucket size */
			  int hash_flags)	/* info about infoP */
{
	return ShmemInitHashInSegment(name, init_size, max_size, infoP, hash_flags,
							   MAIN_SHMEM_SEGMENT);
}

HTAB *
ShmemInitHashInSegment(const char *name,		/* table string name for shmem index */
			  long init_size,		/* initial table size */
			  long max_size,		/* max size of the table */
			  HASHCTL *infoP,		/* info about key and bucket size */
			  int hash_flags,		/* info about infoP */
			  int shmem_segment) 	/* in which segment to keep the table */
{
	bool		found;
	void	   *location;

	/*
	 * Hash tables allocated in shared memory have a fixed directory; it can't
	 * grow or other backends wouldn't be able to find it. So, make sure we
	 * make it big enough to start with.
	 *
	 * The shared memory allocator must be specified too.
	 */
	infoP->dsize = infoP->max_dsize = hash_select_dirsize(max_size);
	infoP->alloc = ShmemAllocNoError;
	hash_flags |= HASH_SHARED_MEM | HASH_ALLOC | HASH_DIRSIZE;

	/* look it up in the shmem index */
	location = ShmemInitStructInSegment(name,
							   hash_get_shared_size(infoP, hash_flags),
							   &found, shmem_segment);

	/*
	 * if it already exists, attach to it rather than allocate and initialize
	 * new space
	 */
	if (found)
		hash_flags |= HASH_ATTACH;

	/* Pass location of hashtable header to hash_create */
	infoP->hctl = (HASHHDR *) location;

	return hash_create(name, init_size, infoP, hash_flags);
}

/*
 * ShmemInitStruct -- Create/attach to a structure in shared memory.
 *
 *		This is called during initialization to find or allocate
 *		a data structure in shared memory.  If no other process
 *		has created the structure, this routine allocates space
 *		for it.  If it exists already, a pointer to the existing
 *		structure is returned.
 *
 *	Returns: pointer to the object.  *foundPtr is set true if the object was
 *		already in the shmem index (hence, already initialized).
 *
 *	Note: before Postgres 9.0, this function returned NULL for some failure
 *	cases.  Now, it always throws error instead, so callers need not check
 *	for NULL.
 */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	return ShmemInitStructInSegment(name, size, foundPtr, MAIN_SHMEM_SEGMENT);
}

void *
ShmemInitStructInSegment(const char *name, Size size, bool *foundPtr,
					  int shmem_segment)
{
	ShmemIndexEnt *result;
	void	   *structPtr;

	LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);

	if (!ShmemIndex)
	{
		PGShmemHeader *shmemseghdr = Segments[shmem_segment].ShmemSegHdr;

		/* Must be trying to create/attach to ShmemIndex itself */
		Assert(strcmp(name, "ShmemIndex") == 0);

		if (IsUnderPostmaster)
		{
			/* Must be initializing a (non-standalone) backend */
			Assert(shmemseghdr->index != NULL);
			structPtr = shmemseghdr->index;
			*foundPtr = true;
		}
		else
		{
			/*
			 * If the shmem index doesn't exist, we are bootstrapping: we must
			 * be trying to init the shmem index itself.
			 *
			 * Notice that the ShmemIndexLock is released before the shmem
			 * index has been initialized.  This should be OK because no other
			 * process can be accessing shared memory yet.
			 */
			Assert(shmemseghdr->index == NULL);
			structPtr = ShmemAllocInSegment(size, shmem_segment);
			shmemseghdr->index = structPtr;
			*foundPtr = false;
		}
		LWLockRelease(ShmemIndexLock);
		return structPtr;
	}

	/* look it up in the shmem index */
	result = (ShmemIndexEnt *)
		hash_search(ShmemIndex, name, HASH_ENTER_NULL, foundPtr);

	if (!result)
	{
		LWLockRelease(ShmemIndexLock);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("could not create ShmemIndex entry for data structure \"%s\" in segment %d",
						name, shmem_segment)));
	}

	if (*foundPtr)
	{
		/*
		 * Structure is in the shmem index so someone else has allocated it
		 * already. Verify the structure's size:
		 * - If it's the same, we've found the expected structure.
		 * - If it's different, we're resizing the expected structure.
		 *
		 * XXX: There is an implicit assumption this can only happen in
		 * "resizable" segments, where only one shared structure is allowed.
		 * This has to be implemented more cleanly.
		 */
		if (result->size != size)
		{
			Size delta = size - result->size;

			result->size = size;

			/* Reflect size change in the shared segment */
			SpinLockAcquire(Segments[shmem_segment].ShmemLock);
			Segments[shmem_segment].ShmemSegHdr->freeoffset += delta;
			SpinLockRelease(Segments[shmem_segment].ShmemLock);
		}

		structPtr = result->location;
	}
	else
	{
		Size		allocated_size;

		/* It isn't in the table yet. allocate and initialize it */
		structPtr = ShmemAllocRawInSegment(size, &allocated_size, shmem_segment);
		if (structPtr == NULL)
		{
			/* out of memory; remove the failed ShmemIndex entry */
			hash_search(ShmemIndex, name, HASH_REMOVE, NULL);
			LWLockRelease(ShmemIndexLock);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("not enough shared memory for data structure"
							" \"%s\" (%zu bytes requested)",
							name, size)));
		}
		result->size = size;
		result->allocated_size = allocated_size;
		result->location = structPtr;
	}

	LWLockRelease(ShmemIndexLock);

	Assert(ShmemAddrIsValidInSegment(structPtr, shmem_segment));

	Assert(structPtr == (void *) CACHELINEALIGN(structPtr));

	return structPtr;
}

/*
 * Add two Size values, checking for overflow
 */
Size
add_size(Size s1, Size s2)
{
	Size		result;

	result = s1 + s2;
	/* We are assuming Size is an unsigned type here... */
	if (result < s1 || result < s2)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested shared memory size overflows size_t")));
	return result;
}

/*
 * Multiply two Size values, checking for overflow
 */
Size
mul_size(Size s1, Size s2)
{
	Size		result;

	if (s1 == 0 || s2 == 0)
		return 0;
	result = s1 * s2;
	/* We are assuming Size is an unsigned type here... */
	if (result / s2 != s1)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested shared memory size overflows size_t")));
	return result;
}

/* SQL SRF showing allocated shared memory */
Datum
pg_get_shmem_allocations(PG_FUNCTION_ARGS)
{
#define PG_GET_SHMEM_SIZES_COLS 4
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS hstat;
	ShmemIndexEnt *ent;
	Size		named_allocated = 0;
	Datum		values[PG_GET_SHMEM_SIZES_COLS];
	bool		nulls[PG_GET_SHMEM_SIZES_COLS];

	InitMaterializedSRF(fcinfo, 0);

	LWLockAcquire(ShmemIndexLock, LW_SHARED);

	hash_seq_init(&hstat, ShmemIndex);

	/* output all allocated entries */
	memset(nulls, 0, sizeof(nulls));
	/* XXX: take all shared memory segments into account. */
	while ((ent = (ShmemIndexEnt *) hash_seq_search(&hstat)) != NULL)
	{
		values[0] = CStringGetTextDatum(ent->key);
		values[1] = Int64GetDatum((char *) ent->location - (char *) Segments[MAIN_SHMEM_SEGMENT].ShmemSegHdr);
		values[2] = Int64GetDatum(ent->size);
		values[3] = Int64GetDatum(ent->allocated_size);
		named_allocated += ent->allocated_size;

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	/* output shared memory allocated but not counted via the shmem index */
	values[0] = CStringGetTextDatum("<anonymous>");
	nulls[1] = true;
	values[2] = Int64GetDatum(Segments[MAIN_SHMEM_SEGMENT].ShmemSegHdr->freeoffset - named_allocated);
	values[3] = values[2];
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	/* output as-of-yet unused shared memory */
	nulls[0] = true;
	values[1] = Int64GetDatum(Segments[MAIN_SHMEM_SEGMENT].ShmemSegHdr->freeoffset);
	nulls[1] = false;
	values[2] = Int64GetDatum(Segments[MAIN_SHMEM_SEGMENT].ShmemSegHdr->totalsize - Segments[MAIN_SHMEM_SEGMENT].ShmemSegHdr->freeoffset);
	values[3] = values[2];
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	LWLockRelease(ShmemIndexLock);

	return (Datum) 0;
}
