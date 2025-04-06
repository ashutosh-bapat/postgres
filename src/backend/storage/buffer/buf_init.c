/*-------------------------------------------------------------------------
 *
 * buf_init.c
 *	  buffer manager initialization routines
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_init.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/aio.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/pg_shmem.h"

BufferDescPadded *BufferDescriptors;
char	   *BufferBlocks;
ConditionVariableMinimallyPadded *BufferIOCVArray;
WritebackContext BackendWritebackContext;
CkptSortItem *CkptBufferIds;

/*
 * Data Structures:
 *		buffers live in a freelist and a lookup data structure.
 *
 *
 * Buffer Lookup:
 *		Two important notes.  First, the buffer has to be
 *		available for lookup BEFORE an IO begins.  Otherwise
 *		a second process trying to read the buffer will
 *		allocate its own copy and the buffer pool will
 *		become inconsistent.
 *
 * Buffer Replacement:
 *		see freelist.c.  A buffer cannot be replaced while in
 *		use either by data manager or during IO.
 *
 *
 * Synchronization/Locking:
 *
 * IO_IN_PROGRESS -- this is a flag in the buffer descriptor.
 *		It must be set when an IO is initiated and cleared at
 *		the end of the IO.  It is there to make sure that one
 *		process doesn't start to use a buffer while another is
 *		faulting it in.  see WaitIO and related routines.
 *
 * refcount --	Counts the number of processes holding pins on a buffer.
 *		A buffer is pinned during IO and immediately after a BufferAlloc().
 *		Pins must be released before end of transaction.  For efficiency the
 *		shared refcount isn't increased if an individual backend pins a buffer
 *		multiple times. Check the PrivateRefCount infrastructure in bufmgr.c.
 */


/*
 * Initialize shared buffer pool
 *
 * This is called once during shared-memory initialization (either in the
 * postmaster, or in a standalone backend) or during shared-memory resize. Size
 * of data structures initialized here depends on NBuffers, and to be able to
 * change NBuffers without a restart we store each structure into a separate
 * shared memory segment, which could be resized on demand.
 *
 * FirstBufferToInit tells where to start initializing buffers. For
 * initialization it always will be zero, but when resizing shared-memory it
 * indicates the number of already initialized buffers.
 *
 * No locks are taking in this function, it is the caller responsibility to
 * make sure only one backend can work with new buffers.
 */
void
BufferManagerShmemInit(int FirstBufferToInit)
{
	bool		foundBufs,
				foundDescs,
				foundIOCV,
				foundBufCkpt;
	int			i;
	elog(DEBUG1, "BufferManagerShmemInit from %d to %d",
				 FirstBufferToInit, NBuffers);

	/* Align descriptors to a cacheline boundary. */
	BufferDescriptors = (BufferDescPadded *)
		ShmemInitStructInSegment("Buffer Descriptors",
						NBuffers * sizeof(BufferDescPadded),
						&foundDescs, BUFFER_DESCRIPTORS_SHMEM_SEGMENT);

	/* Align buffer pool on IO page size boundary. */
	BufferBlocks = (char *)
		TYPEALIGN(PG_IO_ALIGN_SIZE,
				  ShmemInitStructInSegment("Buffer Blocks",
								  NBuffers * (Size) BLCKSZ + PG_IO_ALIGN_SIZE,
								  &foundBufs, BUFFERS_SHMEM_SEGMENT));

	/* Align condition variables to cacheline boundary. */
	BufferIOCVArray = (ConditionVariableMinimallyPadded *)
		ShmemInitStructInSegment("Buffer IO Condition Variables",
						NBuffers * sizeof(ConditionVariableMinimallyPadded),
						&foundIOCV, BUFFER_IOCV_SHMEM_SEGMENT);

	/*
	 * The array used to sort to-be-checkpointed buffer ids is located in
	 * shared memory, to avoid having to allocate significant amounts of
	 * memory at runtime. As that'd be in the middle of a checkpoint, or when
	 * the checkpointer is restarted, memory allocation failures would be
	 * painful.
	 */
	CkptBufferIds = (CkptSortItem *)
		ShmemInitStructInSegment("Checkpoint BufferIds",
						NBuffers * sizeof(CkptSortItem), &foundBufCkpt,
						CHECKPOINT_BUFFERS_SHMEM_SEGMENT);

	if (foundDescs || foundBufs || foundIOCV || foundBufCkpt)
	{
		/* should find all of these, or none of them */
		Assert(foundDescs && foundBufs && foundIOCV && foundBufCkpt);
		/*
		 * note: this path is only taken in EXEC_BACKEND case when initializing
		 * shared memory, or in all cases when resizing shared memory.
		 */
	}

#ifndef EXEC_BACKEND
	/*
	 * Initialize all the buffer headers.
	 */
	for (i = FirstBufferToInit; i < NBuffers; i++)
	{
		BufferDesc *buf = GetBufferDescriptor(i);

		ClearBufferTag(&buf->tag);

		pg_atomic_init_u32(&buf->state, 0);
		buf->wait_backend_pgprocno = INVALID_PROC_NUMBER;

		buf->buf_id = i;

		pgaio_wref_clear(&buf->io_wref);

		/*
		 * Initially link all the buffers together as unused. Subsequent
		 * management of this list is done by freelist.c.
		 */
		buf->freeNext = i + 1;

		LWLockInitialize(BufferDescriptorGetContentLock(buf),
						 LWTRANCHE_BUFFER_CONTENT);

		ConditionVariableInit(BufferDescriptorGetIOCV(buf));
	}
#endif

	/* Correct last entry of linked list */
	GetBufferDescriptor(NBuffers - 1)->freeNext = FREENEXT_END_OF_LIST;

	/* Init other shared buffer-management stuff */
	StrategyInitialize(!foundDescs);

	/* Initialize per-backend file flush context */
	WritebackContextInit(&BackendWritebackContext,
						 &backend_flush_after);
}

/*
 * BufferManagerShmemSize
 *
 * compute the size of shared memory for the buffer pool including
 * data pages, buffer descriptors, hash tables, etc. based on the
 * shared memory segment. The main segment must not allocate anything
 * related to buffers, every other segment will receive part of the
 * data.
 */
Size
BufferManagerShmemSize(int shmem_segment)
{
	Size		size = 0;

	if (shmem_segment == MAIN_SHMEM_SEGMENT)
		return size;

	if (shmem_segment == BUFFER_DESCRIPTORS_SHMEM_SEGMENT)
	{
		/* size of buffer descriptors */
		size = add_size(size, mul_size(NBuffers, sizeof(BufferDescPadded)));
		/* to allow aligning buffer descriptors */
		size = add_size(size, PG_CACHE_LINE_SIZE);
	}

	if (shmem_segment == BUFFERS_SHMEM_SEGMENT)
	{
		/* size of data pages, plus alignment padding */
		size = add_size(size, PG_IO_ALIGN_SIZE);
		size = add_size(size, mul_size(NBuffers, BLCKSZ));
	}

	if (shmem_segment == STRATEGY_SHMEM_SEGMENT)
	{
		/* size of stuff controlled by freelist.c */
		size = add_size(size, StrategyShmemSize());
	}

	if (shmem_segment == BUFFER_IOCV_SHMEM_SEGMENT)
	{
		/* size of I/O condition variables */
		size = add_size(size, mul_size(NBuffers,
									   sizeof(ConditionVariableMinimallyPadded)));
		/* to allow aligning the above */
		size = add_size(size, PG_CACHE_LINE_SIZE);
	}

	if (shmem_segment == CHECKPOINT_BUFFERS_SHMEM_SEGMENT)
	{
		/* size of checkpoint sort array in bufmgr.c */
		size = add_size(size, mul_size(NBuffers, sizeof(CkptSortItem)));
	}

	return size;
}
