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

#include "storage/buf_internals.h"
#include "storage/bufmgr.h"

BufferDescPadded *BufferDescriptors;
char	   *BufferBlocks;
ConditionVariableMinimallyPadded *BufferIOCVArray;
WritebackContext BackendWritebackContext;
CkptSortItem *CkptBufferIds;

/*
 * Currently broadcasted value of NBuffers in shared memory.
 *
 * Most of the time this value is going to be equal to NBuffers. But if
 * postmaster is resizing shared memory and a new backend was created
 * at the same time, there is a possibility for the new backend to inherit the
 * old NBuffers value, but miss the resize signal if ProcSignal infrastructure
 * was not initialized yet. Consider this situation:
 *
 *     Postmaster ------> New Backend
 *         |                   |
 *         |                Launch
 *         |                   |
 *         |             Inherit NBuffers
 *         |                   |
 *     Resize NBuffers         |
 *         |                   |
 *     Emit Barrier            |
 *         |            Init ProcSignal
 *         |                   |
 *     Finish resize           |
 *         |                   |
 *     New NBuffers       Old NBuffers
 *
 * In this case the backend is not yet ready to receive a signal from
 * EmitProcSignalBarrier, and will be ignored. The same happens if ProcSignal
 * is initialized even later, after the resizing was finished.
 *
 * To address resulting inconsistency, postmaster broadcasts the current
 * NBuffers value via shared memory. Every new backend has to verify this value
 * before it will access the buffer pool: if it differs from its own value,
 * this indicates a shared memory resize has happened and the backend has to
 * first synchronize with rest of the pack.
 */
ShmemControl *ShmemCtrl = NULL;

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
 * postmaster, or in a standalone backend). Size of data structures initialized
 * here depends on NBuffers, and to be able to change NBuffers without a
 * restart we store each structure into a separate shared memory segment, which
 * could be resized on demand.
 */
void
BufferManagerShmemInit(void)
{
	bool		foundBufs,
				foundDescs,
				foundIOCV,
				foundBufCkpt,
				foundShmemCtrl;

	ShmemCtrl = (ShmemControl *)
		ShmemInitStruct("Shmem Control", sizeof(ShmemControl),
						&foundShmemCtrl);

	if (!foundShmemCtrl)
	{
		/* Initialize with the currently known value */
		pg_atomic_init_u32(&ShmemCtrl->NSharedBuffers, NBuffers);
		BarrierInit(&ShmemCtrl->Barrier, 0);
	}

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
		/* note: this path is only taken in EXEC_BACKEND case */
	}
	else
	{
		int			i;

		/*
		 * Initialize all the buffer headers.
		 */
		for (i = 0; i < NBuffers; i++)
		{
			BufferDesc *buf = GetBufferDescriptor(i);

			ClearBufferTag(&buf->tag);

			pg_atomic_init_u32(&buf->state, 0);
			buf->wait_backend_pgprocno = INVALID_PROC_NUMBER;

			buf->buf_id = i;

			/*
			 * Initially link all the buffers together as unused. Subsequent
			 * management of this list is done by freelist.c.
			 */
			buf->freeNext = i + 1;

			LWLockInitialize(BufferDescriptorGetContentLock(buf),
							 LWTRANCHE_BUFFER_CONTENT);

			ConditionVariableInit(BufferDescriptorGetIOCV(buf));
		}

		/* Correct last entry of linked list */
		GetBufferDescriptor(NBuffers - 1)->freeNext = FREENEXT_END_OF_LIST;
	}

	/* Init other shared buffer-management stuff */
	StrategyInitialize(!foundDescs);

	/* Initialize per-backend file flush context */
	WritebackContextInit(&BackendWritebackContext,
						 &backend_flush_after);
}

/*
 * Reinitialize shared memory structures, which size depends on NBuffers. It's
 * similar to InitBufferPool, but applied only to the buffers in the range
 * between NBuffersOld and NBuffers.
 *
 * NBuffersOld tells what was the original value of NBuffersOld. It will be
 * used to identify new and not yet initialized buffers.
 *
 * initNew flag indicates that the caller wants new buffers to be initialized.
 * No locks are taking in this function, it is the caller responsibility to
 * make sure only one backend can work with new buffers.
 */
void
ResizeBufferPool(int NBuffersOld, bool initNew)
{
	bool		foundBufs,
				foundDescs,
				foundIOCV,
				foundBufCkpt;
	int			i;
	elog(DEBUG1, "Resizing buffer pool from %d to %d", NBuffersOld, NBuffers);

	/* XXX: Only increasing of shared_buffers is supported in this function */
	if(NBuffersOld > NBuffers)
		return;

	/* Align descriptors to a cacheline boundary. */
	BufferDescriptors = (BufferDescPadded *)
		ShmemInitStructInSegment("Buffer Descriptors",
						NBuffers * sizeof(BufferDescPadded),
						&foundDescs, BUFFER_DESCRIPTORS_SHMEM_SEGMENT);

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

	/* Align buffer pool on IO page size boundary. */
	BufferBlocks = (char *)
		TYPEALIGN(PG_IO_ALIGN_SIZE,
				  ShmemInitStructInSegment("Buffer Blocks",
								  NBuffers * (Size) BLCKSZ + PG_IO_ALIGN_SIZE,
								  &foundBufs, BUFFERS_SHMEM_SEGMENT));

	/*
	 * It's enough to only resize shmem structures, if some other backend will
	 * do initialization of new buffers for us.
	 */
	if (!initNew)
		return;

	elog(DEBUG1, "Initialize new buffers");

	/*
	 * Initialize the headers for new buffers.
	 */
	for (i = NBuffersOld; i < NBuffers; i++)
	{
		BufferDesc *buf = GetBufferDescriptor(i);

		ClearBufferTag(&buf->tag);

		pg_atomic_init_u32(&buf->state, 0);
		buf->wait_backend_pgprocno = INVALID_PROC_NUMBER;

		buf->buf_id = i;

		/*
		 * Initially link all the buffers together as unused. Subsequent
		 * management of this list is done by freelist.c.
		 */
		buf->freeNext = i + 1;

		LWLockInitialize(BufferDescriptorGetContentLock(buf),
						 LWTRANCHE_BUFFER_CONTENT);

		ConditionVariableInit(BufferDescriptorGetIOCV(buf));
	}

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
