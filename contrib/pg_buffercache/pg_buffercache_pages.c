/*-------------------------------------------------------------------------
 *
 * pg_buffercache_pages.c
 *	  display some contents of the buffer cache
 *
 *	  contrib/pg_buffercache/pg_buffercache_pages.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "port/pg_numa.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"


#define NUM_BUFFERCACHE_PAGES_MIN_ELEM	8
#define NUM_BUFFERCACHE_PAGES_ELEM	9
#define NUM_BUFFERCACHE_SUMMARY_ELEM 5
#define NUM_BUFFERCACHE_USAGE_COUNTS_ELEM 4
#define NUM_BUFFERCACHE_EVICT_ELEM 2
#define NUM_BUFFERCACHE_EVICT_RELATION_ELEM 3
#define NUM_BUFFERCACHE_EVICT_ALL_ELEM 3
#define NUM_BUFFERCACHE_MARK_DIRTY_ELEM 2
#define NUM_BUFFERCACHE_MARK_DIRTY_RELATION_ELEM 3
#define NUM_BUFFERCACHE_MARK_DIRTY_ALL_ELEM 3

#define NUM_BUFFERCACHE_OS_PAGES_ELEM	3

/*
 * Upper bound on OS pages a single BLCKSZ buffer can overlap.  With BLCKSZ
 * up to 32 KB and os_page_size at least 4 KB that's at most 9; 16 is a safe
 * cap so the per-iteration scratch arrays fit on the stack.
 */
#define MAX_PAGES_PER_BUFFER	16

PG_MODULE_MAGIC_EXT(
					.name = "pg_buffercache",
					.version = PG_VERSION
);

static TupleDesc build_buffercache_pages_tupledesc(int natts);


/*
 * Function returning data from the shared buffer cache - buffer number,
 * relation node/tablespace/database/blocknum and dirty indicator.
 */
PG_FUNCTION_INFO_V1(pg_buffercache_pages);
PG_FUNCTION_INFO_V1(pg_buffercache_os_pages);
PG_FUNCTION_INFO_V1(pg_buffercache_numa_pages);
PG_FUNCTION_INFO_V1(pg_buffercache_summary);
PG_FUNCTION_INFO_V1(pg_buffercache_usage_counts);
PG_FUNCTION_INFO_V1(pg_buffercache_evict);
PG_FUNCTION_INFO_V1(pg_buffercache_evict_relation);
PG_FUNCTION_INFO_V1(pg_buffercache_evict_all);
PG_FUNCTION_INFO_V1(pg_buffercache_mark_dirty);
PG_FUNCTION_INFO_V1(pg_buffercache_mark_dirty_relation);
PG_FUNCTION_INFO_V1(pg_buffercache_mark_dirty_all);
PG_FUNCTION_INFO_V1(pg_buffercache_lookup_table_entries);


/* Only need to touch memory once per backend process lifetime */
static bool firstNumaTouch = true;


Datum
pg_buffercache_pages(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	expected_tupledesc;
	TupleDesc	actual_tupledesc;
	MemoryContext oldcontext;
	int			i;

	/*
	 * To smoothly support upgrades from version 1.0 of this extension
	 * transparently handle the (non-)existence of the pinning_backends
	 * column. We unfortunately have to get the result type for that... - we
	 * can't use the result type determined by the function definition without
	 * potentially crashing when somebody uses the old (or even wrong)
	 * function definition though.
	 */
	if (get_call_result_type(fcinfo, NULL, &expected_tupledesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if (expected_tupledesc->natts < NUM_BUFFERCACHE_PAGES_MIN_ELEM ||
		expected_tupledesc->natts > NUM_BUFFERCACHE_PAGES_ELEM)
		elog(ERROR, "incorrect number of output arguments");

	InitMaterializedSRF(fcinfo, 0);

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	actual_tupledesc = build_buffercache_pages_tupledesc(expected_tupledesc->natts);
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Override the caller-supplied descriptor with the tuple descriptor that
	 * matches the values we actually return, so executor-side
	 * tupledesc_match() can verify the caller's row definition.
	 *
	 * Do not free the previous rsinfo->setDesc here: for RECORD results it
	 * can alias rsinfo->expectedDesc, which the executor still needs to
	 * reference.
	 */
	rsinfo->setDesc = actual_tupledesc;

	/*
	 * Scan through all the buffers, adding one row for each of the buffers to
	 * the tuplestore.
	 *
	 * We don't hold the partition locks, so we don't get a consistent
	 * snapshot across all buffers, but we do grab the buffer header locks, so
	 * the information of each buffer is self-consistent.
	 */
	for (i = 0; i < NBuffers; i++)
	{
		BufferDesc *bufHdr;
		uint64		buf_state;
		uint32		bufferid;
		RelFileNumber relfilenumber;
		Oid			reltablespace;
		Oid			reldatabase;
		ForkNumber	forknum;
		BlockNumber blocknum;
		bool		isvalid;
		bool		isdirty;
		uint16		usagecount;
		int32		pinning_backends;
		Datum		values[NUM_BUFFERCACHE_PAGES_ELEM];
		bool		nulls[NUM_BUFFERCACHE_PAGES_ELEM];

		bufHdr = GetBufferDescriptor(i);
		/* Lock each buffer header before inspecting. */
		buf_state = LockBufHdr(bufHdr);

		bufferid = BufferDescriptorGetBuffer(bufHdr);
		relfilenumber = BufTagGetRelNumber(&bufHdr->tag);
		reltablespace = bufHdr->tag.spcOid;
		reldatabase = bufHdr->tag.dbOid;
		forknum = BufTagGetForkNum(&bufHdr->tag);
		blocknum = bufHdr->tag.blockNum;
		usagecount = BUF_STATE_GET_USAGECOUNT(buf_state);
		pinning_backends = BUF_STATE_GET_REFCOUNT(buf_state);

		if (buf_state & BM_DIRTY)
			isdirty = true;
		else
			isdirty = false;

		/* Note if the buffer is valid, and has storage created */
		if ((buf_state & BM_VALID) && (buf_state & BM_TAG_VALID))
			isvalid = true;
		else
			isvalid = false;

		UnlockBufHdr(bufHdr);

		/* Build the tuple and add it to tuplestore */
		values[0] = Int32GetDatum(bufferid);
		nulls[0] = false;

		/*
		 * Set all fields except the bufferid to null if the buffer is unused
		 * or not valid.
		 */
		if (blocknum == InvalidBlockNumber || isvalid == false)
		{
			nulls[1] = true;
			nulls[2] = true;
			nulls[3] = true;
			nulls[4] = true;
			nulls[5] = true;
			nulls[6] = true;
			nulls[7] = true;
			/* unused for v1.0 callers, but the array is always long enough */
			nulls[8] = true;
		}
		else
		{
			values[1] = ObjectIdGetDatum(relfilenumber);
			nulls[1] = false;
			values[2] = ObjectIdGetDatum(reltablespace);
			nulls[2] = false;
			values[3] = ObjectIdGetDatum(reldatabase);
			nulls[3] = false;
			values[4] = Int16GetDatum(forknum);
			nulls[4] = false;
			values[5] = Int64GetDatum((int64) blocknum);
			nulls[5] = false;
			values[6] = BoolGetDatum(isdirty);
			nulls[6] = false;
			values[7] = Int16GetDatum(usagecount);
			nulls[7] = false;
			/* unused for v1.0 callers, but the array is always long enough */
			values[8] = Int32GetDatum(pinning_backends);
			nulls[8] = false;
		}

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

		/*
		 * Check for interrupts here, at the end of the loop, so that the buffer
		 * index i remains valid till the next iteration.
		 */
		CHECK_FOR_INTERRUPTS();
	}

	return (Datum) 0;
}

static TupleDesc
build_buffercache_pages_tupledesc(int natts)
{
	TupleDesc	tupledesc;

	tupledesc = CreateTemplateTupleDesc(natts);
	TupleDescInitEntry(tupledesc, (AttrNumber) 1, "bufferid",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 2, "relfilenode",
					   OIDOID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 3, "reltablespace",
					   OIDOID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 4, "reldatabase",
					   OIDOID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 5, "relforknumber",
					   INT2OID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 6, "relblocknumber",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 7, "isdirty",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupledesc, (AttrNumber) 8, "usagecount",
					   INT2OID, -1, 0);

	if (natts == NUM_BUFFERCACHE_PAGES_ELEM)
		TupleDescInitEntry(tupledesc, (AttrNumber) 9, "pinning_backends",
						   INT4OID, -1, 0);

	TupleDescFinalize(tupledesc);

	return BlessTupleDesc(tupledesc);
}

/*
 * Inquire about OS pages mappings for shared buffers, with NUMA information,
 * optionally.
 *
 * When "include_numa" is false, this routines ignores everything related
 * to NUMA (returned as NULL values), returning mapping information between
 * shared buffers and OS pages.
 *
 * When "include_numa" is true, NUMA is initialized and numa_node values
 * are generated.  In order to get reliable results we also need to touch
 * memory pages, so that the inquiry about NUMA memory node does not return
 * -2, indicating unmapped/unallocated pages.
 *
 * Buffers may be smaller or larger than OS memory pages. For each buffer we
 * return one entry for each memory page used by the buffer (if the buffer is
 * smaller, it only uses a part of one memory page).
 *
 * We expect both sizes (for buffers and memory pages) to be a power-of-2, so
 * one is always a multiple of the other.
 *
 */
static Datum
pg_buffercache_os_pages_internal(FunctionCallInfo fcinfo, bool include_numa)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Size		os_page_size;
	void	   *os_page_ptrs[MAX_PAGES_PER_BUFFER];
	int			os_page_status[MAX_PAGES_PER_BUFFER];
	Datum		values[NUM_BUFFERCACHE_OS_PAGES_ELEM];
	bool		nulls[NUM_BUFFERCACHE_OS_PAGES_ELEM];
	char	   *startptr;
	int			i;

	InitMaterializedSRF(fcinfo, 0);

	if (include_numa && pg_numa_init() == -1)
		elog(ERROR, "libnuma initialization failed or NUMA is not supported on this platform");

	os_page_size = pg_get_shmem_pagesize();

	Assert((os_page_size % BLCKSZ == 0) || (BLCKSZ % os_page_size == 0));
	Assert(Max(1, BLCKSZ / os_page_size) + 1 <= MAX_PAGES_PER_BUFFER);

	if (include_numa && firstNumaTouch)
		elog(DEBUG1, "NUMA: page-faulting the buffercache for proper NUMA readouts");

	startptr = (char *) TYPEALIGN_DOWN(os_page_size,
									   (char *) BufferGetBlock(1));

	/*
	 * We don't hold the partition locks, so we don't get a consistent
	 * snapshot across all buffers, but we do grab the buffer header locks,
	 * so the information of each buffer is self-consistent.
	 */
	for (i = 0; i < NBuffers; i++)
	{
		char	   *buffptr = (char *) BufferGetBlock(i + 1);
		char	   *startptr_buff = (char *) TYPEALIGN_DOWN(os_page_size,
															buffptr);
		char	   *endptr_buff = buffptr + BLCKSZ;
		BufferDesc *bufHdr;
		uint32		bufferid;
		int32		page_num;
		int			num_pages_this_buffer = 0;
		int			j;
		char	   *ptr;

		bufHdr = GetBufferDescriptor(i);
		LockBufHdr(bufHdr);
		bufferid = BufferDescriptorGetBuffer(bufHdr);
		UnlockBufHdr(bufHdr);

		page_num = (startptr_buff - startptr) / os_page_size;

		for (ptr = startptr_buff; ptr < endptr_buff; ptr += os_page_size)
		{
			os_page_ptrs[num_pages_this_buffer++] = ptr;

			/* Only need to touch memory once per backend process lifetime */
			if (include_numa && firstNumaTouch)
				pg_numa_touch_mem_if_required(ptr);
		}

		if (include_numa)
		{
			memset(os_page_status, 0xff, sizeof(int) * num_pages_this_buffer);
			if (pg_numa_query_pages(0, num_pages_this_buffer,
									os_page_ptrs, os_page_status) == -1)
				elog(ERROR, "failed NUMA pages inquiry: %m");
		}

		values[0] = Int32GetDatum(bufferid);
		nulls[0] = false;
		nulls[1] = false;

		for (j = 0; j < num_pages_this_buffer; j++)
		{
			values[1] = Int64GetDatum(page_num++);

			if (include_numa && os_page_status[j] >= 0)
			{
				values[2] = Int32GetDatum(os_page_status[j]);
				nulls[2] = false;
			}
			else
			{
				values[2] = (Datum) 0;
				nulls[2] = true;
			}

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								 values, nulls);
		}

		/*
		 * Check for interrupts here, at the end of the loop, so that the
		 * buffer index i remains valid till the next iteration.
		 */
		CHECK_FOR_INTERRUPTS();
	}

	if (include_numa)
		firstNumaTouch = false;

	return (Datum) 0;
}

/*
 * pg_buffercache_os_pages
 *
 * Retrieve information about OS pages, with or without NUMA information.
 */
Datum
pg_buffercache_os_pages(PG_FUNCTION_ARGS)
{
	bool		include_numa;

	/* Get the boolean parameter that controls the NUMA behavior. */
	include_numa = PG_GETARG_BOOL(0);

	return pg_buffercache_os_pages_internal(fcinfo, include_numa);
}

/* Backward-compatible wrapper for v1.6. */
Datum
pg_buffercache_numa_pages(PG_FUNCTION_ARGS)
{
	/* Call internal function with include_numa=true */
	return pg_buffercache_os_pages_internal(fcinfo, true);
}

Datum
pg_buffercache_summary(PG_FUNCTION_ARGS)
{
	Datum		result;
	TupleDesc	tupledesc;
	HeapTuple	tuple;
	Datum		values[NUM_BUFFERCACHE_SUMMARY_ELEM];
	bool		nulls[NUM_BUFFERCACHE_SUMMARY_ELEM];

	int32		buffers_used = 0;
	int32		buffers_unused = 0;
	int32		buffers_dirty = 0;
	int32		buffers_pinned = 0;
	int64		usagecount_total = 0;

	if (get_call_result_type(fcinfo, NULL, &tupledesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	for (int i = 0; i < NBuffers; i++)
	{
		BufferDesc *bufHdr;
		uint64		buf_state;

		/*
		 * This function summarizes the state of all headers. Locking the
		 * buffer headers wouldn't provide an improved result as the state of
		 * the buffer can still change after we release the lock and it'd
		 * noticeably increase the cost of the function.
		 */
		bufHdr = GetBufferDescriptor(i);
		buf_state = pg_atomic_read_u64(&bufHdr->state);

		if (buf_state & BM_VALID)
		{
			buffers_used++;
			usagecount_total += BUF_STATE_GET_USAGECOUNT(buf_state);

			if (buf_state & BM_DIRTY)
				buffers_dirty++;
		}
		else
			buffers_unused++;

		if (BUF_STATE_GET_REFCOUNT(buf_state) > 0)
			buffers_pinned++;

		/*
		 * Check for interrupts here, at the end of the loop, so that the buffer
		 * index i remains valid till the next iteration.
		 */
		CHECK_FOR_INTERRUPTS();
	}

	memset(nulls, 0, sizeof(nulls));
	values[0] = Int32GetDatum(buffers_used);
	values[1] = Int32GetDatum(buffers_unused);
	values[2] = Int32GetDatum(buffers_dirty);
	values[3] = Int32GetDatum(buffers_pinned);

	if (buffers_used != 0)
		values[4] = Float8GetDatum((double) usagecount_total / buffers_used);
	else
		nulls[4] = true;

	/* Build and return the tuple. */
	tuple = heap_form_tuple(tupledesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

Datum
pg_buffercache_usage_counts(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			usage_counts[BM_MAX_USAGE_COUNT + 1] = {0};
	int			dirty[BM_MAX_USAGE_COUNT + 1] = {0};
	int			pinned[BM_MAX_USAGE_COUNT + 1] = {0};
	Datum		values[NUM_BUFFERCACHE_USAGE_COUNTS_ELEM];
	bool		nulls[NUM_BUFFERCACHE_USAGE_COUNTS_ELEM] = {0};

	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < NBuffers; i++)
	{
		BufferDesc *bufHdr = GetBufferDescriptor(i);
		uint64		buf_state = pg_atomic_read_u64(&bufHdr->state);
		int			usage_count;

		CHECK_FOR_INTERRUPTS();

		usage_count = BUF_STATE_GET_USAGECOUNT(buf_state);
		usage_counts[usage_count]++;

		if (buf_state & BM_DIRTY)
			dirty[usage_count]++;

		if (BUF_STATE_GET_REFCOUNT(buf_state) > 0)
			pinned[usage_count]++;
	}

	for (int i = 0; i < BM_MAX_USAGE_COUNT + 1; i++)
	{
		values[0] = Int32GetDatum(i);
		values[1] = Int32GetDatum(usage_counts[i]);
		values[2] = Int32GetDatum(dirty[i]);
		values[3] = Int32GetDatum(pinned[i]);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}

/*
 * Helper function to check if the user has superuser privileges.
 */
static void
pg_buffercache_superuser_check(char *func_name)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use %s()",
						func_name)));
}

/*
 * Try to evict a shared buffer.
 */
Datum
pg_buffercache_evict(PG_FUNCTION_ARGS)
{
	Datum		result;
	TupleDesc	tupledesc;
	HeapTuple	tuple;
	Datum		values[NUM_BUFFERCACHE_EVICT_ELEM];
	bool		nulls[NUM_BUFFERCACHE_EVICT_ELEM] = {0};

	Buffer		buf = PG_GETARG_INT32(0);
	bool		buffer_flushed;

	if (get_call_result_type(fcinfo, NULL, &tupledesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	pg_buffercache_superuser_check("pg_buffercache_evict");

	if (buf < 1 || buf > NBuffers)
		elog(ERROR, "bad buffer ID: %d", buf);

	values[0] = BoolGetDatum(EvictUnpinnedBuffer(buf, &buffer_flushed));
	values[1] = BoolGetDatum(buffer_flushed);

	tuple = heap_form_tuple(tupledesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

/*
 * Try to evict specified relation.
 */
Datum
pg_buffercache_evict_relation(PG_FUNCTION_ARGS)
{
	Datum		result;
	TupleDesc	tupledesc;
	HeapTuple	tuple;
	Datum		values[NUM_BUFFERCACHE_EVICT_RELATION_ELEM];
	bool		nulls[NUM_BUFFERCACHE_EVICT_RELATION_ELEM] = {0};

	Oid			relOid;
	Relation	rel;

	int32		buffers_evicted = 0;
	int32		buffers_flushed = 0;
	int32		buffers_skipped = 0;

	if (get_call_result_type(fcinfo, NULL, &tupledesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	pg_buffercache_superuser_check("pg_buffercache_evict_relation");

	relOid = PG_GETARG_OID(0);

	rel = relation_open(relOid, AccessShareLock);

	if (RelationUsesLocalBuffers(rel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation uses local buffers, %s() is intended to be used for shared buffers only",
						"pg_buffercache_evict_relation")));

	EvictRelUnpinnedBuffers(rel, &buffers_evicted, &buffers_flushed,
							&buffers_skipped);

	relation_close(rel, AccessShareLock);

	values[0] = Int32GetDatum(buffers_evicted);
	values[1] = Int32GetDatum(buffers_flushed);
	values[2] = Int32GetDatum(buffers_skipped);

	tuple = heap_form_tuple(tupledesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}


/*
 * Try to evict all shared buffers.
 */
Datum
pg_buffercache_evict_all(PG_FUNCTION_ARGS)
{
	Datum		result;
	TupleDesc	tupledesc;
	HeapTuple	tuple;
	Datum		values[NUM_BUFFERCACHE_EVICT_ALL_ELEM];
	bool		nulls[NUM_BUFFERCACHE_EVICT_ALL_ELEM] = {0};

	int32		buffers_evicted = 0;
	int32		buffers_flushed = 0;
	int32		buffers_skipped = 0;

	if (get_call_result_type(fcinfo, NULL, &tupledesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	pg_buffercache_superuser_check("pg_buffercache_evict_all");

	EvictAllUnpinnedBuffers(&buffers_evicted, &buffers_flushed,
							&buffers_skipped);

	values[0] = Int32GetDatum(buffers_evicted);
	values[1] = Int32GetDatum(buffers_flushed);
	values[2] = Int32GetDatum(buffers_skipped);

	tuple = heap_form_tuple(tupledesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

/*
 * Try to mark a shared buffer as dirty.
 */
Datum
pg_buffercache_mark_dirty(PG_FUNCTION_ARGS)
{

	Datum		result;
	TupleDesc	tupledesc;
	HeapTuple	tuple;
	Datum		values[NUM_BUFFERCACHE_MARK_DIRTY_ELEM];
	bool		nulls[NUM_BUFFERCACHE_MARK_DIRTY_ELEM] = {0};

	Buffer		buf = PG_GETARG_INT32(0);
	bool		buffer_already_dirty;

	if (get_call_result_type(fcinfo, NULL, &tupledesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	pg_buffercache_superuser_check("pg_buffercache_mark_dirty");

	if (buf < 1 || buf > NBuffers)
		elog(ERROR, "bad buffer ID: %d", buf);

	values[0] = BoolGetDatum(MarkDirtyUnpinnedBuffer(buf, &buffer_already_dirty));
	values[1] = BoolGetDatum(buffer_already_dirty);

	tuple = heap_form_tuple(tupledesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

/*
 * Try to mark all the shared buffers of a relation as dirty.
 */
Datum
pg_buffercache_mark_dirty_relation(PG_FUNCTION_ARGS)
{
	Datum		result;
	TupleDesc	tupledesc;
	HeapTuple	tuple;
	Datum		values[NUM_BUFFERCACHE_MARK_DIRTY_RELATION_ELEM];
	bool		nulls[NUM_BUFFERCACHE_MARK_DIRTY_RELATION_ELEM] = {0};

	Oid			relOid;
	Relation	rel;

	int32		buffers_already_dirty = 0;
	int32		buffers_dirtied = 0;
	int32		buffers_skipped = 0;

	if (get_call_result_type(fcinfo, NULL, &tupledesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	pg_buffercache_superuser_check("pg_buffercache_mark_dirty_relation");

	relOid = PG_GETARG_OID(0);

	rel = relation_open(relOid, AccessShareLock);

	if (RelationUsesLocalBuffers(rel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation uses local buffers, %s() is intended to be used for shared buffers only",
						"pg_buffercache_mark_dirty_relation")));

	MarkDirtyRelUnpinnedBuffers(rel, &buffers_dirtied, &buffers_already_dirty,
								&buffers_skipped);

	relation_close(rel, AccessShareLock);

	values[0] = Int32GetDatum(buffers_dirtied);
	values[1] = Int32GetDatum(buffers_already_dirty);
	values[2] = Int32GetDatum(buffers_skipped);

	tuple = heap_form_tuple(tupledesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

/*
 * Try to mark all the shared buffers as dirty.
 */
Datum
pg_buffercache_mark_dirty_all(PG_FUNCTION_ARGS)
{
	Datum		result;
	TupleDesc	tupledesc;
	HeapTuple	tuple;
	Datum		values[NUM_BUFFERCACHE_MARK_DIRTY_ALL_ELEM];
	bool		nulls[NUM_BUFFERCACHE_MARK_DIRTY_ALL_ELEM] = {0};

	int32		buffers_already_dirty = 0;
	int32		buffers_dirtied = 0;
	int32		buffers_skipped = 0;

	if (get_call_result_type(fcinfo, NULL, &tupledesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	pg_buffercache_superuser_check("pg_buffercache_mark_dirty_all");

	MarkDirtyAllUnpinnedBuffers(&buffers_dirtied, &buffers_already_dirty,
								&buffers_skipped);

	values[0] = Int32GetDatum(buffers_dirtied);
	values[1] = Int32GetDatum(buffers_already_dirty);
	values[2] = Int32GetDatum(buffers_skipped);

	tuple = heap_form_tuple(tupledesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

/*
 * Return lookup table content as a set of records.
 */
Datum
pg_buffercache_lookup_table_entries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	/* Fill the tuplestore */
	BufTableGetContents(rsinfo->setResult, rsinfo->setDesc);

	return (Datum) 0;
}
