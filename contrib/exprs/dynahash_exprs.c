/*
 * contrib/exprs/palloc_expr.c
 *
 * Module to experiment with palloc and pfree.
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/buf_internals.h"
#include "utils/memutils.h"
#include "exprs.h"
#include "utils/hsearch.h"

PG_FUNCTION_INFO_V1(sparse_hashes);

Datum
sparse_hashes(PG_FUNCTION_ARGS)
{
    int num_entries = PG_GETARG_INT32(0);
    int max_entries = PG_GETARG_INT32(1);
    static HTAB *hashtab;
	HASHCTL		info;
    BufferTag tag;
	instr_time	starttime;
	double		inserttime = 0;
	double		searchtime = 0;
	double		deletetime = 0;
    bool        found;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
    Datum       values[3];
    bool        nulls[3];

    typedef struct
    {
    	BufferTag	key;			/* Tag of a disk page */
        int			id;				/* Associated buffer ID */
    } BufferLookupEnt;

	/* Create a hash table, BufferTag maps to Buffer */
	info.keysize = sizeof(BufferTag);
	info.entrysize = sizeof(BufferLookupEnt);
	info.num_partitions = NUM_BUFFER_PARTITIONS;

	hashtab = hash_create("experimental buffer hash table", max_entries, &info,
								  HASH_ELEM | HASH_BLOBS);

    /* Except page numbers everything else to random value. */
    tag.dbOid = 16348;
    tag.relNumber = 16349;
    tag.spcOid = 16350;
    tag.forkNum = 1;

	INSTR_TIME_SET_CURRENT(starttime);
    for (tag.blockNum = 0; tag.blockNum < num_entries; tag.blockNum++)
    {
        BufferLookupEnt *entry = hash_search(hashtab, &tag, HASH_ENTER, &found);
        entry->id = tag.blockNum;
        Assert(!found);
    }
	inserttime += elapsed_time(&starttime);

	INSTR_TIME_SET_CURRENT(starttime);
    for (tag.blockNum = 0; tag.blockNum < num_entries; tag.blockNum++)
    {
       hash_search(hashtab, &tag, HASH_FIND, &found);
       Assert(found);
    }
	searchtime += elapsed_time(&starttime);

	INSTR_TIME_SET_CURRENT(starttime);
    for (tag.blockNum = 0; tag.blockNum < num_entries; tag.blockNum++)
    {
       hash_search(hashtab, &tag, HASH_REMOVE, &found);
       Assert(found);
    }
	deletetime += elapsed_time(&starttime);

    hash_destroy(hashtab);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

    values[0] = Float8GetDatum(inserttime);
    values[1] = Float8GetDatum(searchtime);
    values[2] = Float8GetDatum(deletetime);
    memset(nulls, 0, sizeof(nulls));
    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}