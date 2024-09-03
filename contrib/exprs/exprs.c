/*
 * contrib/expr.c
 *
 * Experimental module implementation.
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "portability/instr_time.h"
#include "utils/array.h"

PG_MODULE_MAGIC;

/* Compute elapsed time in milliseconds since given timestamp */
static double
elapsed_time(instr_time *starttime)
{
	instr_time	endtime;

	INSTR_TIME_SET_CURRENT(endtime);
	INSTR_TIME_SUBTRACT(endtime, *starttime);
	return INSTR_TIME_GET_MILLISEC(endtime);
}


PG_FUNCTION_INFO_V1(palloc_pfree);

Datum
palloc_pfree(PG_FUNCTION_ARGS)
{
	int num = PG_GETARG_INT32(0);
	int size = PG_GETARG_INT32(1);
	int i;
	instr_time	starttime;
	double		totaltime = 0;

	INSTR_TIME_SET_CURRENT(starttime);
	for (i = 0; i < num; i++)
	{
		char *ptr = palloc(size);
		ptr[0] = 0;
		pfree(ptr);
	}
	totaltime += elapsed_time(&starttime);

	PG_RETURN_FLOAT8(totaltime);
}

PG_FUNCTION_INFO_V1(mem_context_free);

Datum
mem_context_free(PG_FUNCTION_ARGS)
{
	int num = PG_GETARG_INT32(0);
	int size = PG_GETARG_INT32(1);
	int i;
	instr_time	starttime;
	double		totaltime = 0;

	INSTR_TIME_SET_CURRENT(starttime);
	/* Don't pfree. Let the memory context free free up the memory. */
	for (i = 0; i < num; i++)
	{
		char *ptr = palloc(size);
		ptr[0] = 0;
	}
	totaltime += elapsed_time(&starttime);

	PG_RETURN_FLOAT8(totaltime);
}

PG_FUNCTION_INFO_V1(perf_bms_equal);

/*
 * The function
 * 1. creates two singleton bitmapset each containging the given member.
 * 2. calls bms_equal() with those two BMSes as argument 'reps' times.
 * 3. Reports the time taken by step 2
 */
Datum
perf_bms_equal(PG_FUNCTION_ARGS)
{
	int member = PG_GETARG_INT32(0);
	int reps = PG_GETARG_INT32(1);
	int i;
	Bitmapset *bms_a = bms_make_singleton(member);
	Bitmapset *bms_b = bms_make_singleton(member);
	instr_time	starttime;
	double		totaltime = 0;

	INSTR_TIME_SET_CURRENT(starttime);
	for (i = 0; i < reps; i++)
		bms_equal(bms_a, bms_b);
	totaltime += elapsed_time(&starttime);

	PG_RETURN_FLOAT8(totaltime);
}

static Bitmapset *
array_to_bitmapset(ArrayType *arr)
{
	Bitmapset *bms = NULL;
	ArrayIterator iter;
	bool	isnull;
	Datum itemvalue;

	Assert(ARR_ELEMTYPE(arr) == INT4OID);
	Assert(ARR_NDIM(arr));

	iter = array_create_iterator(arr, 0, NULL);

	while (array_iterate(iter, &itemvalue, &isnull))
		bms = bms_add_member(bms, DatumGetInt32(itemvalue));

	return bms;
}

PG_FUNCTION_INFO_V1(perf_bms_overlap);

Datum
perf_bms_overlap(PG_FUNCTION_ARGS)
{
	ArrayType *arr1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *arr2 = PG_GETARG_ARRAYTYPE_P(1);
	int	reps = PG_GETARG_INT32(2);
	int	i;
	Bitmapset *bms1 = array_to_bitmapset(arr1);
	Bitmapset *bms2 = array_to_bitmapset(arr2);
	instr_time	starttime;
	double		totaltime = 0;

	INSTR_TIME_SET_CURRENT(starttime);
	for (i = 0; i < reps; i++)
		bms_overlap(bms1, bms2);
	totaltime += elapsed_time(&starttime);

	PG_RETURN_FLOAT8(totaltime);
}

PG_FUNCTION_INFO_V1(perf_bms_union);

Datum
perf_bms_union(PG_FUNCTION_ARGS)
{
	ArrayType *arr1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *arr2 = PG_GETARG_ARRAYTYPE_P(1);
	int	reps = PG_GETARG_INT32(2);
	int	i;
	Bitmapset *bms1 = array_to_bitmapset(arr1);
	Bitmapset *bms2 = array_to_bitmapset(arr2);
	instr_time	starttime;
	double		totaltime = 0;

	INSTR_TIME_SET_CURRENT(starttime);
	for (i = 0; i < reps; i++)
		bms_union(bms1, bms2);
	totaltime += elapsed_time(&starttime);

	PG_RETURN_FLOAT8(totaltime);
}

/* Next set of function to be tried */
#ifdef TODO_COMPLETE
Datum
perf_bms_is_subset()

Datum
perf_bms_membership()

Datum
perf_bms_get_singleton_member()

Datum
perf_bms_is_empty()

Datum
perf_bms_add_members()

Datum
perf_bms_copy()

Datum
perf_bms_make_singleton()

Datum
perf_bms_intersect()

Datum
perf_bms_next_member()

Datum
perf_bms_difference()
#endif /* TODO_COMPLETE */