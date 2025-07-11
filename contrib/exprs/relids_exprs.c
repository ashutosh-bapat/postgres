/*
 * contrib/palloc_expr.c
 *
 * Module to experiment with palloc and pfree.
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/pathnodes.h"
#include "portability/instr_time.h"
#include "utils/array.h"
#include "exprs.h"


PG_FUNCTION_INFO_V1(perf_relids_equal);

/*
 * The function
 * 1. creates two singleton bitmapset each containging the given member.
 * 2. calls bms_equal() with those two BMSes as argument 'reps' times.
 * 3. Reports the time taken by step 2
 */
Datum
perf_relids_equal(PG_FUNCTION_ARGS)
{
	int member = PG_GETARG_INT32(0);
	int reps = PG_GETARG_INT32(1);
	int i;
	Relids bms_a = bms_make_singleton(member);
	Relids bms_b = bms_make_singleton(member);
	instr_time	starttime;
	double		totaltime = 0;

	INSTR_TIME_SET_CURRENT(starttime);
	for (i = 0; i < reps; i++)
		bms_equal(bms_a, bms_b);
	totaltime += elapsed_time(&starttime);

	PG_RETURN_FLOAT8(totaltime);
}

static Relids
array_to_relids(ArrayType *arr)
{
	Relids relids = NULL;
	ArrayIterator iter;
	bool	isnull;
	Datum itemvalue;

	Assert(ARR_ELEMTYPE(arr) == INT4OID);
	Assert(ARR_NDIM(arr));

	iter = array_create_iterator(arr, 0, NULL);

	while (array_iterate(iter, &itemvalue, &isnull))
		relids = bms_add_member(relids, DatumGetInt32(itemvalue));

	return relids;
}

PG_FUNCTION_INFO_V1(perf_relids_overlap);

Datum
perf_relids_overlap(PG_FUNCTION_ARGS)
{
	ArrayType *arr1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *arr2 = PG_GETARG_ARRAYTYPE_P(1);
	int	reps = PG_GETARG_INT32(2);
	int	i;
	Relids relids1 = array_to_relids(arr1);
	Relids relids2 = array_to_relids(arr2);
	instr_time	starttime;
	double		totaltime = 0;

	INSTR_TIME_SET_CURRENT(starttime);
	for (i = 0; i < reps; i++)
		bms_overlap(relids1, relids2);
	totaltime += elapsed_time(&starttime);

	PG_RETURN_FLOAT8(totaltime);
}

PG_FUNCTION_INFO_V1(perf_relids_union);

Datum
perf_relids_union(PG_FUNCTION_ARGS)
{
	ArrayType *arr1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *arr2 = PG_GETARG_ARRAYTYPE_P(1);
	int	reps = PG_GETARG_INT32(2);
	int	i;
	Relids relids1 = array_to_relids(arr1);
	Relids relids2 = array_to_relids(arr2);
	instr_time	starttime;
	double		totaltime = 0;

	INSTR_TIME_SET_CURRENT(starttime);
	for (i = 0; i < reps; i++)
		bms_union(relids1, relids2);
	totaltime += elapsed_time(&starttime);

	PG_RETURN_FLOAT8(totaltime);
}

/* Next set of function to be tried */
#ifdef TODO_COMPLETE
Datum
perf_relids_is_subset()

Datum
perf_relids_membership()

Datum
perf_relids_get_singleton_member()

Datum
perf_relids_is_empty()

Datum
perf_relids_add_members()

Datum
perf_relids_copy()

Datum
perf_relids_make_singleton()

Datum
perf_relids_intersect()

Datum
perf_relids_next_member()

Datum
perf_relids_difference()
#endif /* TODO_COMPLETE */