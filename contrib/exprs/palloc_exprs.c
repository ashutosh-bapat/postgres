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
#include "utils/memutils.h"
#include "exprs.h"

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
