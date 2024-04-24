/*
 * contrib/xml2/xpath.c
 *
 * Parser interface for DOM-based parser (libxml) rather than
 * stream-based SAX-type parser
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "portability/instr_time.h"

PG_MODULE_MAGIC;

/* Compute elapsed time in seconds since given timestamp */
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