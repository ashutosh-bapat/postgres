/*
 * contrib/exprs/common.c
 *
 * Code common to all experiments.
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/pathnodes.h"
#include "portability/instr_time.h"
#include "utils/array.h"
#include "utils/memutils.h"

#include "exprs.h"

PG_MODULE_MAGIC;

/* Compute elapsed time in milliseconds since given timestamp */
double
elapsed_time(instr_time *starttime)
{
	instr_time	endtime;

	INSTR_TIME_SET_CURRENT(endtime);
	INSTR_TIME_SUBTRACT(endtime, *starttime);
	return INSTR_TIME_GET_MILLISEC(endtime);
}

