/*
 * contrib/exprs/exprs.h
 *
 * Code common to all experiments.
 */

#include "postgres.h"

#include "portability/instr_time.h"

double elapsed_time(instr_time *starttime);
