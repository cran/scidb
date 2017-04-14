#include <R.h>
#include <Rinternals.h>
#include <stdlib.h>             // for NULL
#include <R_ext/Rdynload.h>

/* .Call calls */
extern SEXP scidb_parse (SEXP, SEXP, SEXP, SEXP, SEXP, SEXP);
extern SEXP scidb_raw (SEXP);

static const R_CallMethodDef CallEntries[] = {
  {"scidb_parse", (DL_FUNC) & scidb_parse, 6},
  {"scidb_raw", (DL_FUNC) & scidb_raw, 1},
  {NULL, NULL, 0}
};

void
R_init_scidb (DllInfo * dll)
{
  R_registerRoutines (dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols (dll, FALSE);
}
