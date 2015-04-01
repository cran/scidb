/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2013 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <math.h>
#include <signal.h>
#include <string.h>
#ifdef WIN32
#include <windows.h>
#else
#include <sys/mman.h>
#endif

#include <R.h>
#define USE_RINTERNALS
#include <Rinternals.h>

#define LINESIZE 4096

#define NOT_MISSING 255
#define IS_MISSING 0

/* sig_int is used to detect user SIGINT signals */
static volatile int sig_int = 0;

#ifndef WIN32
static void
handler (int s, siginfo_t *i, void *x)
{
  sig_int = 1;
}
#endif

typedef struct
{
  char *s;
  size_t len;
  size_t pos;
} abuf;

abuf
newbuf(size_t size)
{
  abuf a;
  if(size < 1) error("Invalid buffer size");
  a.s   = (char *)malloc(size);
  if(!a.s) error("Not enough memory");
  a.len = size;
  a.pos = 0;
  return a;
}

void
append(abuf *buf, char *string)
{
  char *new;
  int l = strlen(string) + 1;
  if(buf->len - buf->pos < l)
  {
    if(buf->len + l > buf->len*2)
      buf->len = buf->len + l;
    else
      buf->len = buf->len*2;
    new     = realloc(buf->s, buf->len);
    if(new) buf->s = new;
    else
    {
      if(buf->s) free(buf->s);
      error ("Not enough memory.");
    }
  }
  strncpy(&buf->s[buf->pos], string, l); // Includes terminating \0
  buf->pos+= l-1; // Does not include terminating \0
}

/* df2scidb converts a data.frame object to SciDB ASCII input format, returning
 * the result in a character string.  It only handles double, int, logical and
 * string data types in the data.frame.  chunk is the integer SciDB 1-D array
 * chunk size.  start is the integer SciDB 1-D array starting index.
 */
SEXP
df2scidb (SEXP A, SEXP chunk, SEXP start, SEXP REALFORMAT)
{
  int j, k, m, n, m1, m2, J, M, logi;
  char line[LINESIZE];
  abuf buf;
  double x;
  SEXP ans;
  const char *rfmt = CHAR(STRING_ELT(REALFORMAT,0));
  double S = *(REAL (start));
  int R = *(INTEGER (chunk));

  buf = newbuf(1048576); // 1MB initial buffer

  n = length (A);
  m = nrows (VECTOR_ELT (A, 0));
  M = ceil (((double) m) / R);

  for (J = 0; J < M; ++J)
    {
      m1 = J * R;
      m2 = (J + 1) * R;
      if (m2 > m)
        m2 = m;
      snprintf(line, LINESIZE, "{%ld}[\n", (long) (m1 + S));
      append(&buf,line);
      for (j = m1; j < m2; ++j)
        {
          append(&buf, "(");
          for (k = 0; k < n; ++k)
            {
              memset(line,0,LINESIZE);
// Check for factor and print factor level instead of integer
              if (!
                  (getAttrib (VECTOR_ELT (A, k), R_LevelsSymbol) ==
                   R_NilValue))
                {
                  if ((INTEGER (VECTOR_ELT (A, k))[j]) != R_NaInt)
                  {
                    const char *vi =
                      translateChar (STRING_ELT
                                   (getAttrib
                                    (VECTOR_ELT (A, k), R_LevelsSymbol),
                                    INTEGER (VECTOR_ELT (A, k))[j] - 1));
                    snprintf(line, LINESIZE, "\"%s\"", vi);
                    append(&buf,line);
                  }
                }
              else
                {
                  switch (TYPEOF (VECTOR_ELT (A, k)))
                    {
                    case LGLSXP:
                      logi = 0;
                      if ((LOGICAL (VECTOR_ELT (A, k))[j]) != NA_LOGICAL)
                        logi = (int) LOGICAL (VECTOR_ELT (A, k))[j];
                      if (logi)
                        snprintf(line, LINESIZE, "%s", "true");
                      else
                        snprintf(line, LINESIZE, "%s", "false");
                      append(&buf,line);
                      break;
                    case INTSXP:
                      if ((INTEGER (VECTOR_ELT (A, k))[j]) != R_NaInt)
                        snprintf(line, LINESIZE, "%d", INTEGER (VECTOR_ELT (A, k))[j]);
                      append(&buf,line);
                      break;
                    case REALSXP:
                      x = REAL (VECTOR_ELT (A, k))[j];
                      if (!ISNA (x))
                        snprintf(line, LINESIZE, rfmt, x);
                      append(&buf,line);
                      break;
                    case STRSXP:
                      if (STRING_ELT (VECTOR_ELT (A, k), j) != NA_STRING)
                        snprintf(line, LINESIZE, "\"%s\"",
                                 CHAR (STRING_ELT (VECTOR_ELT (A, k), j)));
                      append(&buf,line);
                      break;
                    default:
                      break;
                    }
                }
              if (k == n - 1)
                append(&buf, ")");
              else
                append(&buf, ",");
            }
          if (j < m2 - 1)
            append(&buf, ",");
          else
            append(&buf, "];");
        }
    }

  ans = mkString(buf.s);
  if(buf.s) free(buf.s);
  return ans;
}


/* Convert basic R vectors into SciDB nullable types, translating R
 * missing values into SciDB missing values.
 */
SEXP
scidb_raw (SEXP A)
{
  SEXP ans = R_NilValue;
  char *buf;
  R_xlen_t j, len = XLENGTH(A);
  unsigned int slen, l;
  const unsigned char not_missing = NOT_MISSING;
  const char missing = IS_MISSING;
  switch (TYPEOF (A))
    {
    case REALSXP:
      PROTECT(ans = allocVector(RAWSXP, len*(sizeof(double) + 1)));
      buf = (char *)RAW(ans);
      if(!buf) error ("Not enough memory.");
      for(j=0;j<len;++j)
      {
        if(!ISNA(REAL(A)[j]))
        {
          memcpy(buf, &not_missing, 1); buf++;
          memcpy(buf, &REAL(A)[j], sizeof(double)); buf+=sizeof(double);
        } else
        {
          memcpy(buf, &missing, 1); buf++;
          buf+=sizeof(double);
        }
      }
      break;
    case INTSXP:
      PROTECT(ans = allocVector(RAWSXP, len*(sizeof(int) + 1)));
      buf = (char *)RAW(ans);
      if(!buf) error ("Not enough memory.");
      for(j=0;j<len;++j)
      {
        if(INTEGER(A)[j] != R_NaInt)
        {
          memcpy(buf, &not_missing, 1); buf++;
          memcpy(buf, &INTEGER(A)[j], sizeof(int)); buf+=sizeof(int);
        } else
        {
          memcpy(buf, &missing, 1); buf++;
          buf+=sizeof(int);
        }
      }
      break;
    case LGLSXP:
      PROTECT(ans = allocVector(RAWSXP, len*(sizeof(char) + 1)));
      buf = (char *)RAW(ans);
      if(!buf) error ("Not enough memory.");
      for(j=0;j<len;++j)
      {
        if(LOGICAL(A)[j] != NA_LOGICAL)
        {
          memcpy(buf, &not_missing, 1); buf++;
          memcpy(buf, &LOGICAL(A)[j], sizeof(char)); buf++;
        } else
        {
          memcpy(buf, &missing, 1); buf+=2;
        }
      }
      break;
    case STRSXP:
/* Compute the output length first, padding length for SciDB string header,
 * null flag (byte), string length (unsigned int), and for the terminating
 * string zero byte.
 */
      slen = 0;
      for(j=0;j<len;++j)
      {
        if(STRING_ELT(A,j) == NA_STRING) slen = slen + 4 + 1 + 1;
        else slen = slen + 4 + 1 + 1 + strlen(CHAR(STRING_ELT(A,j)));
      }
      PROTECT(ans = allocVector(RAWSXP,  slen));
      buf = (char *)RAW(ans);
      if(!buf) error ("Not enough memory.");
      for(j=0;j<len;++j)
      {
        if(STRING_ELT(A,j) != NA_STRING)
        {
          l = strlen(CHAR(STRING_ELT(A,j))) + 1;
          memcpy(buf, &not_missing, 1); buf++;
          memcpy(buf, &l, 4); buf+=4;
          memcpy(buf, CHAR(STRING_ELT(A,j)), l); buf = buf + l;
        } else
        {
          memcpy(buf, &missing, 1); buf+=6;
        }
      }
      break;
    default:
      break;
    }
  UNPROTECT(1);
  return ans;
}

void
reset_sig ()
{
  sig_int = 0;
}

SEXP
state ()
{
  return ScalarInteger(sig_int);
}

SEXP
reset ()
{
  reset_sig();
  return R_NilValue;
}

/* Enable or disable SIGINT
   I = 1: Ignore SIGINT
   I = 2: Use custom handler
   ELSE : Use default handler
 */
SEXP
sig (SEXP I)
{
  int i = INTEGER (I)[0];
#ifdef WIN32
  switch (i)
  {
    case 1:
      signal(SIGINT, SIG_IGN);
      break;
    case 2:
      signal(SIGINT, SIG_IGN);
      break;
    default:
      signal(SIGINT, SIG_DFL);
  }
#else
  int j;
  struct sigaction action;
  memset (&action, 0, sizeof(action));
  action.sa_sigaction = handler;
  action.sa_flags = SA_SIGINFO;
 
  switch (i)
    {
    case 1:
      signal (SIGINT, SIG_IGN);
      break;
    case 2:
      j = sigaction(SIGINT, &action, NULL);
      if(j<0) error("Error setting signal handler");
      break;
    default:
      reset_sig();
      signal (SIGINT, SIG_DFL);
    }
#endif
  return R_NilValue;
}
