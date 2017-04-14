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

typedef struct
{
  char *s;
  size_t len;
  size_t pos;
} abuf;

abuf
newbuf (size_t size)
{
  abuf a;
  if (size < 1)
    error ("Invalid buffer size");
  a.s = (char *) malloc (size);
  if (!a.s)
    error ("Not enough memory");
  a.len = size;
  a.pos = 0;
  return a;
}

void
append (abuf * buf, char *string)
{
  char *new;
  int l = strlen (string) + 1;
  if (buf->len - buf->pos < l)
    {
      if (buf->len + l > buf->len * 2)
        buf->len = buf->len + l;
      else
        buf->len = buf->len * 2;
      new = realloc (buf->s, buf->len);
      if (new)
        buf->s = new;
      else
        {
          if (buf->s)
            free (buf->s);
          error ("Not enough memory.");
        }
    }
  strncpy (&buf->s[buf->pos], string, l);       // Includes terminating \0
  buf->pos += l - 1;            // Does not include terminating \0
}


/* Convert basic R vectors into SciDB nullable types, translating R
 * missing values into SciDB missing values.
 */
SEXP
scidb_raw (SEXP A)
{
  SEXP ans = R_NilValue;
  char *buf;
  R_xlen_t j, len = XLENGTH (A);
  unsigned int slen, l;
  const unsigned char not_missing = NOT_MISSING;
  const char missing = IS_MISSING;
  switch (TYPEOF (A))
    {
    case REALSXP:
      PROTECT (ans = allocVector (RAWSXP, len * (sizeof (double) + 1)));
      buf = (char *) RAW (ans);
      if (!buf)
        error ("Not enough memory.");
      for (j = 0; j < len; ++j)
        {
          if (!ISNA (REAL (A)[j]))
            {
              memcpy (buf, &not_missing, 1);
              buf++;
              memcpy (buf, &REAL (A)[j], sizeof (double));
              buf += sizeof (double);
            }
          else
            {
              memcpy (buf, &missing, 1);
              buf++;
              buf += sizeof (double);
            }
        }
      break;
    case INTSXP:
      PROTECT (ans = allocVector (RAWSXP, len * (sizeof (int) + 1)));
      buf = (char *) RAW (ans);
      if (!buf)
        error ("Not enough memory.");
      for (j = 0; j < len; ++j)
        {
          if (INTEGER (A)[j] != R_NaInt)
            {
              memcpy (buf, &not_missing, 1);
              buf++;
              memcpy (buf, &INTEGER (A)[j], sizeof (int));
              buf += sizeof (int);
            }
          else
            {
              memcpy (buf, &missing, 1);
              buf++;
              buf += sizeof (int);
            }
        }
      break;
    case LGLSXP:
      PROTECT (ans = allocVector (RAWSXP, len * (sizeof (char) + 1)));
      buf = (char *) RAW (ans);
      if (!buf)
        error ("Not enough memory.");
      for (j = 0; j < len; ++j)
        {
          if (LOGICAL (A)[j] != NA_LOGICAL)
            {
              memcpy (buf, &not_missing, 1);
              buf++;
              memcpy (buf, &LOGICAL (A)[j], sizeof (char));
              buf++;
            }
          else
            {
              memcpy (buf, &missing, 1);
              buf += 2;
            }
        }
      break;
    case STRSXP:
/* Compute the output length first, padding length for SciDB string header,
 * null flag (byte), string length (unsigned int), and for the terminating
 * string zero byte.
 */
      slen = 0;
      for (j = 0; j < len; ++j)
        {
          if (STRING_ELT (A, j) == NA_STRING)
            slen = slen + 4 + 1 + 1;
          else
            slen = slen + 4 + 1 + 1 + strlen (CHAR (STRING_ELT (A, j)));
        }
      PROTECT (ans = allocVector (RAWSXP, slen));
      buf = (char *) RAW (ans);
      if (!buf)
        error ("Not enough memory.");
      for (j = 0; j < len; ++j)
        {
          if (STRING_ELT (A, j) != NA_STRING)
            {
              l = strlen (CHAR (STRING_ELT (A, j))) + 1;
              memcpy (buf, &not_missing, 1);
              buf++;
              memcpy (buf, &l, 4);
              buf += 4;
              memcpy (buf, CHAR (STRING_ELT (A, j)), l);
              buf = buf + l;
            }
          else
            {
              memcpy (buf, &missing, 1);
              buf += 6;
            }
        }
      break;
    case RAWSXP:
/* Compute the output length first, padding length for SciDB binary header,
 * null flag (byte), data length (unsigned int)
 */
      if (len > INT_MAX)
        error ("Too big.");
      slen = 1 + 4 + len;       // null_flag + len + data
      PROTECT (ans = allocVector (RAWSXP, slen));
      buf = (char *) RAW (ans);
      if (!buf)
        error ("Not enough memory.");
      memcpy (buf, &not_missing, 1);
      buf++;
      l = (unsigned int) len;
      memcpy (buf, &l, 4);
      buf += 4;
      memcpy (buf, RAW (A), len);
      break;
    default:
      break;
    }
  UNPROTECT (1);
  return ans;
}
