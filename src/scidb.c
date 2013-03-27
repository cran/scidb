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
#ifndef WIN32
#include <sys/mman.h>
#endif
#include <string.h>

#include <R.h>
#define USE_RINTERNALS
#include <Rinternals.h>

/* df2scidb converts a data.frame object to SciDB ASCII input format, returning
 * the result in a character value.  Only handles double, int, logical and
 * string.  chunk is the SciDB 1-D array chunk size.  start is the SciDB 1-D
 * array starting index. The array will have an integer dimension.
 */
SEXP
df2scidb (SEXP A, SEXP chunk, SEXP start, SEXP REALFORMAT)
{
  int j, k, m, n, m1, m2, J, M, logi;
  double x;
  int fd;
  FILE *fp;
  SEXP ans;
  struct stat sb;
  off_t length;
  const char *rfmt = CHAR(STRING_ELT(REALFORMAT,0));
  double S = *(REAL (start));
  int R = *(INTEGER (chunk));
  char *buf;

  fp = tmpfile();
  fd = fileno(fp);
  if(fd<0)
    error ("df2scidb can't create temporary file");
  n = length (A);
  m = nrows (VECTOR_ELT (A, 0));
  M = ceil (((double) m) / R);

  for (J = 0; J < M; ++J)
    {
      m1 = J * R;
      m2 = (J + 1) * R;
      if (m2 > m)
        m2 = m;
      fprintf (fp, "{%ld} [\n", (long) (m1 + S));
      for (j = m1; j < m2; ++j)
        {
          fprintf (fp, "(");
          for (k = 0; k < n; ++k)
            {
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
                    fprintf (fp, "\"%s\"", vi);
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
                        fprintf (fp, "%s", "true");
                      else
                        fprintf (fp, "%s", "false");
                      break;
                    case INTSXP:
                      if ((INTEGER (VECTOR_ELT (A, k))[j]) != R_NaInt)
                        fprintf (fp, "%d", INTEGER (VECTOR_ELT (A, k))[j]);
                      break;
                    case REALSXP:
                      x = REAL (VECTOR_ELT (A, k))[j];
                      if (!ISNA (x))
                        fprintf (fp, rfmt, x);
                      break;
                    case STRSXP:
                      if (STRING_ELT (VECTOR_ELT (A, k), j) != NA_STRING)
                        fprintf (fp, "\"%s\"",
                                 CHAR (STRING_ELT (VECTOR_ELT (A, k), j)));
                      break;
                    default:
                      break;
                    }
                }
              if (k == n - 1)
                fprintf (fp, ")");
              else
                fprintf (fp, ",");
            }
          if (j < m2 - 1)
            fprintf (fp, ",\n");
          else
            fprintf (fp, "];\n");
        }
    }

  fflush(fp);
  fstat (fd, &sb);
  length = sb.st_size;
  rewind(fp);
  buf = (char *)malloc(length);
  length = fread(buf,sizeof(char),length,fp);
  fclose(fp);
  close(fd);
  ans = mkString(buf);
  free(buf);
  return ans;
}


/* Convert a fixed-length type R matrix  A to binary SciDB input
 * format, writing to the specified open file descriptor. Return R NULL.
 * Presently supported types: double, int32, char, bool This function writes:
 * int64 rowindex, int64 colindex, <type> data, ...
 */
SEXP
m2scidb (SEXP A, SEXP F)
{
  long long j, k, m, n;
  int fp;
  char a;
  ssize_t w;
  SEXP dims = getAttrib (A, R_DimSymbol);
  if (dims == R_NilValue)
    j = 0;
  else
    j = LENGTH (dims);

  switch (j)
    {
    case 0:
      m = length (A);
      n = 1;
      break;
    default:
      m = nrows (A);
      n = ncols (A);
    }
  fp = INTEGER(F)[0];

  switch (TYPEOF (A))
    {
    case STRSXP:
      for (j = 0; j < m; j++)
        for (k = 0; k < n; k++) {
          w = write (fp, &j, sizeof(long long));
          w = write (fp, &k, sizeof(long long));
          w = write (fp, CHAR (STRING_ELT (A, j + k * m)), sizeof (char));
        }
      break;
    case LGLSXP:
      for (j = 0; j < m; j++)
        for (k = 0; k < n; k++) {
          w = write (fp, &j,sizeof(long long));
          w = write (fp, &k,sizeof(long long));
          a = (char) LOGICAL(A)[j + k *m];
          w = write (fp, &a, sizeof (char));
        }
      break;
    case INTSXP:
      for (j = 0; j < m; j++)
        for (k = 0; k < n; k++) {
          w = write (fp,&j,sizeof(long long));
          w = write (fp,&k,sizeof(long long));
          w = write (fp,&INTEGER (A)[j + k * m], sizeof (int));
         }
      break;
    case REALSXP:
      for (j = 0; j < m; j++)
        for (k = 0; k < n; k++) {
          w = write (fp,&j,sizeof(long long));
          w = write (fp,&k,sizeof(long long));
          w = write (fp,&REAL (A)[j + k * m], sizeof (double));
          if(w<sizeof(double)) warning("Data corrupted");
         }
      break;
    default:
      break;
    };

  return R_NilValue;
}

/* Read SciDB single-attribute binary output from the specified
 * file.
 * TYPE must be an SEXP of the correct output type.
 * DIM is a vector of maximum output dimension lengths (INTEGER)
 * NULLABLE is an integer, 1 indicates we need to parse for nullable
 * output from SciDB.
 *
 * It expects the binary input file to include integer dimensions
 * like:
 * value, int64, int64, ..., value, int64, int64, ..., etc.
 * It returns a list of two elements, each of length NR*NC:
 * 1. A vector of values  of length L = prod(DIM)
 * 2. A double precision matrix of L rows and number of dimension columns
 *    The matrix contains the dimension indices of each value.
 * Only values with corresponding row/column indices that are not marked
 * NA are valid.
 */
SEXP
scidb2m (SEXP file, SEXP DIM, SEXP TYPE, SEXP NULLABLE)
{
  int j, k, l;
  long long i;
  size_t m;
  FILE *fp;
  SEXP A, ans, I;
  double x;
  char xc[2];
  int xi;
  char a;
  char nx;
  int nullable = INTEGER(NULLABLE)[0];
  const char *f = CHAR (STRING_ELT (file, 0));
  fp = fopen (f, "r");
  if (!fp)
    error ("Invalid file");

  l = 1;
  for(j=0;j<length(DIM);++j) l = l*INTEGER(DIM)[j];

/* We reserve the maximum elements and fill the **indices** with NA. The
 * variable A contains the data and I the indices. Remember, R does not have
 * 64-bit integers, so we use doubles instead to store indices.
 */
  PROTECT (A = allocVector (TYPEOF (TYPE), l));
  PROTECT (I = allocMatrix (REALSXP, l, length(DIM)));
// XXX memset here?
  for(j=0;j<l*length(DIM);++j) REAL(I)[j] = NA_REAL;
  nx = 1;

  switch (TYPEOF (TYPE))
    {
    case REALSXP:
      for (j = 0; j < l; ++j)
        {
          REAL(A)[j] = NA_REAL;
          if(nullable) {
            m = fread(&nx, sizeof(char), 1, fp);
            if (m < 1) break;
          }
          m = fread (&x, sizeof (double), 1, fp);
          if (m < 1) break;
          for(k=0;k<length(DIM);++k) {
            m = fread(&i, sizeof(long long),1,fp);
            if (m < 1) break;
            REAL(I)[j + k*l] = (double)i;
          }
          if((int)nx != 0) REAL (A)[j] = x;
        }
      break;
    case STRSXP:
      for (j = 0; j < l; ++j)
        {
          SET_STRING_ELT (A, j, NA_STRING);
          if(nullable) {
            m = fread(&nx, sizeof(char), 1, fp);
            if (m < 1) break;
          }
          memset(xc,0,2);
          m = fread (xc, sizeof (char), 1, fp);
          if (m < 1) break;
          for(k=0;k<length(DIM);++k) {
            m = fread(&i, sizeof(long long),1,fp);
            if (m < 1) break;
            REAL(I)[j + k*l] = (double)i;
          }
          if((int)nx != 0) SET_STRING_ELT (A, j, mkChar (xc));
        }
      break;
    case LGLSXP:
      for (j = 0; j < l; ++j)
        {
          LOGICAL (A)[j] = NA_LOGICAL;
          if(nullable) {
            m = fread(&nx, sizeof(char), 1, fp);
            if (m < 1) break;
          }
          m = fread (&a, sizeof (char), 1, fp);
          if (m < 1) break;
          for(k=0;k<length(DIM);++k) {
            m = fread(&i, sizeof(long long),1,fp);
            if (m < 1) break;
            REAL(I)[j + k*l] = (double)i;
          }
          if((int)nx != 0) LOGICAL (A)[j] = (int)a;
        }
      break;
    case INTSXP:
      for (j = 0; j < l; ++j)
        {
          INTEGER (A)[j] = R_NaInt;
          if(nullable) {
            m = fread(&nx, sizeof(char), 1, fp);
            if (m < 1) break;
          }
          m = fread (&xi, sizeof (int), 1, fp);
          if (m < 1) break;
          for(k=0;k<length(DIM);++k) {
            m = fread(&i, sizeof(long long),1,fp);
            if (m < 1) break;
            REAL(I)[j + k*l] = (double)i;
          }
          if((int) nx != 0) INTEGER (A)[j] = xi;
        }
      break;
    default:
      break;
    };
  ans = PROTECT(allocVector(VECSXP, 2));
  SET_VECTOR_ELT(ans, 0, A);
  SET_VECTOR_ELT(ans, 1, I);

  fclose (fp);
  UNPROTECT (3);
  return ans;
}





/*
 * BUF: RAW input data buffer
 * DIM: vector of array dimensions
 * TYPE: SciDB attribute type (limited to supported TYPEOF enumeration)
 *
 * returns vector of TYPE
 */
SEXP
blob2R (SEXP BUF, SEXP DIM, SEXP TYPE, SEXP NULLABLE)
{
  int j, l;
  char *p;
  SEXP A;
  double x;
  char xc[2];
  int xi;
  char a;
  char nx;
  int nullable = INTEGER(NULLABLE)[0];
  p = (char *)RAW(BUF);

  l = 1;
  for(j=0;j<length(DIM);++j) l = l*INTEGER(DIM)[j];

  PROTECT (A = allocVector (TYPEOF (TYPE), l));
  nx = 1;

  switch (TYPEOF (TYPE))
    {
    case REALSXP:
      for (j = 0; j < l; ++j)
        {
          REAL(A)[j] = NA_REAL;
          if(nullable) {
            nx = (int) (char)(*((char *)p));
            p+=1;
          }
          x = *((double *)p);
          p+=sizeof(double);
          if((int)nx != 0) REAL (A)[j] = x;
        }
      break;
    case STRSXP:
      for (j = 0; j < l; ++j)
        {
          SET_STRING_ELT (A, j, NA_STRING);
          if(nullable) {
            nx = (int) ((char)*((char *)p));
            p+=1;
          }
          memset(xc,0,2);
          memcpy(xc, p, sizeof(char));
          p+=1;
          if((int)nx != 0) SET_STRING_ELT (A, j, mkChar (xc));
        }
      break;
    case LGLSXP:
      for (j = 0; j < l; ++j)
        {
          LOGICAL (A)[j] = NA_LOGICAL;
          if(nullable) {
            nx = (int) ((char)*((char *)p));
            p+=1;
          }
          a = (int) ((char)(*(char *)p));
          p+=1;
          if((int)nx != 0) LOGICAL (A)[j] = (int)a;
        }
      break;
    case INTSXP:
      for (j = 0; j < l; ++j)
        {
          INTEGER (A)[j] = R_NaInt;
          if(nullable) {
            nx = (int) ((char)*((char *)p));
            p+=1;
          }
          xi = (int) *( (int *)p);
          p+=sizeof(int);
          if((int) nx != 0) INTEGER (A)[j] = xi;
        }
      break;
    default:
      break;
    };

  UNPROTECT (1);
  return A;
}
