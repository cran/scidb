#
#    _____      _ ____  ____
#   / ___/_____(_) __ \/ __ )
#   \__ \/ ___/ / / / / __  |
#  ___/ / /__/ / /_/ / /_/ / 
# /____/\___/_/_____/_____/  
#
#
#
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#

# Signal constants
SIG_DFL = 0L # Default SIGINT signal handler
SIG_IGN = 1L # Ignore SIGINT
SIG_TRP = 2L # A custom signal handler (see scidb.c)

.onLoad = function(libname,pkgname)
{
# RStudio does not let us set up a custom signal handler, and this has also
# been problematic to set up on non-Console Windows R processes.  We check for
# these special cases and resort to a more basic method to gracefully trap
# SIGINT and bail out of RCurl sessions.
  if(Sys.getenv("RSTUDIO")=="1" || "windows" %in% tolower(Sys.info()["sysname"]))
  {
    env = asNamespace(pkgname)
    assign("SIG_TRP",1L,envir=env)  # SIG_TRP = SIG_IGN
  }
# Maximum allowed sequential index limit (for larger, use between)
  options(scidb.index.sequence.limit=1000000)
# Maximum allowed elements in an array return result
  options(scidb.max.array.elements=100000000)
# The scidb.version option is set during scidbconnect(). However, users
# may carefully override it to enable certain bug fixes specific to older
# versions of SciDB.
  options(scidb.version=13.9)
# Set this to 32 for SciDB version 13.6
  options(scidb.gemm_chunk_size=1000)
# Default shim port
  options(scidb.default_shim_port=8080L)
  options(scidb.default_shim_host="localhost")
# There was a bad gemm/gesvd/subarray bug until 14.3. When TRUE, this option
# puts a reliable work-around in place (inserting sg everywhere).
  options(scidb.gemm_bug=TRUE)
# Make it harder to remove arrays. When this option is TRUE, users
# have to specify scidbrm(array, force=TRUE) to remove arrays that do not
# begin with "R_array".
  options(scidb.safe_remove=TRUE)
# Disable SSL certificate host name checking by default. This is important mostly
# for Amazon EC2 where hostnames rarely match their DNS names. If you enable this
# then the shim SSL certificate CN entry *must* match the server host name for the
# encrypted session to work. Set this TRUE for stronger security (help avoid MTM)
# in SSL connections.
  options(scidb.verifyhost=FALSE)
}

.onUnload = function(libpath)
{
  options(scidb.index.sequence.limit=c())
  options(scidb.max.array.elements=c())
  options(scidb.version=c())
  options(scidb.gemm_chunk_size=c())
  options(scidb.safe_remove=c())
  options(scidb.default_shim_port=c())
  options(scidb.default_shim_host=c())
}

# scidb array object type map. We don't yet support strings in scidb array
# objects. Use df2scidb and iquery for strings.
# R type = SciDB type
.scidbtypes = list(
  double="double",
  double="int64",
  double="uint64",
  integer="int32",
  logical="bool",
  character="char"
)

# These types are used to infer dataframe column classes.
.scidbdftypes = list(
  double="double",
  int64="double",
  uint64="double",
  uint32="double",
  int32="integer",
  int16="integer",
  unit16="integer",
  int8="integer",
  uint8="integer",
  bool="logical",
  string="character",
  char="character",
  datetime="Date"
)

.typelen = list(
  double=8,
  integer=4,
  logical=1,
  character=1
)

# A convenience mapper for a few common apply-style aggregate
# functions.
.scidbfun = function(FUN)
{
  fns = list(
  mean="avg",
  sd="stdev",
  var="var",
  sum="sum",
  prod="prod",
  max="max",
  min="min",
  median="median",
  length="count",
  count="count")
  i = unlist(lapply(list(mean,sd,var,sum,prod,max,min,median,length,count),
               function(x) identical(x,FUN)))
  if(!any(i)) return(NULL)
  fns[i][[1]]
}

# SciDB Integer dimension minimum, maximum
.scidb_DIM_MIN = "-4611686018427387902"
.scidb_DIM_MAX = "4611686018427387902"

# To quiet a check NOTE:
if(getRversion() >= "2.15.1")  utils::globalVariables(c("n", "p"))
