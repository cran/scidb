#/*
#    _____      _ ____  ____
#   / ___/_____(_) __ \/ __ )
#   \__ \/ ___/ / / / / __  |
#  ___/ / /__/ / /_/ / /_/ / 
# /____/\___/_/_____/_____/  
#
#
#**
#* BEGIN_COPYRIGHT
#*
#* This file is part of SciDB.
#* Copyright (C) 2008-2013 SciDB, Inc.
#*
#* SciDB is free software: you can redistribute it and/or modify
#* it under the terms of the AFFERO GNU General Public License as published by
#* the Free Software Foundation.
#*
#* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
#* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
#* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
#* the AFFERO GNU General Public License for the complete license terms.
#*
#* You should have received a copy of the AFFERO GNU General Public License
#* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#*
#* END_COPYRIGHT
#*/

.onLoad = function(libname,pkgname)
{
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
}

.onUnload = function(libpath)
{
  options(scidb.index.sequence.limit=c())
  options(scidb.max.array.elements=c())
  options(scidb.version=c())
  options(scidb.gemm_chunk_size=c())
}

# scidb array object type map. We don't yet support strings in scidb array
# objects. Use df2scidb and iquery for strings.
# R type = SciDB type
.scidbtypes = list(
  double="double",
  integer="int32",
  logical="bool",
  character="char"
)

.typelen = list(
  double=8,
  integer=4,
  logical=1,
  character=1
)

# SciDB Integer dimension minimum, maximum
.scidb_DIM_MIN = "-4611686018427387902"
.scidb_DIM_MAX = "4611686018427387902"

# To quiet a check NOTE:
if(getRversion() >= "2.15.1")  utils::globalVariables(c("n", "p"))
