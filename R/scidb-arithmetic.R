#/*
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
# Element-wise operations
Ops.scidb = function(e1,e2) {
  switch(.Generic,
    '+' = .binop(e1,e2,"+"),
    '-' = .binop(e1,e2,"-"),
    '*' = .binop(e1,e2,"*"),
    '/' = .binop(e1,e2,"/"),
    '<' = .compare(e1,e2,"<"),
    '<=' = .compare(e1,e2,"<="),
    '>' = .compare(e1,e2,">"),
    '>=' = .compare(e1,e2,">="),
    '==' = .compare(e1,e2,"=="),
    default = stop("Unsupported binary operation.")
  )
}


# e1 and e2 must each already be SciDB arrays.
scidbmultiply = function(e1,e2)
{
  x = tmpnam("array")
  a1 = e1@attribute
  a2 = e2@attribute
  op1 = e1@name
  op2 = e2@name
  if(length(e1@attributes)>1)
    op1 = sprintf("project(%s,%s)",e1@name,a1)
  if(length(e2@attributes)>1)
    op2 = sprintf("project(%s,%s)",e2@name,a2)

# We use subarray to handle starting index mismatches, for
# subarray always returns an array with dimension indices starting
# at zero.
  l1 = length(dim(e1))
  lb = paste(rep(.scidb_DIM_MIN,l1),collapse=",")
  ub = paste(rep(.scidb_DIM_MAX,l1),collapse=",")
  op1 = sprintf("subarray(%s,%s,%s)",op1,lb,ub)
  l2 = length(dim(e2))
  lb = paste(rep(.scidb_DIM_MIN,l2),collapse=",")
  ub = paste(rep(.scidb_DIM_MAX,l2),collapse=",")
  op2 = sprintf("subarray(%s,%s,%s)",op2,lb,ub)

  j = which(e2@attribute == e2@attributes)[[1]]
# Adjust the 2nd array to be schema-compatible with the 1st:
  op2 = sprintf("repart(%s, <%s:%s>[%s=0:%.0f,%.0f,%.0f,%s=0:%.0f,%.0f,%.0f])",
          op2, a2, e2@type[[j]],
          e2@D$name[[1]], e2@D$length[[1]] - 1,
                          e1@D$chunk_interval[[2]], e1@D$chunk_overlap[[2]],
          e2@D$name[[2]], e2@D$length[[2]] - 1,
                          e2@D$chunk_interval[[2]], e2@D$chunk_overlap[[2]])
  scidbquery(paste("store(multiply(",op1,",",op2,"),",x,")",sep=""))
  return(scidb(x,gc=TRUE))
}

# Element-wise binary operations
.binop = function(e1,e2,op)
{
  e1s = e1
  e2s = e2
  if(!inherits(e1,"scidb")) {
    x = tmpnam()
    e1 = as.scidb(e1,name=x,gc=TRUE)
  }
  if(!inherits(e2,"scidb")) {
    x = tmpnam()
    e2 = as.scidb(e2,name=x,gc=TRUE)
  }
# OK, we've got two scidb arrays, op them:
  x = tmpnam("array")
  e1a = e1@attribute
  e2a = e2@attribute
  v = paste(e1a,e2a,sep="_")

# We use subarray to handle starting index mismatches...
  l1 = length(dim(e1))
  lb = paste(rep(.scidb_DIM_MIN,l1),collapse=",")
  ub = paste(rep(.scidb_DIM_MAX,l1),collapse=",")
  q1 = sprintf("subarray(%s,%s,%s)",e1@name,lb,ub)
  l = length(dim(e2))
  lb = paste(rep(.scidb_DIM_MIN,l),collapse=",")
  ub = paste(rep(.scidb_DIM_MAX,l),collapse=",")
  q2 = sprintf("subarray(%s,%s,%s)",e2@name,lb,ub)
# Adjust the 2nd array to be schema-compatible with the 1st:
  if(l==2 && l1==l)
  {
    q2 = sprintf(
       "repart(%s, <%s:%s>[%s=%.0f:%.0f,%.0f,%.0f,%s=%.0f:%.0f,%.0f,%.0f])",
       q2, e2a, e2@type[[1]],
       e2@D$name[[1]], 0, e2@D$length[[1]],
                          e1@D$chunk_interval[[1]], e1@D$chunk_overlap[[1]],
       e2@D$name[[2]], 0, e2@D$length[[2]],
                          e1@D$chunk_interval[[2]], e1@D$chunk_overlap[[2]])
  }
# Handle special scalar multiplication case:
  if(length(e1s)==1)
    Q = sprintf("apply(%s,%s,%.15f %s %s)",q2,v,e1s,op,e2a)
  else if(length(e2s)==1)
    Q = sprintf("apply(%s,%s,%.15f %s %s)",q1,v,e2s,op,e1a)
  else
  {
    Q = sprintf("join(%s as e1, %s as e2)", q1, q2)
    Q = sprintf("apply(%s, %s, e1.%s %s e2.%s)", Q, v, e1a, op, e2a)
  }
  Q = sprintf("project(%s, %s)",Q,v)
  Q = sprintf("store(%s, %s)",Q,x)
#  Q = paste("join(",q1," as e1, ",q2," as e2)",sep="")
#  Q = paste("apply(",Q,",",v,", e1.",e1a," ",op," e2.",e2a,")",sep="")
#  Q = paste("project(",Q,",",v,")")
#  Q = paste("store(",Q,",",x,")",sep="")
  scidbquery(Q)
  return(scidb(x,gc=TRUE))
}

# Very basic comparisons. See also filter.
# e1: A scidb array
# e2: A scalar or a scidb array. If a scidb array, the return .joincompare(e1,e2,op) (q.v.)
# op: A comparison infix operator character
#
# Return a scidb object
# Can throw a query error.
.compare = function(e1,e2,op)
{
  if(!inherits(e1,"scidb")) stop("Sorry, not yet implemented.")
  if(inherits(e2,"scidb")) return(.joincompare(e1,e2,op))
  type = names(.scidbtypes[.scidbtypes==e1@type])
  if(length(type)<1) stop("Unsupported data type.")
  op = gsub("==","=",op)
  tval = vector(mode=type,length=1)
  query = sprintf("filter(%s, %s %s %s)",e1@name, e1@attribute, op, e2)
  x = tmpnam()
  query = sprintf("store(%s,%s)",query,x)
  scidbquery(query)
  return(scidb(x,gc=TRUE))
}

.joincompare = function(e1,e2,op)
{
  stop("Yikes!")
}
