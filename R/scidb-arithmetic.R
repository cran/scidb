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
    '^' = .binop(e1,e2,"^"),
    '+' = .binop(e1,e2,"+"),
    '-' = .binop(e1,e2,"-"),
    '*' = .binop(e1,e2,"*"),
    '/' = .binop(e1,e2,"/"),
    '<' = .compare(e1,e2,"<"),
    '<=' = .compare(e1,e2,"<="),
    '>' = .compare(e1,e2,">"),
    '>=' = .compare(e1,e2,">="),
    '==' = .compare(e1,e2,"="),
    '!=' = .compare(e1,e2,"<>"),
    default = stop("Unsupported binary operation.")
  )
}

# e1 and e2 must each already be SciDB arrays.
scidbmultiply = function(e1,e2)
{
# As of SciDB version 13.12, SciDB exhibits nasty bugs when gemm is nested
# within other SciDB operators, in particular subarray. We use sg to avoid
# this problem. XXX
  `eval` = FALSE
# Check for availability of spgemm
  P4 = length(grep("spgemm",.scidbenv$ops[,2]))>0
  if(length(e1@attributes)>1)
    e1 = project(e1,e1@attribute)
  if(length(e2@attributes)>1)
    e2 = project(e2,e2@attribute)
  e1.sparse = is.sparse(e1)
  e2.sparse = is.sparse(e2)
  SPARSE = e1.sparse || e2.sparse

  a1 = e1@attribute
  a2 = e2@attribute
  op1 = e1@name
  op2 = e2@name

# Promote vectors to row- or column-vectors as required.
  if(length(dim(e1))<2)
  {
    L = dim(e1)
    M = L/e2@D$length[1]
    if(M != floor(M)) stop("Non-conformable dimensions")

    as = build_attr_schema(e1)
    op1 = sprintf("reshape(%s,%s[i=0:%.0f,%.0f,0,j=0:%.0f,%.0f,0])",
            op1, as, M-1, e1@D$chunk_interval[1],
              e2@D$length[1]-1, e2@D$chunk_interval[1])
    
    e1@D$name = c("i","j")
    e1@D$type = c("int64","int64")
    e1@D$start = c(0,0)
    e1@D$length = c(M,e2@D$length[1])
    e1@D$chunk_interval = c(e1@D$chunk_interval[1],e2@D$chunk_interval[1])
    e1@D$chunk_overlap = c(0,0)
    e1@dim = e1@D$length
  }
  if(length(dim(e2))<2)
  {
    L = dim(e2)
    N = L/e1@D$length[2]
    if(N != floor(N)) stop("Non-conformable dimensions")

    as = build_attr_schema(e2)
    op2 = sprintf("reshape(%s,%s[i=0:%.0f,%.0f,0,j=0:%.0f,%.0f,0])",
            op2, as, e1@D$length[2]-1, e1@D$chunk_interval[2],
                     N-1, e2@D$chunk_interval[1])
    
    e2@D$name = c("i","j")
    e2@D$type = c("int64","int64")
    e2@D$start = c(0,0)
    e2@D$length = c(e1@D$length[2],N)
    e2@D$chunk_interval = c(e1@D$chunk_interval[2],e2@D$chunk_interval[1])
    e2@D$chunk_overlap = c(0,0)
    e2@dim = e2@D$length
  }

# We use subarray to handle starting index mismatches (subarray
# returns an array with dimension indices starting at zero).
  l1 = length(dim(e1))
  lb = paste(rep("null",l1),collapse=",")
  ub = paste(rep("null",l1),collapse=",")
  op1 = sprintf("sg(subarray(%s,%s,%s),1,-1)",op1,lb,ub)
  l2 = length(dim(e2))
  lb = paste(rep("null",l2),collapse=",")
  ub = paste(rep("null",l2),collapse=",")
  op2 = sprintf("sg(subarray(%s,%s,%s),1,-1)",op2,lb,ub)

  if(!SPARSE)
  {
# Adjust the arrays to conform to GEMM requirements
    dnames = make.names_(c(e1@D$name[[1]],e2@D$name[[2]]))
    CHUNK_SIZE = options("scidb.gemm_chunk_size")[[1]]
    op1 = sprintf("repart(%s,<%s:%s>[%s=0:%.0f,%.0f,0,%s=0:%.0f,%.0f,0])",op1,a1,e1@type[1],e1@D$name[[1]],e1@D$length[[1]]-1,CHUNK_SIZE,e1@D$name[[2]],e1@D$length[[2]]-1,CHUNK_SIZE)
    op2 = sprintf("repart(%s,<%s:%s>[%s=0:%.0f,%.0f,0,%s=0:%.0f,%.0f,0])",op2,a2,e2@type[1],e2@D$name[[1]],e2@D$length[[1]]-1,CHUNK_SIZE,e2@D$name[[2]],e2@D$length[[2]]-1,CHUNK_SIZE)
    osc = sprintf("<%s:%s>[%s=0:%.0f,%.0f,0,%s=0:%.0f,%.0f,0]",a1,e1@type[1],dnames[[1]],e1@D$length[[1]]-1,CHUNK_SIZE,dnames[[2]],e2@D$length[[2]]-1,CHUNK_SIZE)
    op3 = sprintf("build(%s,0)",osc)
  } else
  {
# Adjust array partitions as required by spgemm
    op2 = sprintf("repart(%s, <%s:%s>[%s=0:%.0f,%.0f,0,%s=0:%.0f,%.0f,0])",
            op2, a2, e2@type[1], e2@D$name[[1]], e2@D$length[[1]]-1, e1@D$chunk_interval[[2]],
            e2@D$name[[2]], e2@D$length[[2]]-1, e2@D$chunk_interval[[2]])
  }

# Decide which multiplication algorithm to use
  if(SPARSE && !P4)
  {
    stop("Sparse matrix multiplication not supported")
  }
  else if (SPARSE && P4)
    query = sprintf("spgemm(%s, %s)", op1, op2)
  else
    query = sprintf("gemm(%s, %s, %s)",op1,op2,op3)

  ans = .scidbeval(query,gc=TRUE,eval=eval,depend=list(e1,e2))
  ans
}

# Element-wise binary operations
.binop = function(e1,e2,op)
{
  e1s = e1
  e2s = e2
  e1a = "scalar"
  e2a = "scalar"
  depend = c()
# Check for non-scidb object arguments and convert to scidb
  if(!inherits(e1,"scidb") && length(e1)>1) {
    x = tmpnam()
    e1 = as.scidb(e1,name=x,gc=TRUE)
  }
  if(!inherits(e2,"scidb") && length(e2)>1) {
    x = tmpnam()
    e2 = as.scidb(e2,name=x,gc=TRUE)
  }
  if(inherits(e1,"scidb"))
  {
#    e1 = scidbeval(e1,gc=TRUE)
    e1a = e1@attribute
    depend = c(depend, e1)
  }
  if(inherits(e2,"scidb"))
  {
#    e2 = scidbeval(e2,gc=TRUE)
    e2a = e2@attribute
    depend = c(depend, e2)
  }
# OK, we've got two scidb arrays, op them. v holds the new attribute name.
  v = make.unique_(c(e1a,e2a), "v")

# We use subarray to handle starting index mismatches...
  q1 = q2 = ""
  l1 = length(dim(e1))
  lb = paste(rep("null",l1),collapse=",")
  ub = paste(rep("null",l1),collapse=",")
  if(inherits(e1,"scidb"))
  {
    q1 = sprintf("sg(subarray(project(%s,%s),%s,%s),1,-1)",e1@name,e1@attribute,lb,ub)
  }
  l = length(dim(e2))
  lb = paste(rep("null",l),collapse=",")
  ub = paste(rep("null",l),collapse=",")
  if(inherits(e2,"scidb"))
  {
    q2 = sprintf("sg(subarray(project(%s,%s),%s,%s),1,-1)",e2@name,e2@attribute,lb,ub)
  }
# Adjust the 2nd array to be schema-compatible with the 1st:
  if(l==2 && l1==2)
  {
    schema = sprintf(
       "<%s:%s>[%s=%.0f:%.0f,%.0f,%.0f,%s=%.0f:%.0f,%.0f,%.0f]",
       e2a, e2@type[[1]],
       e2@D$name[[1]], 0, e2@D$length[[1]] - 1,
                          e1@D$chunk_interval[[1]], e1@D$chunk_overlap[[1]],
       e2@D$name[[2]], 0, e2@D$length[[2]] - 1,
                          e1@D$chunk_interval[[2]], e1@D$chunk_overlap[[2]])
    q2 = sprintf("repart(%s, %s)", q2, schema)

# Handle sparsity by cross-merging data (full outer join):
    if(l==l1)
    {
      if(is.sparse(e1))
      {
        q1 = sprintf("merge(%s,project(apply(%s,__zero__,%s(0)),__zero__))",q1,q2,e1@type)
      }
      if(is.sparse(e2))
      {
        q2 = sprintf("merge(%s,project(apply(%s,__zero__,%s(0)),__zero__))",q2,q1,e2@type)
      }
    }
  }
  p1 = p2 = ""
# Syntax sugar for exponetiation (map the ^ infix operator to pow):
  if(op=="^")
  {
    p1 = "pow("
    op = ","
    p2 = ")"
  }
# Handle special scalar multiplication case:
  if(length(e1s)==1)
    Q = sprintf("apply(%s,%s, %s %.15f %s %s %s)",q2,v,p1,e1s,op,e2a,p2)
  else if(length(e2s)==1)
    Q = sprintf("apply(%s,%s,%s %s %s %.15f %s)",q1,v,p1,e1a,op,e2s,p2)
  else if(l1==1 && l==2)
  {
# Handle special case similar to, but a bit different than vector recylcing.
# This case requires a dimensional match along the 1st dimensions, and it's
# useful for matrix row scaling.
# First, conformably redimension e1.
    newschema = build_dim_schema(e2,I=1,newnames=e1@D$name[1])
    re1 = sprintf("redimension(%s,%s%s)",q1,build_attr_schema(e1),newschema)
    Q = sprintf("cross_join(%s as e1, %s as e2, e1.%s, e2.%s)", re1, q2, e1@D$name[1], e2@D$name[1])
    Q = sprintf("apply(%s, %s, %s e1.%s %s e2.%s %s)", Q,v,p1,e1a,op,e2a,p2)
  }
  else
  {
    Q = sprintf("join(%s as e1, %s as e2)", q1, q2)
    Q = sprintf("apply(%s, %s, %s e1.%s %s e2.%s %s)", Q,v,p1,e1a,op,e2a,p2)
  }
  Q = sprintf("project(%s, %s)",Q,v)
  .scidbeval(Q, eval=FALSE, gc=TRUE, depend=depend)
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
  if(!(inherits(e1,"scidb") || inherits(e1,"scidbdf"))) stop("Sorry, not yet implemented.")
  if(inherits(e2,"scidb")) return(.joincompare(e1,e2,op))
#  type = names(.scidbtypes[.scidbtypes==e1@type])
#  if(length(type)<1) stop("Unsupported data type.")
  op = gsub("==","=",op,perl=TRUE)
  if(is.scidb(e1))
    query = sprintf("filter(%s, %s %s %s)",e1@name, e1@attribute, op, e2)
  else
    query = sprintf("filter(%s, %s %s %s)",e1@name, e1@attributes[[1]], op, e2)
  .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(e1))
}

.joincompare = function(e1,e2,op)
{
  stop("Yikes! Not implemented yet...")
}

tsvd = function(x,nu,tol=0.0001,maxit=20)
{
  m = ceiling(nrow(x)/1e6)
  n = ceiling(ncol(x)/1e6)
  schema = sprintf("[%s=0:%.0f,%.0f,0,%s=0:%.0f,%.0f,0]",
                     x@D$name[1], nrow(x)-1, m,
                     x@D$name[2], ncol(x)-1, ncol(x))
  tschema = sprintf("[%s=0:%.0f,%.0f,0,%s=0:%.0f,%.0f,0]",
                     x@D$name[2], ncol(x)-1, n,
                     x@D$name[1], nrow(x)-1, nrow(x))
  schema = sprintf("%s%s",build_attr_schema(x), schema)
  tschema = sprintf("%s%s",build_attr_schema(x), tschema)
  query  = sprintf("tsvd(redimension(unpack(%s,row),%s), redimension(unpack(transpose(%s),row),%s), %.0f, %f, %.0f)", x@name, schema, x@name, tschema, nu,tol,maxit)
  narray = .scidbeval(query, eval=TRUE, gc=TRUE)
  ans = list(u=slice(narray, "matrix", 0,eval=FALSE)[,between(0,nu-1)],
             d=slice(narray, "matrix", 1,eval=FALSE)[between(0,nu-1),between(0,nu-1)],
             v=slice(narray, "matrix", 2,eval=FALSE)[between(0,nu-1),],
             narray=narray)
  attr(ans$u,"sparse") = TRUE
  attr(ans$d,"sparse") = TRUE
  attr(ans$v,"sparse") = TRUE
  ans
}

svd_scidb = function(x, nu=min(dim(x)), nv=nu)
{
  got_tsvd = length(grep("tsvd",.scidbenv$ops[,2]))>0
  if(missing(nu)) nu = min(dim(x))
  if(!is.sparse(x) && (nu > (min(dim(x))/3)) || !got_tsvd)
  {
# Compute the full SVD
    u = tmpnam()
    d = tmpnam()
    v = tmpnam()
    schema = sprintf("[%s=0:%.0f,1000,0,%s=0:%.0f,1000,0]",
                     x@D$name[1],x@D$length[1]-1,
                     x@D$name[2],x@D$length[2]-1)
    schema = sprintf("%s%s",build_attr_schema(x),schema)
    iquery(sprintf("store(gesvd(repart(%s,%s),'left'),%s)",x@name,schema,u))
    iquery(sprintf("store(gesvd(repart(%s,%s),'values'),%s)",x@name,schema,d))
    iquery(sprintf("store(transpose(gesvd(repart(%s,%s),'right')),%s)",x@name,schema,v))
    return(list(u=scidb(u,gc=TRUE),d=scidb(d,gc=TRUE),v=scidb(v,gc=TRUE)))
  }
  warning("Using the IRLBA truncated SVD algorithm")
  return(tsvd(x,nu))
}


# Miscellaneous functions
log_scidb = function(x, base=exp(1))
{
  w = x@types == "double"
  if(!any(w)) stop("requires at least one double-precision valued attribute")
  if(class(x) %in% "scidb") attr = x@attribute
  else attr = x@attributes[which(w)[[1]]]
  new_attribute = sprintf("%s_log",attr)
  if(base==exp(1))
  {
    query = sprintf("apply(%s, %s, log(%s))",x@name, new_attribute, attr)
  } else if(base==10)
  {
    query = sprintf("apply(%s, %s, log10(%s))",x@name, new_attribute, attr)
  }
  else
  {
    query = sprintf("apply(%s, %s, log(%s)/log(%.15f))",x@name, new_attribute, attr, base)
  }
  ans = .scidbeval(query,`eval`=FALSE)
  if(class(x) %in% "scidb") ans@attribute = new_attribute
  ans
}

# S4 method conforming to standard generic trig functions. See help for
# details about attribute selection and naming.
fn_scidb = function(x,fun,attr)
{
  if(missing(attr))
  {
    w = x@types == "double"
    if(!any(w)) stop("requires at least one double-precision valued attribute")
    if(class(x) %in% "scidb") attr = x@attribute
    else attr = x@attributes[which(w)[[1]]]
  }
  new_attribute = sprintf("%s_%s",attr,fun)
  query = sprintf("apply(%s, %s, %s(%s))",x@name, new_attribute, fun, attr)
  ans = .scidbeval(query,`eval`=FALSE,gc=TRUE,depend=list(x))
  if(class(x) %in% "scidb") ans@attribute = new_attribute
  ans
}

# S3 Method conforming to usual diff implementation. The `differences`
# argument is not supported here.
diff.scidb = function(x, lag=1, ...)
{
  y = lag(x,lag)
  n = make.unique_(c(x@attributes,y@attributes),"diff")
  z = merge(y,x,by=x@D$name[1],all=FALSE)
  expr = paste(z@attributes,collapse=" - ")
  project(bind(z, n, expr), n)
}
