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

# This file contains functions that map R indexing operations to SciDB queries.
# Examples include subarray and cross_join wrappers.  They are a bit
# complicated in order to support empty-able and NID arrays.

# A utility function that returns TRUE if entries in the numeric vector j
# are sequential and in increasing order.
checkseq = function(j)
{
  if(!is.numeric(j)) return(FALSE)
  !any(diff(j)!=1)
}

# Returns a function that evaluates to a list of bounds
between = function(a,b)
{
  if(missing(b))
  {
    if(length(a)==2)
    {
      b = a[[2]]
      a = a[[1]]
    } else stop("between requires two arguments or a single argument with two elements")
  }
  function() list(a,b)
}


# A utility function that writes a query that adds indices as attributes.
# A is a scidb object, q is a query in process or just the name of the array.
apply_indices = function(A,q=A@name)
{
  i = A@D$name
  a = paste(paste(paste("__",i,sep=""),i,sep=","),collapse=",")
  sprintf("apply(%s,%s)",q, a)
}

# Return a query that converts selected NIDs to integer
# A a scidb object
# idx a list of dimensions numbers to drop NID, or a boolean vector of length
#     equal to the number of dimensions with TRUE indicating drop NID.
# schema_only means only return the new schema string.
# query is a query to be wrapped in drop_nid, defaults to array name.
# nullable is  either "NULL" or "" and indicates nullable on the attribute.
selectively_drop_nid = function(A, idx, schema_only=FALSE, query, nullable)
{
  if(missing(nullable)) nullable = ""
  if(inherits(A,"scidb"))
  {
    D = A@D
    if(missing(nullable))
      nullable = ifelse(A@nullable[A@attributes == A@attribute][[1]],"NULL","")
  }
  else stop("A must be a SciDB reference object")
  if(missing(query)) query = A@name
  if(missing(idx)) idx = rep(TRUE,length(D$name))
  if(is.numeric(idx)) idx = is.na(match(1:length(D$name),idx))
# XXX This is a problem...SciDB can index up to 2^62 - 1, but we can only
# really resolve smaller numbers here since we don't have 64-bit integers in R.
  ulim = "4611686018427387902"
  j=which(D$length>=2^62)
  if(length(j)>0) D$length[j] = ulim
  query = sprintf("project(%s, %s)",query, A@attribute)
#  if(!any(idx)) return(query) # XXX screws up schema_only requests...
# Pass-through non-nid indices
  idx = idx | (D$type %in% "int64")

  schema = lapply(1:length(D$name), function(j) {
    dlen = D$length[j]
    if(idx[j]) {
# Drop NID
      if(is.character(dlen))
        sprintf("%s=%.0f:%s,%.0f,%.0f",D$name[j],D$start[j],dlen,D$chunk_interval[j],D$chunk_overlap[j])
      else
        sprintf("%s=%.0f:%.0f,%.0f,%.0f",D$name[j],D$start[j],dlen+D$start[j]-1,D$chunk_interval[j],D$chunk_overlap[j])
    } else {
# Don't drop NID
      if(is.character(dlen))
        sprintf("%s(%s)=%s,%.0f,%.0f",D$name[j],D$type[j],dlen,D$chunk_interval[j],D$chunk_overlap[j])
      else
        sprintf("%s(%s)=%.0f,%.0f,%.0f",D$name[j],D$type[j],dlen,D$chunk_interval[j],D$chunk_overlap[j])
    }
  })
  schema = paste(schema,collapse=",")
  schema = sprintf("<%s:%s %s>[%s]",A@attribute,A@type,nullable,schema)
  if(schema_only) return(schema)
  sprintf("cast(%s,%s)",query, schema)
}


# Materialize the single-attribute scidb array x to R.
materialize = function(x, default=options("scidb.default.value"), drop=FALSE)
{
  type = names(.scidbtypes[.scidbtypes==x@type])
  if(length(type)<1) stop("Unsupported data type.")
  tval = vector(mode=type,length=1)
# Run quey
  query = selectively_drop_nid(x,rep(TRUE,length(x@D$type)),nullable="NULL")
# Fill out sparsity with default value for return to R. XXX THIS DOES NOT
# WORK FOR NIDs, since merge does not work for NIDs.
  dolabel = TRUE
  if(all(x@D$type=="int64"))
  {
    s = selectively_drop_nid(x,rep(TRUE,length(x@D$type)),nullable="NULL",schema_only=TRUE)
    query = sprintf("merge(%s, build(%s, %s))",query,s,as.character(default))
    dolabel = FALSE
  }
  query = apply_indices(x,query)

  i = paste(rep("int64",length(x@dim)),collapse=",")
#  nl = x@nullable[x@attribute==x@attributes][[1]]
  nl = TRUE
  N = ifelse(nl,"NULL","")

  savestring = sprintf("&save=(%s %s,%s)",x@type,N,i)

  sessionid = tryCatch( scidbquery(query, save=savestring, async=FALSE, release=0),
                    error = function(e) {stop(e)})
# Release the session on exit
  on.exit( GET(paste("/release_session?id=",sessionid,sep=""),async=FALSE) ,add=TRUE)
  host = get("host",envir=.scidbenv)
  port = get("port",envir=.scidbenv)
  n = 1048576
  buf = 1
  BUF = c()

  tryCatch(
    while(length(buf)>0)
    {
      r = sprintf("http://%s:%d/read_bytes?id=%s&n=%.0f",host,port,sessionid,n)
      u = url(r, open="rb")
      buf = readBin(u, what="raw", n=n)
      close(u)
      BUF = c(BUF,buf)
    }, error = function(e) warning(e))

  if(prod(dim(x))>options("scidb.max.array.elements")) stop("Size exceeds options('scidb.max.array.elements')")
  fdim = dim(x)

  A = tryCatch(
    {
      if(dolabel) .scidb2m(BUF,fdim,tval,dim(x),drop,x@D$name,nl,S=x,default=default)
      else .scidb2m(BUF,fdim,tval,dim(x),drop,x@D$name,nl,default=default)
    },
    error = function(e){stop(e)})
  A
}

# dimfilter: The workhorse array subsetting function
# INPUT
# x: A SciDB array reference object
# i: a list of index expressions
# OUTPUT
# returns a new SciDB reference array
#
# dimfilter distinguishes between four kinds of indexing operations:
# 'si' sequential numeric index range, for example c(1,2,3,4,5)
# 'bi' special between index range, that is a function that returns upper/lower limits
# 'ui' not specified range (everything, by R convention)
# 'ci' lookup-style range, a non-sequential numeric or labeled set, for example
#      c(3,3,1,5,3)   or  c('a1','a3')
#
dimfilter = function(x, i)
{
# Partition the indices into class:
# Identify sequential, numeric indices
  si = sapply(i, scidb:::checkseq)
# Identify explicit between-type indices (functions)
  bi = sapply(i, function(x) inherits(x,"function"))
# Unspecified range
  ui = sapply(i,is.null)
# Identify lookup-type indices
  ci = !(si | bi | ui)

# Check for mismatched range and dimension types.
  rt = rangetype(x,i, si, bi, ci)
  q = rt$query
  r = lapply(1:length(bi), function(j)
    {
      if(bi[j])
      {
# Just use the provided range
        rx = unlist(i[j][[1]]())
        if(inherits(rx,"character"))
          rx = paste("'",rx,"'",sep="")
        rx
      }
      else if(si[j] || ci[j])
      {
# numeric or lookup-type range
        rx = c(min(i[j][[1]]),max(i[j][[1]]))
        if(inherits(rx,"character"))
          rx = paste("'",rx,"'",sep="")
        rx
      }
      else
       {
# Unspecified range
         if(x@D$type[j]=="string")
         {
# XXX Is there a better way that avoids the query?
           mx = iquery(sprintf("max(%s:%s)",x@name,x@D$name[j]),return=TRUE)[1,2]
           c("''",sprintf("'%s'",mx))
         }
         else
         {
           c(x@D$start[j],x@D$start[j] + x@D$length[j] - 1)
         }
       }
    })
  r  = unlist(lapply(r,as.character))
  ro = r[seq(from=1,to=length(r),by=2)]
  re = r[seq(from=2,to=length(r),by=2)]
  r = paste(c(ro,re),collapse=",")
  q = sprintf("between(%s,%s)",q,r)

# Return a new scidb array reference
  if(any(ci)) 
  {
    ans = lookup_subarray(x,q,i,ci,rt$mask)
  }
  else
  {
    ans = tmpnam("array")
#    q = sprintf("store(%s,%s)",q,ans)
# We use subarray here to conform with R subsetting
    q = sprintf("store(subarray(%s,%s),%s)",q,r,ans)
    iquery(q)
  }
  scidb(ans,gc=TRUE,`data.frame`=FALSE)
}

# XXX NOT really working with NIDs. Warnings may occur.
# x: SciDB array reference
# q: A query string with a between statement that bounds the selection
# i: list of requested indices from user
# ci: logical vector of length(i) indicating which ones are lookup-type
# mask
lookup_subarray = function(x, q, i, ci, mask)
{
# Create ancillary arrays for each dimension index list.
  n = length(ci)
  if(n>2) stop ("This kind of indexing not yet supported in the R package yet for arrays of dimension > 2, sorry. Use numeric indices instead.")
  xdim = unlist(lapply(ci, function(j) tmpnam()))
  on.exit(scidbremove(xdim[!is.na(xdim)],error=function(e) invisible()),add=TRUE)
  for(j in 1:n)
  {
    if(ci[[j]])
    {
# User-specified indices
      X = data.frame(i[[j]])
      names(X) = "xxx__a"
      if(!is.numeric(i[[j]]))
      {
        df2scidb(X,types="string",nullable=FALSE,name=xdim[j],dimlabel=x@D$name[[j]])
        mask[j] = TRUE
# XXX Don't support returning NIDS yet.
warning("Dimension labels were dropped.")
      } else
      {
        df2scidb(X,types="int64",nullable=FALSE,name=xdim[j],dimlabel=x@D$name[[j]])
      }
    } else # Not lookup-style index
    {
      if(x@D$type[[j]]=="string")
      {
        q1=sprintf("create_array(%s,<value:string>[%s(string)=*,%.0f,0])",xdim[j],x@D$name[[j]],x@D$chunk_interval[[j]])
        iquery(q1)
        q2=sprintf("redimension_store(apply(%s:%s,%s,value),%s)",x@name,x@D$name[[j]],x@D$name[[j]],xdim[j])
        iquery(q2)
      } else # assume integer dimension
      {
         if(!is.null(i[[j]]))
           q1 = sprintf("build(<xxx__a:int64>[%s=%.0f:%.0f,%.0f,0],%s)",x@D$name[[j]],min(i[[j]]),max(i[[j]]),x@D$chunk_interval[[j]],x@D$name[[j]])
         else
           q1 = sprintf("build(<xxx__a:int64>[%s=%.0f:%.0f,%.0f,0],%s)",x@D$name[[j]],x@D$low[[j]],x@D$high[[j]],x@D$chunk_interval[[j]],x@D$name[[j]])
         q1 = sprintf("store(%s,%s)",q1,xdim[j])
         iquery(q1)
      }
    }
  }

  ans = tmpnam("array")
  if(n==1) q = sprintf("lookup(%s, %s)",xdim[1],q)
  else     q = sprintf("lookup(cross(%s,%s),%s)",xdim[1],xdim[2],q)
  lb = x@D$type
  ub = x@D$type
# Adjust for NIDs selectively dropped
  if(any(mask))
  {
    lb[mask] = "int64"
    ub[mask] = "int64"
  }
  li = lb == "int64"
  lb[li] = .scidb_DIM_MIN
  lb[!li] = "''"
  ui = ub == "int64"
  ub[ui] = .scidb_DIM_MAX
  ub[!ui] = "'~~~~~~~~~~~~~~~~~~'"   # XXX only approx upper bound
  lb = paste(lb, collapse=",")
  ub = paste(ub, collapse=",")
  q = sprintf("store(subarray(%s,%s,%s),%s)",q,lb,ub,ans)
  iquery(q)
  ans
}

# x: A SciDB array reference
# i: A list of index ranges
# si: A vector of logicals equal to length of i showing positions with
#     sequential numeric index types.
# bi: A vector showing positions of i with between/subarray-style index types
# ci: A vector showing positions of i with lookup-style types
# Returns a list with two elements:
# $query:  A starting query
# $mismatch: A logical value. TRUE means the schema differs from the
#            input array.
# mask: A vector set to TRUE in positions that have been cast to int64
#
# The rangetype function checks for mismatches between index range types
# and array schema, for example specifying integer indices instead of
# NID. The function tries to harmonize the types with CAST. It will fail
# with an error if the ranges can't be fixed up with CAST.
rangetype = function(x, i, si, bi, ci)
{
  mismatch = FALSE
  schema   = NA
  q        = x@name
  M        = rep(FALSE,length(x@D$type))
# Check si types
  if(any(si))
  {
    sim = x@D$type != "int64" & si
    M   = M | sim
    if(any(sim))
    {
      mismatch = TRUE
      schema = selectively_drop_nid(x,sim, schema_only=TRUE)
      q = selectively_drop_nid(x,sim)
    }
  }
# Check ci types
  if(any(ci))
  {
    mask = x@D$type =="string" & sapply(i,class)=="numeric"
    M   = M | mask
    if(any(mask))
    {
      mismatch = TRUE
      schema = selectively_drop_nid(x,mask, schema_only=TRUE)
      q = selectively_drop_nid(x,mask)
    }
  }
# Check bi types XXX to do

  if(!mismatch) q = sprintf("project(%s,%s)",q,x@attribute)
  list(mismatch=mismatch, query=q, schema=schema, mask=M)
}


# Import data from a binary save into an R matrix.
# x: binary SciDB output file or raw vector
# dim: A vector of the same length as the number of dimensions of the output
#      and such that the product of the entries is the maximum number of
#      possible output elements in the array.
# req: vector of requested dimension indices
# type: R type of output matrix values
# drop: Project away singleton dimensions if TRUE
# dimlabels: Optional vector of output dimension lables
# nullable: (logical) Are the binary output data (SciBD)nullable?
# S: Optional scidb array reference. If present, will label
#    output dimensions,
.scidb2m = function (x, dim, type, req,drop, dimlabels,nullable, S,default=options("scidb.default.value"))
{
  N = ifelse(nullable,1L,0L)
  ans = tryCatch(
   {
    if(is.raw(x))
      .Call('scidb2mnew',x, as.integer(dim), type, N, PACKAGE='scidb')
    else
      .Call('scidb2mnew',as.character(x), as.integer(dim), type, N, PACKAGE='scidb')
   },
  error=function(e) {
    unlink(file)
    stop(e)
  })

# scidb2m returns a list with:
# 1. a vector of values
# 2. a matrix of dimension indices for each value.

# The returned indices are relative to the original SciDB matrix.  Because
# SciDB omits empty values, the returned indices may not exactly match the
# requested indices. Undefined indices are marked NA (that's indices, not
# data). We return the SciDB subarray, which may differ from the usual R array
# subset result.

# XXX This needs to be fixed for sparse arrays, current approach is not good.
# XXX

  f = function(x,i)
  {
    i = sort(i, method="quick")
    match(x,i) - 1
  }
  na2 = na.omit(ans[[2]])
  idx = as.list(data.frame(na2))
  if(!missing(dimlabels)) names(idx)[1:length(dimlabels)] = dimlabels
  dn = lapply(idx,unique)
  dim = unlist(lapply(dn,length))

  # Check for too-big result and return data frame instead.
  if(prod(dim) > options("scidb.max.array.elements")) {
    warning("Number of returned results exceeds scidb.max.array.elements option; returning indexed result table instead.")
    idx$value = ans[[1]]
# XXX Apply NIDs here to indices (TODO)
    return(data.frame(idx))
  }

  if(typeof(unlist(default))!=typeof(type)) default=NA
# This is problematic on R-2.15.2 (dn type changes!!!)
#  A = array(unlist(default), dim=dim,dimnames=dn)
  A = array(unlist(default), dim=dim, dimnames=as.character(dn))
#  dimnames(A) = udn
  if(nrow(na2)>0) {
    nidx = lapply(1:length(dn),function(i) f(idx[[i]],dn[[i]]))
# The above achieves the following, but more quickly:
#   nidx = lapply(idx, function(x) as.integer(factor(x)) - 1)
    off = c(1,cumprod(dim)[-length(dim)])
    aidx = rep(0,length(nidx[[1]]))
    nidx = lapply(1:length(off),function(j) off[j]*nidx[j][[1]])
    for(j in nidx) aidx = aidx + j
    A[aidx + 1] = ans[[1]][1:length(aidx)]
  }
  if(missing(S)) {if(drop) A=drop(A);return(A)}

  w = vector(mode='list',length=length(S@D$type))
  for(j in 1:length(w)) {
    if(length(na2[,j])<1) break
# Create a unique attribute name to apply dimensions into
    a = make.names(c(S@attributes,"dimname"),unique=TRUE,allow_=TRUE)
    a = gsub("\\.","_",a[[length(a)]])
    query = sprintf("aggregate(project(apply(%s,%s,1),%s),count(%s),%s)",
                S@name, a, a, a, S@D$name[j])
# Retrieve the dimension labels
    d = iquery(query,return=TRUE,n=S@D$high[j]-S@D$low[j]+1)[,1]
    w[[j]] = d
  }
  names(w) = S@D$name
# Add a trycatch here?
  dimnames(A) = w
  if(drop) A = drop(A)
  A
}
