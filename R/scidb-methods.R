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

setMethod("%*%",signature(x="scidb", y="scidbdf"),
  function(x,y)
  {
    scidbmultiply(x,cbind(y))
  },
  valueClass="scidb"
)

setMethod("%*%",signature(x="scidb", y="scidb"),
  function(x,y)
  {
    scidbmultiply(x,y)
  },
  valueClass="scidb"
)

setMethod("%*%",signature(x="matrix", y="scidb"),
  function(x,y)
  {
    as.scidb(x,gc=TRUE) %*% y
  },
  valueClass="character"
)

setMethod("%*%",signature(x="scidb", y="matrix"),
  function(x,y)
  {
    x %*% as.scidb(y,gc=TRUE)
  },
  valueClass="character"
)

setMethod("%*%",signature(x="scidb", y="numeric"),
  function(x,y)
  {
    x %*% as.scidb(matrix(y,nrow=ncol(x)), gc=TRUE)
  },
  valueClass="character"
)

setMethod("%*%",signature(x="numeric", y="scidb"),
  function(x,y)
  {
    as.scidb(matrix(x,ncol=nrow(y)), gc=TRUE) %*% y
  },
  valueClass="character"
)

setOldClass("crossprod")
setGeneric("crossprod")
setMethod("crossprod",signature(x="scidb", y="missing"),
  function(x)
  {
    t(x) %*% x
  },
  valueClass="scidb"
)

setOldClass("crossprod")
setGeneric("crossprod")
setMethod("crossprod",signature(x="scidb", y="scidb"),
  function(x,y)
  {
    t(x) %*% y
  },
  valueClass="scidb"
)


setMethod("tcrossprod",signature(x="scidb", y="scidb"),
  function(x,y)
  {
    x %*% t(y)
  },
  valueClass="scidb"
)

setMethod("tcrossprod",signature(x="scidb", y="missing"),
  function(x)
  {
    x %*% t(x)
  },
  valueClass="scidb"
)



# The remaining functions return data to R:
setGeneric("sum")
setMethod("sum", signature(x="scidb"),
function(x)
{
  iquery(sprintf("sum(%s)",x@name),return=TRUE)[,2]
})

setGeneric("mean")
setMethod("mean", signature(x="scidb"),
function(x)
{
  iquery(sprintf("avg(%s)",x@name),return=TRUE)[,2]
})

setGeneric("min")
setMethod("min", signature(x="scidb"),
function(x)
{
  iquery(sprintf("min(%s)",x@name),return=TRUE)[,2]
})

setGeneric("max")
setMethod("max", signature(x="scidb"),
function(x)
{
  iquery(sprintf("max(%s)",x@name),return=TRUE)[,2]
})

setGeneric("sd")
setMethod("sd", signature(x="scidb"),
function(x)
{
  iquery(sprintf("stdev(%s)",x@name),return=TRUE)[,2]
})

setGeneric("var")
setMethod("var", signature(x="scidb"),
function(x)
{
  iquery(sprintf("var(%s)",x@name),return=TRUE)[,2]
})

setGeneric("diag")
setMethod("diag", signature(x="scidb"),
function(x)
{
  D = dim(x)
  if(length(D)>2) stop("diag requires a matrix or vector")
# Two cases
# Case 1: Given a matrix, return its diagonal as a vector.
  if(length(D)==2)
  {
    bschema = sprintf("<%s:double>",x@attribute)
    bschema = sprintf("%s%s",bschema,build_dim_schema(x,newstart=c(0,0)))
    mask = sprintf("build(<%s:double>[%s=%.0f:%.0f,1000000,0],1)",x@attribute,x@D$name[1],0,min(x@D$length)-1)
    mask = sprintf("apply(%s,%s,%s)",mask,x@D$name[2],x@D$name[1])
    mask = sprintf("redimension(%s,%s)",mask, bschema)
    mask = sprintf("attribute_rename(%s,%s,%s)",mask,x@attribute,make.unique_(x@attribute,"v"))
    query = sprintf("project(join(sg(subarray(%s,null,null,null,null),1,-1),%s),%s)",x@name,mask,x@attribute)
    query = sprintf("unpack(%s,%s)",query, make.unique_(x@D$name, "i"))
    query = sprintf("project(%s,%s)",query,x@attribute)
    query = sprintf("sg(subarray(%s,0,%.0f),1,-1)",query,min(x@D$length)-1)
    return(.scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x)))
  }
# Case 2: Given a vector return  diagonal matrix.
  dim2 = make.unique_(x@D$name, "j")
  query = sprintf("build(<%s:%s>[%s=0:%.0f,%.0f,%.0f],nan)",
           make.unique_(x@attributes,"v"), x@type,
           x@D$name[1], x@D$length[1] - 1 , x@D$chunk_interval[1], x@D$chunk_overlap[1])
  query = sprintf("apply(%s,%s,%s)",query,dim2,x@D$name[1])
  query = sprintf("redimension(%s,<%s:%s>[%s=0:%.0f,%.0f,%.0f,%s=0:%.0f,%.0f,%.0f])",
           query, make.unique_(x@attributes,"v"), x@type,
           x@D$name[1], x@D$length[1] - 1 , x@D$chunk_interval[1], x@D$chunk_overlap[1],
           dim2, x@D$length[1] - 1 , x@D$chunk_interval[1], x@D$chunk_overlap[1])
  query = sprintf("cross_join(%s as __X,%s as __Y,__X.%s,__Y.%s)",query,x@name,x@D$name[1],x@D$name[1])
  query = sprintf("project(%s,%s)",query,x@attribute)
  ans = .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
  attr(ans, "sparse") = TRUE
  return(ans)
})

setGeneric("head")
setMethod("head", signature(x="scidb"),
function(x, n=6L, ...)
{
  m = x@D$start
  p = m + n - 1
  limits = lapply(1:length(m), function(j) seq(m[j],p[j]))
  tryCatch(
    do.call(dimfilter,args=list(x=x,i=limits,eval=FALSE))[],
    error = function(e)
    {
      warning("Unsupported data type. Using iquery to display array data.")
      iquery(x@name, return=TRUE, n=n)
    })
})

setGeneric("tail")
setMethod("tail", signature(x="scidb"),
function(x, n=6L, ...)
{
  p = x@D$start + x@D$length - 1
  m = x@D$start + x@D$length - n
  m = unlist(lapply(1:length(m),function(j) max(m[j],x@D$start[j])))
  limits = lapply(1:length(m), function(j) seq(m[j],p[j]))
  do.call(dimfilter,args=list(x=x,i=limits,eval=FALSE))[]
})


setGeneric('is.scidb', function(x) standardGeneric('is.scidb'))
setMethod('is.scidb', signature(x='ANY'),
  function(x) 
  {
    if(inherits(x, "scidb")) return(TRUE)
    FALSE
  }
)
#setMethod('is.scidb', definition=function(x) return(FALSE))

setGeneric('print', function(x) standardGeneric('print'))
setMethod('print', signature(x='scidb'),
  function(x) {
    show(x)
  })

setMethod('show', 'scidb',
  function(object) {
    atr=object@attribute
    if(is.null(dim(object)) || length(dim(object))==1)
      cat("Reference to a SciDB vector of length",object@length,"\n")
    else
      cat("A reference to a ",
          paste(object@dim,collapse="x"),
          "SciDB array\n")
  })

setGeneric("image", function(x,...) x)
setMethod("image", signature(x="scidb"),
function(x, grid=c(500,500), op=sprintf("sum(%s)", x@attribute), na=0, ...)
{
  if(length(dim(x))!=2) stop("Sorry, array must be two-dimensional")
  if(length(grid)!=2) stop("The grid parameter must contain two values")
  blocks = x@D$length
  blocks = blocks/grid
  if(any(blocks<1)) blocks[which(blocks<1)] = 1
  query = sprintf("regrid(project(%s,%s),%.0f,%.0f,%s)",x@name,x@attribute,blocks[1],blocks[2],op)
  A = iquery(query,return=TRUE,n=Inf)
  A[is.na(A[,3]),3] = na
  m = max(A[,1]) + 1
  n = max(A[,2]) + 1
  B = matrix(0,m,n)
  B[A[,1] + A[,2]*m + 1] = A[,3]
  xlbl=(1:ncol(B))*blocks[2]
  xat=seq(from=0,to=1,length.out=ncol(B))
  ylbl=(nrow(B):1)*blocks[1]
  yat=seq(from=0,to=1,length.out=nrow(B))
  image(B,xaxt='n',yaxt='n',...)
  axis(side=1,at=xat,labels=xlbl)
  axis(side=2,at=yat,labels=ylbl)
  B
})

setOldClass("aggregate")
setGeneric("aggregate")
setMethod("aggregate", signature(x="scidb"), aggregate_scidb)

setOldClass("sweep")
setGeneric("sweep")
setMethod("sweep", signature(x="scidb"), sweep_scidb)

setOldClass("apply")
setGeneric("apply")
setMethod("apply", signature(X = "scidb"), apply_scidb)

setMethod("unpack",signature(x="scidb"),unpack_scidb)

setOldClass("reshape")
setGeneric("reshape", function(data,...) data)
setMethod("reshape", signature(data="scidb"), reshape_scidb)

setOldClass("svd")
setGeneric("svd")
setMethod("svd", signature(x="scidb"), svd_scidb)

setOldClass("glm.fit")
setGeneric("glm.fit")
setMethod("glm.fit", signature(x="scidb"), glm_scidb)

# Transpose a matrix or vector
setOldClass("t")
setGeneric("t")
setMethod("t", signature(x="scidb"), 
  function(x)
  {
    query = sprintf("transpose(%s)",x@name)
    .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
  }
)

# Lead or lag a time series
setOldClass("lag")
setGeneric("lag")
setMethod("lag",signature(x="scidb"),
  function(x,k=1,dim=1,eval=FALSE)
  {
    n = make.unique_(c(x@attributes, x@D$name), "n")
    expr = sprintf("%s - %s", x@D$name[dim], k)
    y = bind(x,n,expr)
    start = x@D$start
    start[dim] = start[dim] - k
    names = x@D$name
    names[dim] = n
    schema = sprintf("%s%s", build_attr_schema(x),
      build_dim_schema(x,newstart=start,newnames=names))
    y = redimension(y,schema)
    cschema = sprintf("%s%s",build_attr_schema(x),
                build_dim_schema(y,newnames=x@D$name))
    y = cast(y,cschema)
    b = paste(paste(noE(x@D$start),collapse=","),
        paste(noE(x@D$length-1),collapse=","),sep=",")
    query = sprintf("between(%s,%s)",y@name,b)
    query = sprintf("redimension(%s,%s%s)",query, build_attr_schema(x),build_dim_schema(x))
    .scidbeval(query,eval=FALSE,gc=TRUE,depend=list(x))
  })

# SciDB's regrid and xgrid operators (simple wrappers)
setGeneric("regrid", def=function(x,grid,expr){NULL})
setMethod("regrid", signature(x="scidb"),
  function(x, grid, expr)
  {
    if(missing(expr)) expr = sprintf("avg(%s)",x@attribute)
    query = sprintf("regrid(%s, %s, %s)",
               x@name, paste(noE(grid),collapse=","), expr)
    .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
  })
setGeneric("xgrid", def=function(x,grid){NULL})
setMethod("xgrid", signature(x="scidb"),
  function(x, grid)
  {
    query = sprintf("xgrid(%s, %s)", x@name, paste(noE(grid),collapse=","))
    .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
  })

setMethod("sin",signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "sin")
  })
setMethod("cos",signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "cos")
  })
setMethod("tan",signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "tan")
  })
setMethod("asin",signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "asin")
  })
setMethod("acos",signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "acos")
  })
setMethod("atan",signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "atan")
  })
setMethod("abs",signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "abs")
  })
