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

# This file contains class definitions, generics, and methods for the scidb
# matrix/vector class. It's a hybrid S4 class with some S3 methods. The class
# contains the slots:
# call = call that created the object
# name = SciDB array name
# D = dimensions data
# dim = dim vector
# length = number of elements
# attribute = attribute in use or 0-length string, in which case the 
#             1st listed attribute is used
# attributes = table (data frame) of array attributes
# type = SciDB type of the attribute in use (character)
# types = list of SciDB types of all attributes (character)
# gc = environment
#      If gc$remove = TRUE, remove SciDB array when R gc is run on object.

setClassUnion("numericOrNULL", c("numeric", "NULL")) 
setClass("scidb",
         representation(call="call",
                        name="character",
                        D="list",
                        dim="numericOrNULL",
                        length="numeric",
                        attribute="character",
                        attributes="character",
                        nullable="logical",
                        type="character",
                        types="character",
                        gc="environment"),
         S3methods=TRUE)

setMethod("%*%",signature(x="scidb", y="scidb"),
  function(x,y) scidbmultiply(x,y),
  valueClass="scidb"
)

# XXX add check for type...
setMethod("%*%",signature(x="matrix", y="scidb"),
  function(x,y) {
    on.exit(tryCatch(scidbremove(x@name),error=function(e)invisible()))
    x = as.scidb(x,name=basename(tempfile(pattern="array")),colChunkSize=y@D$chunk_interval[1],start=c(0L,y@D$start[[2]]))
    ans = scidbmultiply(x,y)
    return(ans)
  },
  valueClass="scidb"
)

setMethod("%*%",signature(x="scidb", y="matrix"),
  function(x,y) {
    on.exit(tryCatch(scidbremove(y@name),error=function(e)invisible()))
    y = as.scidb(y,name=basename(tempfile(pattern="array")), rowChunkSize=x@D$chunk_interval[2],start=c(x@D$start[[1]],0L))
    ans = scidbmultiply(x,y)
    ans
  },
  valueClass="scidb"
)

setGeneric("crossprod")
setMethod("crossprod",signature(x="scidb", y="scidb"),
  function(x,y) t(x) %*% y,
  valueClass="scidb"
)

setGeneric("crossprod")
setMethod("crossprod",signature(x="matrix", y="scidb"),
  function(x,y) t(x) %*% y,
  valueClass="scidb"
)

setGeneric("crossprod")
setMethod("crossprod",signature(x="scidb", y="matrix"),
  function(x,y) t(x) %*% y,
  valueClass="scidb"
)

setGeneric("tcrossprod")
setMethod("tcrossprod",signature(x="scidb", y="scidb"),
  function(x,y) x %*% t(y),
  valueClass="scidb"
)

setGeneric("tcrossprod")
setMethod("tcrossprod",signature(x="matrix", y="scidb"),
  function(x,y) x %*% t(y),
  valueClass="scidb"
)

setGeneric("tcrossprod")
setMethod("tcrossprod",signature(x="scidb", y="matrix"),
  function(x,y) x %*% t(y),
  valueClass="scidb"
)


setGeneric("count",function(x) sum(!is.na(x)))
setMethod("count", signature(x="scidb"),
function(x)
{
  iquery(sprintf("count(%s)",x@name),return=TRUE)$count
})

setGeneric("head")
setMethod("head", signature(x="scidb"),
function(x, n=6L, ...)
{
  do.call(scidb:::dimfilter,args=list(x=x,i=rep(list(as.integer(1:n)),length(x@dim))))[]
})

setGeneric("tail")
setMethod("tail", signature(x="scidb"),
function(x, n=6L, ...)
{
  do.call(scidb:::dimfilter,args=list(x=x,i=rep(list(max((nrow(x)-n),1):nrow(x)),length(x@dim))))[]
})


setGeneric('is.scidb', function(x) standardGeneric('is.scidb'))
setMethod('is.scidb', signature(x='scidb'),
  function(x) return(TRUE))
setMethod('is.scidb', definition=function(x) return(FALSE))

setGeneric('print', function(x) standardGeneric('print'))
setMethod('print', signature(x='scidb'),
  function(x) {
    cat("A reference to a ",paste(nrow(x),ncol(x),sep="x"),
        "dimensional SciDB array\n")
  })

setMethod('show', 'scidb',
  function(object) {
    atr=object@attribute
    if(is.null(dim(object)) || length(dim(object))==1)
      cat("Reference to the SciDB vector.attribute",
          paste(object@name,atr,sep=".")," of length",object@length,"\n")
    else
      cat("A reference to a ",
          paste(object@dim,collapse="x"),
          "dimensional SciDB array\n")
  })

setGeneric("image", function(x,...) x)
setMethod("image", signature(x="scidb"),
function(x, grid=c(x@D$chunk_interval[1], x@D$chunk_interval[2]), op=sprintf("sum(%s)",x@attribute), na=0, ...)
{
  if(length(dim(x))!=2) stop("Sorry, array must be two-dimensional")
  if(length(grid)!=2) stop("The grid parameter must contain two values")
  blocks = c(x@D$high[1] - x@D$low[1] + 1, x@D$high[2] - x@D$low[2] + 1)
  blocks = blocks/grid
  query = sprintf("regrid(project(%s,%s),%.0f,%.0f,%s)",x@name,x@attribute,blocks[1],blocks[2],op)
  A = iquery(query,return=TRUE)
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
