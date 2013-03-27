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

setClass("scidbdf",
         representation(call="call",
                        name="character",
                        D="list",
                        dim="numericOrNULL",
                        length="numeric",
                        attributes="character",
                        nullable="logical",
                        types="character",
                        colClasses="character",
                        gc="environment"),
         S3methods=TRUE)

setGeneric("head")
setMethod("head", signature(x="scidbdf"),
function(x, n=6L, ...)
{
  iquery(sprintf("between(%s,%.0f,%.0f)",x@name,x@D$start,n),`return`=TRUE,colClasses=x@colClasses)[,-1]
})

setGeneric("tail")
setMethod("tail", signature(x="scidbdf"),
function(x, n=6L, ...)
{
  iquery(sprintf("between(%s,%.0f,%.0f)",x@name,x@D$start + x@D$length - n - 1,x@D$start + x@D$length-1),`return`=TRUE, colClasses=x@colClasses)[,-1]
})

setGeneric('is.scidbdf', function(x) standardGeneric('is.scidbdf'))
setMethod('is.scidbdf', signature(x='scidbdf'),
  function(x) return(TRUE))
setMethod('is.scidbdf', definition=function(x) return(FALSE))

setGeneric('print', function(x) standardGeneric('print'))
setMethod('print', signature(x='scidbdf'),
  function(x) {
    cat(sprintf("SciDB array %s: %.0f obs. of %d variables.\n",x@name, x@D$length, length(x@attributes)))
  })

setMethod('show', 'scidbdf',
  function(object) {
    cat(sprintf("SciDB array %s: %.0f obs. of %d variables.\n",object@name, object@D$length, length(object@attributes)))
  })

setOldClass("aggregate")
setGeneric("aggregate", function(x,...) standardGeneric("aggregate"))
setMethod("aggregate", signature(x="scidbdf"),
  function(x,formula,FUN)
  {
    data=x
    if(missing(formula)) stop("Usage: aggregate(1D_scidb_array, formula, scidb_aggregate_expression)")
    if(missing(FUN)) stop("Usage: aggregate(1D_scidb_array, formula, scidb_aggregate_expression)")
# This is a hack until list('aggregates') returns type information, right now
# we have to guess!!!
    agtypes = list(approxdc="uint64 NULL",avg="double NULL",count="uint64 NULL",stdev="double NULL",var="double NULL")
    if(missing(FUN)) stop("You must supply a SciDB aggregate expression")
    v = attr(terms(formula),"term.labels")
    r = setdiff(all.vars(formula),v)
    if(r==".") r = setdiff(data@attributes,v)
# Redimension
    A = tmpnam("array")

    agat = strsplit(FUN,",")[[1]]
    agnames = gsub(".* ","", gsub(" *$","",gsub("^ *","",gsub(".*)","",agat))))
    if(any(nchar(agnames))<1) stop("We require that aggregate expressions name outputs, for example count(x) AS cx")
    agfun = tolower(gsub(" *","",gsub("\\(.*","",agat)))
    agtp = data@types[data@attributes %in% agnames]
    agtp = paste(agtp, "NULL")
    J = agfun %in% names(agtypes)
    if(any(J)) agtp[J] = agtypes[agfun[J]]
    attributes = paste(paste(agnames,agtp,sep=":"),collapse=",")

    chunks = rep(1,length(v))
    chunks[length(chunks)] = data@D$length
    dims = paste(v,data@types[data@attributes %in% v],sep="(")
    dims = paste(dims,"*",sep=")=")
    dims = paste(dims,chunks,sep=",")
    dims = paste(dims,",0",sep="")
    dims = paste(dims,collapse=",")
    schema = sprintf("<%s>[%s]",attributes,dims)
    query = sprintf("create_array(%s,%s)",A, schema)
    iquery(query)
    on.exit(tryCatch(scidbremove(A),error=function(e)invisible()))
    query = sprintf("redimension_store(%s,%s,%s)",data@name, A, paste(FUN,collapse=","))
    iquery(query)
    query = sprintf("scan(%s)",A)
    iquery(query, `return`=TRUE)
  }
)
