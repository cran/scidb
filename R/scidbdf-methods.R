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

setGeneric("head")
setMethod("head", signature(x="scidbdf"),
function(x, n=6L, ...)
{
  iquery(sprintf("between(%s,%.0f,%.0f)",x@name,x@D$start,x@D$start + n - 1),`return`=TRUE,colClasses=x@colClasses)[,-1]
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
    show(x)
  })

setMethod('show', 'scidbdf',
  function(object) {
    l = length(object@attributes)
    v = ifelse(l<2, "variable", "variables")
    cat(sprintf("SciDB 1-D array: %.0f obs. of %d %s.\n", (object@D$length), length(object@attributes),v))
  })

setMethod("aggregate", signature(x="scidbdf"), aggregate_scidb)


# The following methods return data to R
setGeneric("sum")
setMethod("sum", signature(x="scidbdf"),
function(x)
{
  iquery(sprintf("sum(%s)",x@name),return=TRUE)[,2]
})

setGeneric("mean")
setMethod("mean", signature(x="scidbdf"),
function(x)
{
  iquery(sprintf("avg(%s)",x@name),return=TRUE)[,2]
})

setGeneric("min")
setMethod("min", signature(x="scidbdf"),
function(x)
{
  iquery(sprintf("min(%s)",x@name),return=TRUE)[,2]
})

setGeneric("max")
setMethod("max", signature(x="scidbdf"),
function(x)
{
  iquery(sprintf("max(%s)",x@name),return=TRUE)[,2]
})

setGeneric("sd")
setMethod("sd", signature(x="scidbdf"),
function(x)
{
  iquery(sprintf("stdev(%s)",x@name),return=TRUE)[,2]
})

setGeneric("var")
setMethod("var", signature(x="scidbdf"),
function(x)
{
  iquery(sprintf("var(%s)",x@name),return=TRUE)[,2]
})

log.scidbdf = function(x, base=exp(1))
{
  log_scidb(x,base) 
}

setMethod("sin", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "sin")
  })
setMethod("cos", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "cos")
  })
setMethod("tan", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "tan")
  })
setMethod("asin", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "asin")
  })
setMethod("acos", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "acos")
  })
setMethod("atan", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "atan")
  })
setMethod("abs", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "abs")
  })
