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
Ops.scidbdf = function(e1,e2) {
  switch(.Generic,
    '<' = .compare(e1,e2,"<"),
    '<=' = .compare(e1,e2,"<="),
    '>' = .compare(e1,e2,">"),
    '>=' = .compare(e1,e2,">="),
    '==' = .compare(e1,e2,"="),
    '!=' = .compare(e1,e2,"<>"),
    default = stop("Unsupported binary operation.")
  )
}

`cbind.scidbdf` = function(x)
{
  newdim=make.unique_(x@attributes, "j")
  nd = sprintf("%s[%s,%s=0:0,1,0]",build_attr_schema(x) , build_dim_schema(x,bracket=FALSE),newdim)
  redimension(bind(x,newdim,0), nd)
}

colnames.scidbdf = function(x)
{
  x@attributes
}

rownames.scidbdf = function(x)
{
  if(x@D$type[1] != "string") return(c(x@D$start[1],x@D$start[1]+x@D$length[1]-1))
  if(x@D$length[1] > options("scidb.max.array.elements"))
    stop("Result might be too big. Perhaps try a manual query with an iterative result.")
  Q = sprintf("scan(%s:%s)",x@name,x@D$name[1])
  iquery(Q,return=TRUE,n=x@D$length[1]+1)[,2]
}

names.scidbdf = function(x)
{
  x@attributes
}

`names<-.scidbdf` = function(x,value)
{
  old = x@attributes
  if(length(value)!=length(old)) stop(paste("Incorrect number of names (should be",length(old),")"))
  arg = paste(paste(old,value,sep=","),collapse=",")
  query = sprintf("attribute_rename(%s,%s)",x@name,arg)
  iquery(query)
}

dimnames.scidbdf = function(x)
{
  list(rownames.scidbdf(x), x@attributes)
}

`$.scidbdf` = function(x, ...)
{
  M = match.call()
  M[1] = call("[.scidbdf")
  M[4] = as.character(M[3])
  M[3] = expression(NULL)
  eval(M)
}

# Flexible array subsetting wrapper.
# x: A Scidbdf array object
# ...: list of dimensions
# iterative: return a data.frame iterator
# n: if iterative, how many rows to return
# 
`[.scidbdf` = function(x, ..., iterative=FALSE, n=1000)
{
  M = match.call()
  drop = ifelse(is.null(M$drop),TRUE,M$drop)
  M = M[3:length(M)]
  if(!is.null(names(M))) M = M[!(names(M) %in% c("drop","iterative","n"))]
# i shall contain a list of requested index values
  E = parent.frame()
  i = lapply(1:length(M), function(j) tryCatch(eval(M[j][[1]],E),error=function(e)c()))
# User wants this materialized to R...
  if(all(sapply(i,is.null)))
    if(iterative)
    {
      ans = iquery(sprintf("%s",x@name),`return`=TRUE,iterative=TRUE,n=n,excludecol=1,colClasses=x@colClasses)
      return(ans)
    }
    else
    {
      ans = iquery(sprintf("%s",x@name),`return`=TRUE,n=Inf, colClasses=x@colClasses)
      rownames(ans)= ans[,1]
      ans = ans[,-1,drop=FALSE]
      return(ans)
    }
# Not materializing, return a SciDB array
  if(length(i)!=length(dim(x))) stop("Dimension mismatch")
  scidbdf_subset(x,i)
}

`dim.scidbdf` = function(x)
{
  if(length(x@dim)==0) return(NULL)
  d = x@dim
# Try to make arrays with '*' upper bounds seem more reasonable
    if(d[1] - as.numeric(.scidb_DIM_MAX) == 0)
    {
      d[1] = x@D$high - x@D$low + 1
    }
  d
}

`dim<-.scidbdf` = function(x, value)
{
  stop("unsupported")
}


`str.scidbdf` = function(object, ...)
{
  name = substr(object@name,1,20)
  if(nchar(object@name)>20) name = paste(name,"...",sep="")
  cat("SciDB array name: ",name)
  cat("\nSciDB array schema: ",object@schema)
  cat("\nAttributes:\n")
  cat(paste(capture.output(print(data.frame(attribute=object@attributes,type=object@types,nullable=object@nullable))),collapse="\n"))
  cat("\nRow dimension:\n")
  cat(paste(capture.output(print(data.frame(object@D))),collapse="\n"))
  cat("\n")
}

`ncol.scidbdf` = function(x) x@dim[2]
`nrow.scidbdf` = function(x) 
  {
    n = x@dim[1]
# Try to make arrays with '*' upper bounds seem more reasonable
    if(n - as.numeric(.scidb_DIM_MAX) == 0)
    {
      n = x@D$high - x@D$low + 1
    }
    n
  }
`length.scidbdf` = function(x) x@length





# 'si' sequential numeric index range, for example c(1,2,3,4,5)
# 'bi' special between index range, that is a function that returns upper/lower limits
# 'ui' not specified range (everything, by R convention)
# 'ci' lookup-style range, a non-sequential numeric or labeled set, for example
#      c(3,3,1,5,3)   or  c('a1','a3')
scidbdf_subset = function(x, i)
{
  attribute_range = i[[2]]
  if(is.null(attribute_range)) attribute_range = x@attributes
  if(is.numeric(attribute_range))
  {
    attribute_range = x@attributes[attribute_range]
  }

  i = i[[1]]

# How is the row index range specified?
  if(is.null(i))
  {
# Unspecified, return all rows:
    query = x@name
  }
  else if(checkseq(i))
  {
# Sequential numeric index
    query = betweenbound(x,min(i),max(i))
  }
  else if(inherits(i,"function"))
  {
# Bounding box
    r = i()
    query = betweenbound(x, r[1], r[2])
  }
  else
  {
    stop("This kind of indexing is not yet supported.")
  }
  query = sprintf("project(%s, %s)",query, paste(attribute_range,collapse=","))
  .scidbeval(query, `data.frame`=TRUE, gc=TRUE, eval=FALSE, depend=x)
}

betweenbound = function(x, m, n)
{
  ans = sprintf("between(%s, %.0f, %.0f)", x@name, m, n)
# Reset just the upper dimension index (this redimension is really only a
# meta data operation)
  ans = sprintf("redimension(%s,%s[%s=%.0f:%.0f,%.0f,%.0f])", ans, build_attr_schema(x), x@D$name[1], x@D$start[1], n, x@D$chunk_interval[1], x@D$chunk_overlap[1])
}
