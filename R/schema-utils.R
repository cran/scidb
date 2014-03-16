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

# Functions for parsing and building SciDB schema strings.
# A SciDB schema string looks like:
# <attribute_1:type_1 NULL DEFAULT VALUE, attribute_2:type_2, ...>
# [dimension_1=start:end,chunksize,overlap, dimension_2=start:end,chunksize,overlap, ...]

# utility
.dimsplitter = function(x)
{
  if(is.character(x)) s = x
  else
  {
    if(!(inherits(x,"scidb") || inherits(x,"scidbdf"))) return(NULL)
    s = schema(x)
  }
  d = gsub("\\]","",strsplit(s,"\\[")[[1]][[2]])
  strsplit(strsplit(d,"=")[[1]],",")
}

.attsplitter = function(x)
{
  if(is.character(x)) s = x
  else
  {
    if(!(inherits(x,"scidb") || inherits(x,"scidbdf"))) return(NULL)
    s = schema(x)
  }
  strsplit(strsplit(strsplit(strsplit(s,">")[[1]][1],"<")[[1]][2],",")[[1]],":")
}

# Return a vector of SciDB attribute names of x. This is the implementation
# of the names method for scidb and scidbdf objects.
scidb_attributes = function(x)
{
  a = .attsplitter(x)
  unlist(lapply(a,function(x) x[[1]]))
}

# Return a vector of SciDB attribute types of x.
scidb_types = function(x)
{
  a = .attsplitter(x)
  unlist(lapply(a, function(x) strsplit(x[2]," ")[[1]][1]))
}

# Return a logical vector indicating attribute nullability
scidb_nullable = function(x)
{
  a = .attsplitter(x)
  unlist(lapply(a, function(x) length(strsplit(x[2]," ")[[1]])>1))
}

# Return a vector of dimension names of the SciDB object x.
dimensions = function(x)
{
  d = .dimsplitter(x)
  unlist(lapply(d[-length(d)],function(x)x[[length(x)]]))
}

# Returns a list of character-valued vectors of starting and
# ending coordinate bounds
scidb_coordinate_bounds = function(x)
{
  d = .dimsplitter(x)
  start = unlist(lapply(d[-1],function(x)strsplit(x[1],":")[[1]][1]))
  end = unlist(lapply(d[-1],function(x)strsplit(x[1],":")[[1]][2]))
  s1 = gsub("\\*",.scidb_DIM_MAX,start)
  s2 = gsub("\\*",.scidb_DIM_MAX,end)
  len = as.numeric(s2) - as.numeric(s1) + 1
  list(start=noE(start), end=noE(end), length=noE(len))
}

# A between-style string of coordinate bounds
between_coordinate_bounds = function(s)
{
  if((inherits(s,"scidb") || inherits(s,"scidbdf"))) s = schema(s)
  paste(t(matrix(unlist(lapply(strsplit(gsub("\\].*","",gsub(".*\\[","",s,perl=TRUE),perl=TRUE),"=")[[1]][-1],function(x)strsplit(strsplit(x,",")[[1]][1],":")[[1]])),2,byrow=FALSE)),collapse=",")
}

scidb_coordinate_start = function(x)
{
  scidb_coordinate_bounds(x)$start
}

scidb_coordinate_end = function(x)
{
  scidb_coordinate_bounds(x)$end
}

scidb_coordinate_chunksize = function(x)
{
  d = .dimsplitter(x)
  unlist(lapply(d[-1],function(x)x[2]))
}

scidb_coordinate_overlap = function(x)
{
  d = .dimsplitter(x)
  unlist(lapply(d[-1],function(x)x[3]))
}

# Return the SciDB schema of x
schema = function(x)
{
  if(!(inherits(x,"scidb") || inherits(x,"scidbdf"))) return(NULL)
  gsub(".*<","<",x@schema)
}

# Construct a scidb promise from a SciDB schema string s.
# s: schema character string
# expr: SciDB expression or array name
# data.frame: logical
scidb_from_schemastring = function(s, expr=character(), `data.frame`)
{
  attributes = scidb_attributes(s)
  dimensions = dimensions(s)
  if(missing(`data.frame`)) `data.frame` = ( (length(dimensions)==1) &&  (length(attributes)>1))
  if(length(dimensions)>1 && `data.frame`)
    stop("SciDB data frame objects can only be associated with 1-D SciDB arrays")

  if(`data.frame`)
  {
# Set default column types
    return(new("scidbdf",
                schema=gsub("^.*<","<",s,perl=TRUE),
                name=expr,
                attributes=attributes,
                dimensions=dimensions,
                gc=new.env()))
  }

  new("scidb",
      name=expr,
      schema=gsub("^.*<","<",s,perl=TRUE),
      attributes=attributes,
      dimensions=dimensions,
      gc=new.env())
}


# Build the attribute part of a SciDB array schema from a scidb, scidbdf object.
# Set prefix to add a character prefix to all attribute names.
# I: optional vector of dimension indices to use, if missing use all
# newnames: optional vector of new dimension names, must be the same length
#    as I.
# nullable: optional vector of new nullability expressed as FALSE or TRUE,
#    must be the same length as I.
build_attr_schema = function(A, prefix="", I, newnames, nullable)
{
  if(missing(I) || length(I)==0) I = rep(TRUE,length(scidb_attributes(A)))
  if(!(class(A) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  if(is.logical(I)) I = which(I)
  N = rep("", length(scidb_nullable(A)[I]))
  N[scidb_nullable(A)[I]] = " NULL"
  if(!missing(nullable))
  {
    N = rep("", length(I))
    N[nullable] = " NULL"
  }
  N = paste(scidb_types(A)[I],N,sep="")
  attributes = paste(prefix,scidb_attributes(A)[I],sep="")
  if(!missing(newnames)) attributes = newnames
  S = paste(paste(attributes,N,sep=":"),collapse=",")
  sprintf("<%s>",S)
}

# Build the dimension part of a SciDB array schema from a scidb,
# scidbdf object.
# A: A scidb or scidbdf object
# bracket: if TRUE, enclose dimension expression in square brackets
# I: optional vector of dimension indices to use, if missing use all
# newnames, newstart, newend, newchunk, newoverlap, newlen:
# All optional. Must be same length as I. At most one of newlen,newend
# may be specified.
build_dim_schema = function(A, bracket=TRUE, I,
                            newnames, newlen, newstart,
                            newend, newchunk, newoverlap)
{
  if(!(class(A) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")

  dims = dimensions(A)
  bounds = scidb_coordinate_bounds(A)
  start =  bounds$start
  end = bounds$end
  chunksize = scidb_coordinate_chunksize(A)
  overlap = scidb_coordinate_overlap(A)

  if(!missing(I) && length(I)>0)
  {
    dims      = dims[I]
    start     = start[I]
    end       = end[I]
    chunksize = chunksize[I]
    overlap   = overlap[I]
  }
  if(!missing(newnames))
  {
    dims = newnames
  }
  if(!missing(newstart))
  {
    start = noE(newstart)
  }
  if(!missing(newend))
  {
    if(!missing(newlen)) stop("At most one of newend, newlen may be specified")
    end = newend
  }
  if(!missing(newchunk))
  {
    chunksize = noE(newchunk)
  }
  if(!missing(newoverlap))
  {
    overlap = noE(newoverlap)
  }
  if(!missing(newlen))
  {
    star = grep("\\*",newlen)
    len = gsub("\\*",.scidb_DIM_MAX, newlen)
    end = noE(as.numeric(start) + as.numeric(len) - 1)
    if(length(star)>0)
    {
      end[star] = "*"
    }
  }
  R = paste(start,end, sep=":")
  S = paste(dims, R, sep="=")
  S = paste(S, chunksize, sep=",")
  S = paste(S, overlap, sep=",")
  S = paste(S, collapse=",")
  if(bracket) S = sprintf("[%s]",S)
  S
}

# A utility function for operations that require a single attribute
# Throws error if a multi-attribute array is specified.
.get_attribute = function(x)
{
  a = scidb_attributes(x)
  if(length(a) > 1) stop("This function requires a single-attribute array. Consider using project.")
  a
}
