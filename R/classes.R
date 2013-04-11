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

# A general SciDB array class for R. It's a hybrid S4 class with some S3
# methods. The class can represent SciDB arrays and array promises.
# slots:
# name = any SciDB expression that can produce an array 
# schema = the corresponding SciDB array schema for 'name' above
# D = dimensions data derived from the schema
# dim = R dim vector derived from the schema
# length = number of elements derived from the schema
# attribute = attribute in use or 0-length string, in which case the 
#             1st listed attribute is used specified by user for  objects that
#             can only work with one attribute at a time (linear algebra)
# attributes = table (data frame) of array attributes parsed from schema
# type = SciDB type of the attribute in use (character)
# types = list of SciDB types of all attributes (character)
# gc = environment
#      If gc$remove = TRUE, remove SciDB array when R gc is run on object.
#      The gc environment also stores dependencies required by array promises.

setClassUnion("numericOrNULL", c("numeric", "NULL")) 
setClass("scidb",
         representation(name="character",
                        schema="character",
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


setClass("scidbdf",
         representation(name="character",
                        schema="character",
                        D="list",
                        dim="numericOrNULL",
                        length="numeric",
                        attributes="character",
                        nullable="logical",
                        types="character",
                        colClasses="character",
                        gc="environment"),
         S3methods=TRUE)
