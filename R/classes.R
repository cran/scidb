# A general SciDB array class for R. It's a hybrid S4 class with some S3
# methods. The class can represent SciDB arrays and array promises.
#
# A scidb object is fully defined by:
# name = any SciDB expression that can produce an array 
# meta = environment containing SciDB schema (lazily evaluated),
#      If meta$remove = TRUE, remove SciDB array when R gc is run on object.
#      The meta environment also stores dependencies required by array promises, and misc items.

setClass("scidb",
         representation(name="character",
                        meta="environment"),
         S3methods=TRUE)
