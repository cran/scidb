#' SciDB/R Interface
#'
#' @name scidb-package
#' 
#' @description Package options
#'
#' @section Package options:
#' 
#'  options(scidb.prefix=NULL)
#'  
#'# Default shim port and host.
#'
#'  options(scidb.default_shim_port=8080L)
#'  
#'  options(scidb.default_shim_host="localhost")
#'  
#'# How to download arrays and their coordinates. Set scidb.unpack=FALSE
#'# to use apply, which can be faster in some cases when used with aio.
#'
#'  options(scidb.unpack=FALSE)
#'  
#'# Disable SSL certificate host name checking by default. This is important mostly
#'# for Amazon EC2 where hostnames rarely match their DNS names. If you enable this
#'# then the shim SSL certificate CN entry *must* match the server host name for the
#'# encrypted session to work. Set this TRUE for stronger security (help avoid MTM)
#'# in SSL connections.
#'
#'  options(scidb.verifyhost=FALSE)
#'  
#'# List of special DDL operators
#'
#'  options(scidb.ddl=c("create_array", "remove", "rename"))
#'
#' 
#' @useDynLib scidb
#' @seealso \code{\link{scidb}}, \code{\link{iquery}}
#' @docType package
NULL

.onAttach = function(libname, pkgname)
{
  packageStartupMessage("   ____    _ ___  ___\n  / __/___(_) _ \\/ _ )\n _\\ \\/ __/ / // / _  |\n/___/\\__/_/____/____/     Copyright 2016-2017, Paradigm4, Inc.\n\n", domain = NULL, appendLF = TRUE)

  options(scidb.prefix=NULL)
# Default shim port and host.
  options(scidb.default_shim_port=8080L)
  options(scidb.default_shim_host="localhost")
# Binary data parser buffer size
  options(scidb.buffer_size = 5e7)
# How to download arrays and their coordinates. Set scidb.unpack=FALSE
# to use apply, which can be faster in some cases when used with aio.
  options(scidb.unpack=FALSE)
# Disable SSL certificate host name checking by default. This is important mostly
# for Amazon EC2 where hostnames rarely match their DNS names. If you enable this
# then the shim SSL certificate CN entry *must* match the server host name for the
# encrypted session to work. Set this TRUE for stronger security (help avoid MTM)
# in SSL connections.
  options(scidb.verifyhost=FALSE)
# List of special DDL operators
  options(scidb.ddl=c("cancel",
                      "create_array",
                      "remove",
                      "rename",
                      "sync",
                      "add_instances",
                      "remove_instances",
                      "unregister_instances",
                      "create_namespace",
                      "drop_namespace",
                      "move_array_to_namespace",
                      "create_role",
                      "create_user",
                      "drop_namespace",
                      "drop_role",
                      "drop_user",
                      "add_user_to_role",
                      "drop_user_from_role",
                      "set_namespace",
                      "set_role",
                      "set_role_permissions",
                      "store",
                      "verity_user",
                      "create_with_residency"))
}

# Reset the various package options
.onUnload = function(libpath)
{
  options(scidb.buffer_size=c())
  options(scidb.default_shim_port=c())
  options(scidb.default_shim_host=c())
  options(scidb.verifyhost=c())
  options(scidb.ddl=c())
}

# scidb array object type map.
# R type -> SciDB type
.Rtypes = list(
  double="double",
  double="int64",
  double="uint64",
  integer="int32",
  logical="bool",
  character="string"
)

# These types are used to infer dataframe column classes.
# SciDB type -> R type
.scidbtypes = list(
  double="double",
  int64="double",
  uint64="double",
  uint32="double",
  int32="integer",
  int16="integer",
  uint16="integer",
  int8="integer",
  uint8="integer",
  bool="logical",
  string="character",
  char="character",
  datetime="POSIXct",
  binary="raw",
  float="double"
)

.typelen = list(
  double=8,
  integer=4,
  logical=1,
  character=1
)

# SciDB Integer dimension minimum, maximum
.scidb_DIM_MIN = "-4611686018427387902"
.scidb_DIM_MAX = "4611686018427387903"

# To quiet a check NOTE:
if (getRversion() >= "2.15.1")  utils::globalVariables(c("n", "p"))
