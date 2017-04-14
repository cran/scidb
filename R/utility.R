#' Evaluate an expression to \code{scidb} or \code{scidb} objects
#'
#' Force evaluation of an expression that yields a \code{scidb} or \code{scidb} object,
#' storing the result to a SciDB array when \code{eval=TRUE}.
#' @param db scidb connection object from \code{\link{scidbconnect}}
#' @param expr a quoted SciDB expression \code{scidb} object
#' @param eval \code{FALSE} do not evaluate the expression in SciDB (leave as a view)
#' @param name (character) optional SciDB array name to store result to
#' @param gc (logical) optional, when TRUE tie result to R garbage collector
#' @param temp (logical, optional), when TRUE store as a SciDB temp array
#' @export
store = function(db, expr, name, eval=TRUE, gc=TRUE, temp=FALSE)
{
  ans = eval(expr)
  if (!(inherits(ans, "scidb"))) return(ans)
# If expr is a stored temp array, then re-use its name
  if (!is.null(ans@meta$temp) && ans@meta$temp && missing(name)) name=ans@name
  .scidbeval(db, ans@name, `eval`=eval, name=name, gc=gc, schema=schema(ans), temp=temp)
}

#' Create an R reference to a SciDB array or expression.
#' @param db scidb connection object from \code{\link{scidbconnect}}
#' @param name a character string name of a stored SciDB array or a valid SciDB AFL expression
#' @param gc a logical value, \code{TRUE} means connect the SciDB array to R's garbage collector
#' @param schema optional SciDB array schema, if specified avoid an extra metadata query to determine array schema.
#'        Use this option with care, the schema must exactly match the SciDB array result.
#' @return a \code{scidb} object
#' @importFrom methods new show
#' @export
scidb = function(db, name, gc=FALSE, schema)
{
  stopifnot(inherits(db, "afl"))
  if (missing(name)) stop("array or expression must be specified")
  if (inherits(name, "scidb"))
  {
    query = name@name
    return(.scidbeval(db, name@name, eval=FALSE, gc=gc, depend=list(name)))
  }

  obj = new("scidb", name=name)
  obj@meta = new.env()
  obj@meta$db = db
  if (missing(schema)) delayedAssign("state", lazyeval(db, name), assign.env=obj@meta)
  else assign("state", list(schema=schema), envir=obj@meta)
  delayedAssign("schema", get("state")$schema, eval.env=obj@meta, assign.env=obj@meta)

  if (gc)
  {
    if (length(grep("\\(", name)) == 0)
    {
      obj@meta$name = name
    }
    obj@meta$remove = TRUE
    reg.finalizer(obj@meta, function(e)
        {
          if (e$remove && exists("name", envir=e))
            {
              if (grepl(sprintf("%s$", getuid(e$db)), e$name)) scidbquery(db, sprintf("remove(%s)", e$name), release=1)
            }
        }, onexit = TRUE)
  }
  obj
}


#' Connect to a SciDB database
#' @param host optional host name or I.P. address of a SciDB shim service to connect to
#' @param port optional port number of a SciDB shim service to connect to
#' @param username optional authentication username
#' @param password optional authentication password
#' @param auth_type optional SciDB authentication type
#' @param protocol optional shim protocol type
#' @param int64 logical value, if \code{TRUE} then preserve signed 64-bit SciDB integers
#' as R integer64 values from the bit64 package. Otherwise, 64-bit integers from SciDB
#' are converted to R double values, possibly losing precision.
#' @param doc optional AFL operator/macro documentation (see notes)
#' @note
#' Use the optional \code{username} and \code{password} arguments with
#' \code{auth_type} set to "digest" to use HTTP digest authentication (see the
#' shim documentation to configure this).  Digest authentication may use either
#' "http" or "https" selected by the \code{protocol} setting.
#' Set \code{auth_type = "scidb"} to use SciDB authentication, which only
#' works over "https".
#'
#' Use the returned SciDB connection object (of class \code{afl}) with other
#' package functions to interact with SciDB arrays. Apply R's \code{\link{ls}}
#' function on the returned value to see a list of arrays. The returned value
#' contains a list of available SciDB AFL language operators and macro names.
#' Use the dollar-sign function to accesss those functions.
#'
#' The optional \code{doc} argument may be a three-column data frame with
#' character-valued columns name, signature, and help containing AFL operator
#' names, function signatures, and help strings, respectively. See
#' `data("operators", package="scidb")` for an example.
#'
#' All arguments support partial matching.
#' @return A scidb connection object. Use \code{$} to access AFL operators
#' and macros, \code{ls()} on the returned object to list SciDB arrays,
#' and \code{names()} on the returned object to list all available AFL operators
#' and macros.
#' @examples
#' \dontrun{
#' db <- scidbconnect()
#'
#' # SciDB 15.12 authentication example (using shim's default HTTPS port 8083)
#' db <- scidbconnect(user="root", password="Paradigm4",
#'                    auth_type="scidb", port=8083, protocol="https")
#'
#' # List available AFL operators
#' names(db)
#'
#' # List arrays
#' ls(db)
#'
#' # Explicitly upload an R matrix to SciDB:
#' x <- as.scidb(db, matrix(rnorm(20), 5))
#' # Implicitly do the same as part of an AFL expression
#' y <- db$join(x,  as.scidb(matrix(1:20, 5)))
#' print(y)
#'
#' as.R(y)   # Download a SciDB array to R.
#' }
#' @importFrom digest digest
#' @importFrom openssl base64_encode
#' @seealso \code{\link{scidb_prefix}}
#' @export
scidbconnect = function(host=getOption("scidb.default_shim_host", "127.0.0.1"),
                        port=getOption("scidb.default_shim_port", 8080L),
                        username, password,
                        auth_type=c("scidb", "digest"), protocol=c("http", "https"),
                        int64=FALSE,
                        doc)
{
# Set up a db object
  db = list()
  class(db) = "afl"
  attr(db, "connection") = new.env()
  auth_type = match.arg(auth_type)
  protocol = match.arg(protocol)
  attr(db, "connection")$host = host
  attr(db, "connection")$port = port
  attr(db, "connection")$protocol = protocol
  attr(db, "connection")$int64 = int64

# Update the scidb.version in the db connection environment
  shim.version = SGET(db, "/version")
  v = strsplit(gsub("[A-z\\-]", "", gsub("-.*", "", shim.version)), "\\.")[[1]]
  if (length(v) < 2) v = c(v, "1")
  attr(db, "connection")$scidb.version = sprintf("%s.%s", v[1], v[2])

# set this to TRUE if connecting to an older SciDB version than 16.9
  password_digest = ! at_least(attr(db, "connection")$scidb.version, "16.9")

  if (missing(username)) username = c()
  if (missing(password)) password = c()
# Check for login using either scidb or HTTP digest authentication
  if (!is.null(username))
  {
    attr(db, "connection")$authtype = auth_type
    attr(db, "connection")$authenv = new.env()
    if (auth_type=="scidb")
    {
      attr(db, "connection")$protocol = "https"
      attr(db, "connection")$username = username
      if (password_digest)
        attr(db, "connection")$password = base64_encode(digest(charToRaw(password), serialize=FALSE, raw=TRUE, algo="sha512"))
      else #16.9 no longer hashes the password
        attr(db, "connection")$password = password
    } else # HTTP basic digest auth
    {
      attr(db, "connection")$digest = paste(username, password, sep=":")
    }
  }

# Use the query ID from a query as a unique ID for automated
# array name generation.
  x = tryCatch(
        scidbquery(db, query="list('libraries')", release=1, resp=TRUE),
        error=function(e) stop("Connection error"), warning=invisible)
  if (is.null(attr(db, "connection")$id))
  {
    id = tryCatch(strsplit(x$response, split="\\r\\n")[[1]],
           error=function(e) stop("Connection error"), warning=invisible)
    attr(db, "connection")$id = id[[length(id)]]
  }

# Update available operators and macros and return afl object
  ops = iquery(db, "merge(redimension(project(list('operators'), name), <name:string>[i=0:*,1000000,0]), redimension(apply(project(list('macros'), name), i, No + 1000000), <name:string>[i=0:*,1000000,0]))", `return`=TRUE, binary=FALSE)[, 2]
  attr(db, "connection")$ops = ops
  if (missing(doc)) return (update.afl(db, ops))

  attr(db, "connection")$doc = doc
  update.afl(db, ops, doc)
}


# binary=FALSE is needed by some queries, don't get rid of it.
#' Run a SciDB query, optionally returning the result.
#' @param db a scidb database connection from \code{\link{scidbconnect}}
#' @param query a single SciDB query string or scidb array object
#' @param return if \code{TRUE}, return the result
#' @param binary set to \code{FALSE} to read result from SciDB in text form
#' @param ... additional options passed to \code{read.table} when \code{binary=FALSE},
#' or optional result schema when \code{binary=TRUE} (see note below).
#' @note When \code{query} is an arbitrary AFL query string and \code{binary=TRUE},
#'   optionally specify \code{schema} with a valid result array schema to skip
#'   an extra metadata lookup query (see \code{\link{scidb}}).
#'
#' Setting \code{return=TRUE} wrapes the AFL \code{query} expression with a SciDB
#' save operator, saving the data on the SciDB server in either binary or text
#' format depending on the value of the \code{binary} parameter. Please note that
#' some AFL expressions may not be "saved" using the AFL save operator, including
#' for instance the AFL create_array operator. Trying to return the result of such
#' a SciDB expression will result in a run-time error.
#' @seealso \code{\link{scidb}} \code{\link{as.R}}
#' @importFrom utils read.table
#' @examples
#' \dontrun{
#' db <- scidbconnect()
#' iquery(db, "build(<v:double>[i=1:5], sin(i))", return=TRUE)
#'## i          v
#'## 1  0.8414710
#'## 2  0.9092974
#'## 3  0.1411200
#'## 4 -0.7568025
#'## 5 -0.9589243
#'
#' # Use binary=FALSE and additional options to read.table function:
#' iquery(db, "build(<val:string>[i=1:3], '[(01),(02),(03)]', true)",xi
#'        return=TRUE, binary=FALSE, colClasses=c("integer", "character"))
#'##   i val
#'## 1 1  01
#'## 2 2  02
#'## 3 3  03
#' }
#' @export
iquery = function(db, query, `return`=FALSE, binary=TRUE, ...)
{
  DEBUG = getOption("scidb.debug", FALSE)
  if (inherits(query, "scidb"))  query = query@name
  n = -1    # Indicate to shim that we want all the output
  if (`return`)
  {
    if (binary) return(scidb_unpack_to_dataframe(db, query, ...))

    ans = tryCatch(
       {
        # SciDB save syntax changed in 15.12
        if (at_least(attr(db, "connection")$scidb.version, 15.12))
        {
          sessionid = scidbquery(db, query, save="csv+:l", release=0)
        } else sessionid = scidbquery(db, query, save="csv+", release=0)
        dt1 = proc.time()
        result = tryCatch(
          {
            SGET(db, "/read_lines", list(id=sessionid, n=as.integer(n + 1)))
          },
          error=function(e)
          {
             SGET(db, "/cancel", list(id=sessionid))
             SGET(db, "/release_session", list(id=sessionid), err=FALSE)
             stop(e)
          })
        SGET(db, "/release_session", list(id=sessionid), err=FALSE)
        if (DEBUG) cat("Data transfer time", (proc.time() - dt1)[3], "\n")
        dt1 = proc.time()
# Handle escaped quotes
        result = gsub("\\\\'", "''", result, perl=TRUE)
        result = gsub("\\\\\"", "''", result, perl=TRUE)
# Map SciDB missing (aka null) to NA, but preserve DEFAULT null.
# This sucky parsing is not a problem for binary transfers.
        result = gsub("DEFAULT null", "@#@#@#kjlkjlkj@#@#@555namnsaqnmnqqqo", result, perl=TRUE)
        result = gsub("null", "NA", result, perl=TRUE)
        result = gsub("@#@#@#kjlkjlkj@#@#@555namnsaqnmnqqqo", "DEFAULT null", result, perl=TRUE)
        val = textConnection(result)
        on.exit(close(val), add=TRUE)
        ret = c()
        if (length(val) > 0)
        {
          args = list(file=val, ..., sep=",", stringsAsFactors=FALSE, header=TRUE)
          args$only_attributes = NULL
          args = args[! duplicated(names(args))]
          ret = tryCatch(do.call("read.table", args=args),
                error = function(e) stop("Query result parsing error ", as.character(e)))
        }
        if (DEBUG) cat("R parsing time", (proc.time()-dt1)[3], "\n")
        ret
       }, error = function(e)
           {
             stop(e)
           }, warning=invisible)
      return(ans)
  } else
  {
    scidbquery(db, query, release=1, stream=0L)
  }
  invisible()
}


#' Upload R data to SciDB
#' @param db a scidb database connection returned from \code{\link{scidbconnect}}
#' @param x an R data frame, raw value, Matrix, matrix, or vector object
#' @param name a SciDB array name to use
#' @param start starting SciDB integer coordinate index (does not apply to data frames)
#' @param gc set to FALSE to disconnect the SciDB array from R's garbage collector
#' @param ... other options, see \code{\link{df2scidb}}
#' @note Supported R objects include data frames, scalars, vectors, dense matrices,
#' and double-precision sparse matrices of class CsparseMatrix. Supported R scalar
#' types and their resulting SciDB types are:
#'  \itemize{
#'  \item{integer   -> }{int32}
#'  \item{logical   -> }{int32}
#'  \item{character -> }{string}
#'  \item{double    -> }{double}
#'  \item{integer64 -> }{int64}
#'  \item{raw       -> }{binary}
#'  \item{Date      -> }{datetime}
#' }
#' R factor values are converted to their corresponding character levels.
#' @seealso \code{\link{as.R}}
#' @return A \code{scidb} object
#' @export
as.scidb = function(db, x,
                    name,
                    start,
                    gc=TRUE, ...)
{
  if (missing(name))
  {
    name = tmpnam(db)
  }
  if (inherits(x, "raw"))
  {
    return(raw2scidb(db, x, name=name, gc=gc, ...))
  }
  if (inherits(x, "data.frame"))
  {
    return(df2scidb(db, x, name=name, gc=gc, ...))
  }
  if (inherits(x, "dgCMatrix"))
  {
    return(.Matrix2scidb(db, x, name=name, start=start, gc=gc, ...))
  }
  return(matvec2scidb(db, x, name=name, start=start, gc=gc, ...))
}

#' Download SciDB data to R
#' @param x a \code{\link{scidb}} object (a SciDB array or expression)
#' @param only_attributes optional logical argument, if \code{TRUE} do not download SciDB dimensions (see note)
#' @param binary optional logical value, set to \code{FALSE} to download data using text format (useful for some unsupported SciDB types)
#' @return An R \code{\link{data.frame}}
#' @note This convenience function is equivalent to running \code{iquery(db, x, return=TRUE)} for
#' a SciDB connection object \code{db}.
#'
#' The \code{only_attributes=TRUE} option only works with binary transfers, and if specified will set \code{binary=TRUE}.
#' Beware of the \code{only_attributes=TRUE} setting--SciDB may return data in arbitrary order.
#'
#' SciDB values are always returned as R data frames. SciDB scalar types are converted to
#' corresponding R types as follows:
#'  \itemize{
#'    \item{double   -> }{double}
#'    \item{int64    -> }{integer64}
#'    \item{uint64   -> }{double}
#'    \item{uint32   -> }{double}
#'    \item{int32    -> }{integer}
#'    \item{int16    -> }{integer}
#'    \item{unit16   -> }{integer}
#'    \item{int8     -> }{integer}
#'    \item{uint8    -> }{integer}
#'    \item{bool     -> }{logical}
#'    \item{string   -> }{character}
#'    \item{char     -> }{character}
#'    \item{binary   -> }{raw}
#'    \item{datetime -> }{Date}
#' }
#' @seealso \code{\link{as.scidb}}
#' @examples
#' \dontrun{
#' db <- scidbconnect()
#' x <- scidb(db, "build(<v:double>[i=1:5], sin(i))")
#' as.R(x)
#'## i          v
#'## 1  0.8414710
#'## 2  0.9092974
#'## 3  0.1411200
#'## 4 -0.7568025
#'## 5 -0.9589243
#'
#' as.R(x, only_attributes=TRUE)
#'##          v
#'##  0.8414710
#'##  0.9092974
#'##  0.1411200
#'## -0.7568025
#'## -0.9589243
#' }
#' @export
as.R = function(x, only_attributes=FALSE, binary=TRUE)
{
  stopifnot(inherits(x, "scidb"))
  if (is.null(schema(x, "dimensions"))) only_attributes = TRUE
  if (only_attributes) return(scidb_unpack_to_dataframe(x@meta$db, x, only_attributes=TRUE))
  scidb_unpack_to_dataframe(x@meta$db, x, binary=binary)
}

#' Register an AFL prefix expression
#'
#' SciDB AFL statements are normally executed in a stateless query context.
#' Use scidb_prefix to create compound AFL expressions useful in some
#' circumstances.
#' @param db a scidb database connection returned from \code{\link{scidbconnect}}
#' @param expression a valid AFL expression to be issued prior to, and in the same context as all subsequent
#' query expressions issued to the database corresponding to \code{db}. Set \code{expression=NULL} to remove the prefix expression.
#' @return A new SciDB database connection object with the prefix set.
#' @note This is mostly useful for setting namespaces, see the examples.
#' @examples
#' \dontrun{
#' library(scidb)
#' db <- scidbconnect()
#' ls(db)
#' new_db <- scidb_prefix(db, "set_role('functionary')")
#' ls(new_db)
#' }
#' @export
scidb_prefix = function(db, expression=NULL)
{
  stopifnot(inherits(db, "afl"))
  e = as.list(attributes(db)$connection)
  ans = list()
  class(ans) = "afl"
  attr(ans, "connection") = as.environment(e)
  if (is.null(expression)) attributes(ans)$connection$prefix = c()
  else attributes(ans)$connection$prefix = expression
  if (is.null(ans$connection$doc))
    return(update.afl(ans, attributes(ans)$connection$ops))
  update.afl(ans, attributes(ans)$connection$ops, attributes(ans)$connection$doc)
}


#' Simple utility to interactively enter a password without showing it on the screen
#'
#' @param prompt a text prompt to display, defaults to "Password:"
#' @export
getpwd = function(prompt="Password:")
{
  if (grepl("mingw", R.version["os"])) return(readline(sprintf("%s ", prompt)))
  cat(prompt, " ")
  system("stty -echo", ignore.stdout=TRUE, ignore.stderr=TRUE)
  a = readline()
  system("stty echo", ignore.stdout=TRUE, ignore.stderr=TRUE)
  cat("\n")
  a
}
