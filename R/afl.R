#' Update available AFL operators
#'
#' @param .db an \code{afl} object (a SciDB database connection returned from \code{\link{scidbconnect}})
#' @param .new a character vector of operator names
#' @param .ops an optional three-variable data frame with variables name, signature, help, corresponding
#' to the operator names, signatures, and help files (from SciDB Doxygen documentation)
#' @return an updated database \code{afl} object
#' @note Every operator gets a shallow copy of the db argument; that is, \code{attributes(db[i])$conn} should be the same for every operator index i.
#' @keywords internal
#' @importFrom utils head
update.afl = function(.db, .new, .ops)
{
  if (missing(.ops))
  {
    e = new.env()
    data("operators", package="scidb", envir=e)
    .ops = e$operators
  }

  afl = function(.db, .name, .help, .signature)
  {
    .name = paste(.name)
    .help = paste(.help)
    .signature = paste(.signature)
    function(...)
    {
      .pf = parent.frame()
      .depend = new.env()
      .args = paste(
               lapply(as.list(match.call())[-1],
                 function(.x) tryCatch({
                   ex = eval(.x, envir=.pf)
                   if (class(ex)[1] %in% "scidb")
                   {
                     assign(tail(make.names(c(ls(.depend), paste("V", runif(1), sep="")), unique=TRUE), 1), ex, envir=.depend)
                     ex@name
                   }
                   else .x
               }, error=function(e) .x)),
         collapse=",")
      expr = sprintf("%s(%s)", .name, .args)
      # handle aliasing
      expr = gsub("%as%", " as ", expr)
      # handle R scalar variable substitutions
      expr = rsub(expr, parent.frame())
      # Some special AFL non-operator expressions don't return arrays
      if (any(grepl(.name, getOption("scidb.ddl"), ignore.case=TRUE)))
      {
        return(iquery(.db, expr))
      }
      if (getOption("scidb.debug", FALSE)) message("AFL EXPRESSION: ", expr)
      ans = scidb(.db, expr)
      ans@meta$depend = as.list(.depend)
      ans
    }
  }

  for (.x in .new)
  {
    .formals = NULL
    .signature = ""
    .help = ""
    .i = .ops[, 1] == .x
    # update formal function arguments for nice tab completion help
    if (any(.i))
    {
      .def = head(.ops[.i, ], 1)
      # XXX very ugly...
      .formals = strsplit(gsub("[=:].*", "", gsub("\\|.*", "", gsub(" *", "", gsub("\\]", "", gsub("\\[", "", gsub("\\[.*\\|.*\\]", "", gsub("[+*{})]", "", gsub(".*\\(", "", .def[2])))))))), ",")[[1]]
      .help = .def[3]
      .signature = .def[2]
    }
    .db[[.x]] = afl(.db, .x, .help, .signature)
    if(!is.null(.formals))
      formals(.db[[.x]]) = eval(parse(text=sprintf("alist(%s, ...=)", paste(paste(.formals, "="), collapse=", "))))
    class(.db[[.x]]) = "operator"
  }
  .db
}

#' Substitute scalar-valued R expressions into an AFL expression
#' R expressions are marked with R(expression)
#' @param x character valued AFL expression
#' @param env environment in which to evaluate R expressions
#' @return character valued AFL expression
#' @keywords internal function
rsub = function(x, env)
{
  if (! grepl("[^[:alnum:]_]R\\(", x)) return(x)
  imbalance_paren = function(x) # efficiently find the first imbalanced ")" character position
  {
    which (cumsum( (as.numeric(charToRaw(x) == charToRaw("("))) - (as.numeric(charToRaw(x) == charToRaw(")"))) ) < 0)[1]
  }
  y = gsub("([^[:alnum:]_])R\\(", "\\1@R(", x)
  y = strsplit(y, "@R\\(")[[1]]
  expr = Map(function(x)
  {
    i = imbalance_paren(x)
    rexp = eval(parse(text=substring(x, 1, i - 1)), envir=env)
    rmdr = substring(x, i + 1)
    paste(rexp, rmdr, sep="")
  }, y[-1])
  sprintf("%s%s", y[1], paste(expr, collapse=""))
}

#' Display SciDB AFL operator documentation
#' @param topic an afl object from a SciDB database connection, or optionally a character string name
#' @param db optional database connection from \code{\link{scidbconnect}} (only needed when \code{topic} is a character string)
#' @return displays help
#' @examples
#' \dontrun{
#' d <- scidbconnect()
#' aflhelp("list", d)     # explicitly look up a character string
#' help(d$list)        # same thing via R's \code{help} function
#' }
#' @importFrom  utils data
#' @export
aflhelp = function(topic, db)
{
  if (is.character(topic))
  {
    if (missing(db)) stop("character topics require a database connection argument")
    topic = db[[topic]]
  }
  h = sprintf("%s\n\n%s", environment(topic)$.signature, gsub("\\n{2,}", "\n", environment(topic)$.help))
  message(h)
}
