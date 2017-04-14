# Functions for parsing and building SciDB schema strings.
# A SciDB schema string up to version 15.12 looks like:
# optional_array_name<attribute_1:type_1 NULL DEFAULT VALUE, attribute_2:type_2, ...>
# [dimension_1=start:end,chunksize,overlap, dimension_2=start:end,chunksize,overlap, ...]
#
# Starting with SciDB version 16.9, schema strings changed a lot. They look like:
# optional_array_name<v:double,a:int64 NOT NULL DEFAULT 5> [i=1:2:0:1000; j=1:3:0:1000]
# in particular, the dimensions are now start:end:overlap:chunksize
#

#' Internal function for processing SciDB dimension schema
#' @param x a scidb object or schema string
#' @return a data frame with parsed dimension data
#' @importFrom utils tail
.dimsplitter = function(x)
{
  if (inherits(x, "scidb")) x = schema(x)
  x = gsub("\\t", " ", x)
  x = gsub("\\n", " ", x)
  tokenize = function(s, token)
  {
    x = strsplit(s, token)[[1]]
    x = as.vector(rbind(x, rep(token, length(x))))
    x[- length(x)]
  }

  diagram = function(tokens, labels=c())
  {
    if(length(tokens) == 0) return(labels)
    last = tail(labels, 1)
    prev = tail(labels, 2)[1]
    if(is.null(last))              labels = c(labels, "name")
    else if(tokens[1] == "=")      labels = c(labels, "equals")
    else if(tokens[1] == ";")      labels = c(labels, "semicolon")
    else if(tokens[1] == ":")      labels = c(labels, "colon")
    else if(tokens[1] == ",")      labels = c(labels, "comma")
    else
    {
      if(last == "semicolon")      labels = c(labels, "name")
      else if(last == "equals")    labels = c(labels, "start")
      else if(last == "colon")
      {
        if(is.null(prev))          stop("invalid : character")
        else if(prev == "start")   labels = c(labels, "end")
        else if(prev == "end")     labels = c(labels, "overlap")
        else if(prev == "overlap") labels = c(labels, "chunk")
      }
      else if(last == "comma")
      {
        if(is.null(prev))          stop("invalid , character")
        else if(prev == "name")    labels = c(labels, "name")
        else if(prev == "start")   labels = c(labels, "end")
        else if(prev == "end")     labels = c(labels, "chunk")
        else if(prev == "chunk")   labels = c(labels, "overlap")
        else if(prev == "overlap") labels = c(labels, "name")
      }
    }
    diagram(tokens[-1], labels)
  }
  form = function(x)
  {
    c(name=x["name"], start=x["start"], end=x["end"], chunk=x["chunk"], overlap=x["overlap"])
  }

  s = tryCatch(gsub("]", "", strsplit(x, "\\[")[[1]][[2]]), error=function(e) NULL)
  if(is.null(s) || nchar(s) == 0) return(NULL)
  tokens = Reduce(c, lapply(Reduce(c, lapply(Reduce(c, lapply(tokenize(s, "="), tokenize, ":")), tokenize, ";")), tokenize, ","))
  names(tokens) = diagram(tokens)
  tokens[!(names(tokens) %in% c("equals", "colon", "semicolon", "comma"))]
  i = which(names(tokens) %in% "name")
  j = c((i - 1)[-1], length(tokens))
  ans = Reduce(rbind, lapply(1:length(i), function(k) form(tokens[i[k]:j[k]])))
  if(length(i) == 1) {
    ans = data.frame(as.list(ans), stringsAsFactors=FALSE, row.names=NULL)
  } else ans = data.frame(ans, stringsAsFactors=FALSE, row.names=c())
  names(ans) = c("name", "start", "end", "chunk", "overlap")
  ans$name = gsub(" ", "", ans$name)
  ans
}



#' Internal function for processing SciDB attribute schema
#' @param x a scidb object or schema string
#' @return a data frame with parsed attribute data
.attsplitter = function(x)
{
  if (is.character(x)) s = x
  else
  {
    if (!(inherits(x, "scidb"))) return(NULL)
    s = schema(x)
  }
  s = gsub("\\t", " ", s)
  s = gsub("\\n", " ", s)
  s = gsub("default[^,]*", "", s, ignore.case=TRUE)
  s = strsplit(strsplit(strsplit(strsplit(s, ">")[[1]][1], "<")[[1]][2], ",")[[1]], ":")
  # SciDB schema syntax changed in 15.12
  null = if (at_least(attr(x@meta$db, "connection")$scidb.version, "15.12"))
           ! grepl("NOT NULL", s)
         else grepl(" NULL", s)
  type = gsub(" ", "", gsub("null", "", gsub("not null", "", gsub("compression '.*'", "", vapply(s, function(x) x[2], ""), ignore.case=TRUE), ignore.case=TRUE), ignore.case=TRUE))
  data.frame(name=gsub("[ \\\t\\\n]", "", vapply(s, function(x) x[1], "")),
             type=type,
             nullable=null, stringsAsFactors=FALSE)
}



#' SciDB array schema
#' @param x a \code{\link{scidb}} array object
#' @param what optional schema subset (subsets are returned in data frames; partial
#'  argument matching is supported)
#' @return character-valued SciDB array schema
#' @examples
#' \dontrun{
#' s <- scidbconnect()
#' x <- scidb(s,"build(<v:double>[i=1:10,2,0,j=0:19,1,0],0)")
#' schema(x)
#' # [1] "<v:double> [i=1:10:0:2; j=0:19:0:1]"
#' schema(x, "attributes")
#' #  name   type nullable
#' #1    v double     TRUE
#' schema(x, "dimensions")
#'   name start end chunk overlap
#' #1    i     1  10     2       j
#' #2    0     0  19     1       0
#' }
#' @export
schema = function(x, what=c("schema", "attributes", "dimensions"))
{
  if (!(inherits(x, "scidb"))) return(NULL)
  switch(match.arg(what),
    schema = gsub(".*<", "<", x@meta$schema),
    attributes = .attsplitter(x),
    dimensions = .dimsplitter(x),
    invisible()
  )
}

dfschema = function(names, types, len, chunk=10000)
{
  dimname = make.unique_(names, "i")
  sprintf("<%s>[%s=1:%d,%d,0]", paste(paste(names, types, sep=":"), collapse=","), dimname, len, chunk)
}
