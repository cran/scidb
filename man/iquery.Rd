\name{iquery}
\alias{iquery}
\title{Simple SciDB query tool}
\description{Issue SciDB queries and optionally return output in a data frame.}
\usage{
iquery(query, `return` = FALSE, afl = TRUE, iterative = FALSE, n = 1000, excludecol, ...)
}
\arguments{
  \item{query}{A SciDB query string (character). Separate multiple queries with semicolons. }
  \item{return}{Set to TRUE to return output. Otherwise don't return query output. Only available when \code{afl=TRUE}}
  \item{afl}{TRUE indicates query is in AFL form, FALSE indicates AQL. }
  \item{iterative}{Set to TRUE to return a result iterator. FALSE returns entire result at once.}
  \item{n}{Maximum number of rows to return when iterating through results.}
  \item{excludecol}{An optional numeric range of columns to exclude from iterative results (only applies when iterative=TRIE).}
  \item{...}{Options passed on to \code{read.table} used to parse results.}
}
\details{
The \code{iquery} function is a simple analog of the command-line
SciDB \code{iquery} program. }
\value{
If \code{return=TRUE}, return the query result in data frame form (similar to the
command-line \code{-olcsv+} output option).

If \code{return}=FALSE, return the query ID number.

SciDB errors encountered during query processing are propagated to R and can
be handled with normal R error handling mechanisms.

Set \code{itreative=TRUE} to return a result iterator. Use the iterator \code{nextElem}
function to iteratively return results, a maximum of \code{n} results at a time. See
help in the \code{iterators} package for examples and options.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
iquery("list('instances')",return=TRUE)

# A simple example that iterates through results using foreach
# Build an array with 1 million numbers from zero to 1.
iquery("store(build(<x:double>[i=1:1000000,100000,0],i/1000000),X)")
# Apply a function and return result in an iterator:
i <- iquery("apply(X, y, sin(x))", return=TRUE, iterative=TRUE)

# Sum up x and y (and dimension i too)
library("foreach")
foreach(j=i, .combine=function(...)colSums(rbind(...))) %do% colSums(j)

# Compare with the much faster equivalent inside SciDB:
iquery("aggregate(apply(X, y, sin(x)),sum(x),sum(y))", return=TRUE)

}}
