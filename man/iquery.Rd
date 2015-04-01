\name{iquery}
\alias{iquery}
\title{Simple SciDB query tool}
\description{Issue SciDB queries and optionally return output in a data frame.}
\usage{
iquery(query, `return` = FALSE, afl = TRUE,
       iterative = FALSE, n = 10000, excludecol,
       binary = FALSE, ...)
}
\arguments{
  \item{query}{A SciDB query string (character). Separate multiple queries
               with semicolons. }
  \item{return}{Set to TRUE to return output. Otherwise don't return
                query output. Only available when \code{afl=TRUE}}
  \item{afl}{TRUE indicates query is in AFL form, FALSE indicates AQL. }
  \item{iterative}{Set to TRUE to return a result iterator. FALSE returns
                   entire result at once.}
  \item{n}{Maximum number of rows to return when iterating through results.
           Set to \code{Inf} to return everything. Ignored when binary=TRUE.}
  \item{excludecol}{An optional numeric range of columns to exclude from
                    iterative results (only applies when iterative=TRIE).}
  \item{binary}{TRUE indicates efficient binary transfer method. Set to
                FALSE to use read.table and UTF8 encoded text transfer, parsed
                by \code{read.table} (more flexible parsing for unusual types).}
  \item{...}{Options passed on to \code{read.table} used to parse results when
             \code{binary=FALSE}.  See details.}
}
\details{
The \code{iquery} function is an analog of the command-line SciDB \code{iquery}
program. Use it to issue arbitrary SciDB queries.

Several return options are available. Select \code{binary=TRUE} to bring the
entire result of the query to R using an efficient binary transfer.  Note that
for now, selecting \code{binary=TRUE} implies \code{n=Inf}--that is, the whole
result comes back at once.

When \code{binary=TRUE} is selected, two optional arguments become available:

\itemize{
\item \code{buffer=100000}:  The binary returned data will be parsed in chunks of
at most \code{buffer} rows. Adjust this for performance.

\item \code{row.names=NULL}: Similarly to R's \code{read.table} function, set
this value to be a vector of row labels for the output, or optionally a single
number indicating the output column to use as row names for the output
data.frame.

\item \code{unpack=TRUE}: Set \code{unpack} to FALSE to return only the array
attribute data values. The default \code{unpack=TRUE} returns coordinate indices
and the array attribute values.
}

Select \code{binary=FALSE} to use a delimited text transfer option. Data are
transferred using delimited text and parsed through R's \code{read.table}
function. When this option is selected, any \code{...} arguments are passed
directly to \code{read.table}. You can limit the number of returned results
to \code{n} when this option is set.

Select \code{iterative=TRUE} to return an iterator that iterators over \code{n}
output rows at a time. This option only supports delimted text transfers.
}
\value{
If \code{return=TRUE}, return the query result in data frame form (similar to the
SciDB \code{iquery} command-line \code{-olcsv+} output option).

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
