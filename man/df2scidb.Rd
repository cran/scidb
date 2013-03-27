\name{df2scidb}
\alias{df2scidb}
\title{Copy a data frame into a SciDB array.}
\description{
Copy a data frame into a new 1-D SciDB array.
}
\usage{
df2scidb(X, name = ifelse(exists(as.character(match.call()[2])), as.character(match.call()[2]), "X"), dimlabel = "row", chunkSize, rowOverlap = 0L, types = NULL, nullable = FALSE, real_format = "\%.15f", gc)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{X}{A data frame. }
  \item{name}{The SciDB array name, defaults to the R variable name if available.}
  \item{dimlabel}{Name the SciDB dimension.}
  \item{chunkSize}{The SciDB chunk size.}
  \item{rowOverlap}{The SciDB chunk overlap.}
  \item{types}{An optional vector explicitly specifying the SciDB attribute types. Length must match the number of columns of the data frame.}
  \item{nullable}{An optional vector indicating the SciDB nullable property of each attribute. Length must match the number of columns of the data frame.}
  \item{real_format}{The format string used to print real values.}
  \item{gc}{Optional logical value. If TRUE, then resulting SciDB array will be garbage-collected when the R variable referencing it is. The default value is FALSE.}
}
\details{
\code{df2scidb} is a workhorse utility function that transfers an R data frame
into a 1-D SciDB array via intermediate CSV formatting.
The columns of the data frame correspond to attributes in the SciDB array. 
The \code{iquery} function returns query results using a similar method as R
data frames.
}
\value{
NULL is invisibly returned. SciDB errors are propagated as R error conditions.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}

\examples{
\dontrun{
df2scidb(iris)
scidblist()
head(iquery("scan(iris)", return=TRUE))
}
}
