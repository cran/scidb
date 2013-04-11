\name{df2scidb}
\alias{df2scidb}
\title{Copy a data frame into a SciDB array.}
\description{
Copy a data frame into a new 1-D SciDB array.
}
\usage{
df2scidb(X, name = tmpnam(), dimlabel = "row", chunkSize,
         rowOverlap = 0L, types = NULL, nullable, schema_only = FALSE, gc, start)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{X}{A data frame. }
  \item{name}{The SciDB array name, defaults to an automatically-generated name.}
  \item{dimlabel}{Name the SciDB dimension.}
  \item{chunkSize}{The SciDB chunk size.}
  \item{rowOverlap}{The SciDB chunk overlap.}
  \item{types}{An optional vector explicitly specifying the SciDB attribute types. Length must match the number of columns of the data frame.}
  \item{nullable}{An optional vector indicating the SciDB nullable property of each attribute. Length must match the number of columns of the data frame.}
  \item{schema_only}{If TRUE, return a string that would represent the SciDB array schema without uploading data to SciDB.}
  \item{gc}{Optional logical value. If TRUE, then resulting SciDB array will be garbage-collected when the R variable referencing it is. The default value is FALSE.}
  \item{start}{Optional integer starting index value. Defaults to 1.}
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
