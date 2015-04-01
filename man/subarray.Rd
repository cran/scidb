\name{subarray}
\alias{subarray}
\title{
Wrapper for SciDB subarray and between operators
}
\description{
Extract a rectangular subset from a SciDB array (\code{between=FALSE}),
or mask all but a rectangular subset empty (\code{between=TRUE}).
}
\usage{
subarray(x, limits, between=FALSE, `eval`=FALSE)
}
\arguments{
  \item{x}{A \code{scidb} or \code{scidbdf} object.}
  \item{limits}{Either a vector of coordinate bounds in \code{c(low1,low2,...,upper1,upper2,...)}
                form, or a character string schema, or a SciDB array object. See details.}
  \item{between}{If TRUE, form a SciDB between-style query, otherwise use subarray.}
  \item{eval}{If TRUE, execute the query and store the reult array. Otherwise defer evaluation.}
}
\details{
The \code{limits} argument can take one of three forms:

\itemize{
\item A vector of coordinate bounds listing the lower bounds for each dimension first
followed by the upper bounds for each dimension, in order (see examples).

\item A character string schema that contains a SciDB schema with the same dimensions
as \code{x}. Limits will be taken from the schema dimension bounds.

\item A \code{scidb} or \code{scidbdf} object. Limits will be taken from the object's
dimension bounds.
}

Use \code{between=FALSE} for the default SciDB subarray operator. Subarray returns a new
zero-indexed array containing the specified rectangular subset. Subarray may move data in
SciDB to the new array zero origin.

Use \code{between=TRUE} for the SciDB between operator. Between does not change the array bounds
or origin. It simply masks cells outside of the specified rectangular region empty. Because between
doesn't move data it's very fast.

See the examples for a simple comparison of the difference.
}
\value{
A new \code{scidb} object.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- as.scidb(iris)
# Extract rows 3--5 (note the indices of the returned results)
subarray(x,c(3,5))[]

# Extract rows 3--5 using SciDB between (note the indices)
subarray(x,c(3,5), between=TRUE)[]
}}
