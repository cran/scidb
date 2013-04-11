\name{svd}
\docType{methods}
\alias{svd}
\alias{svd,scidb-method}
\title{
svd
}
\description{
Compute a partial or full singular value decomposition of a SciDB matrix.
}
\usage{
\S4method{svd}{scidb}(x, nu, nv)
}
\arguments{
  \item{x}{A scidb object representing a matrix.}
  \item{nu}{Optional number of singular values/vectors to compute. See notes below.}
  \item{nv}{Not used.}
}
\note{
If the \code{nu} argument is specified, the SciDB \code{tsvd} function will be
used and the matrix \code{x} may be dense or sparse. Otherwise the full SVD is
computed by SciDB and the matrix \code{x} must be dense. The \code{nv}
argument is not used and appears in the function signature only
for compatibility with the base \code{svd} function. This function always
sets \code{nv} = \code{nu}.
}
\value{
A list of three \code{scidb} objects corresponding to a matrix of left
singular vectors, a vector of singular values, and a matrix of transposed
right singular vectors, respectively.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- as.scidb(matrix(rnorm(500*500),500))
y <- svd(x)
print(y)
}}
