\name{build}
\alias{build}
\title{
build
}
\description{
The \code{build} function is a wrapper to the SciDB `build` operator.
Operation is similar to the R \code{matrix} and \code{array} functions.
It creates a new single-attribute SciDB array based on the specified parameters.
}
\usage{
build(data, dim, names, type="double", start, name, chunksize, overlap, gc=TRUE, `eval`)
}
\arguments{
  \item{data}{A valid SciDB expression (expressed as a character string) or constant to fill the array.}
  \item{dim}{A vector of dimension lengths.}
  \item{names}{An optional vector of attribute and dimension names. Default attribute name is `val` and the dimension names are labeled i,j,... by default.}
  \item{type}{The SciDB type of the array attribute.}
  \item{start}{An optional vector of starting dimension coordinate indices. Must match the length of the dim vector.}
  \item{name}{Optional name of the SciDB array. An automatically generated name is used by default.}
  \item{chunksize}{An optional vector of dimension chunk sizes. Must match the length of the dim vector.}
  \item{overlap}{An optional vector of dimension overlap values. Must match the length of the dim vector.}
  \item{gc}{gc=TRUE (the default) removes the array when corresponding R objects are garbage collected. Set gc=FALSE to preserve arrays.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\value{
A \code{scidb} object.
}
\note{
Use \code{eval=FALSE} to defer expression evaluation for efficiency.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
y <- build(pi, c(5,3))
print(head(y))
}}
