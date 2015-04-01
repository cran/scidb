\name{replaceNA}
\alias{replaceNA}
\title{
Replace SciDB NULL values with another value.
}
\description{
Replace SciDB NULL (aka missing) values with another value.
}
\usage{
replaceNA (x, value, `attribute`, `eval`=FALSE, ...)
}
\arguments{
  \item{x}{A \code{scidb}  or \code{scidbdf} object.}
  \item{value}{An optional scidb array containing the replacement value.}
  \item{attribute}{An optional attribute to restrict NULL replacement to.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
  \item{...}{Optional arguments.}
}
\value{
A new \code{scidb} or \code{scidbdf} object.
}
\details{
This R function corresponds to SciDB's \code{substitute} operator.

If \code{value} is missing, a value of numeric zero, false, or the string "0" will be used
to replace NULL values depending on the replacement attribute type.

If \code{attribute} is missing, then an attempt will be made to replace all NULL values in the
array.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- scidb("build(<v:double null>[i=1:3,3,0],'{1}[(null),(null),(null)]',true)")
y <- replaceNA(x)
}}

