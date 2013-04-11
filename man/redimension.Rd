\name{redimension}
\alias{redimension}
\title{
redimension
}
\description{
The \code{redimension} function is a wrapper to the SciDB `redimension` operator.
}
\usage{
redimension(x,s,`eval`)
}
\arguments{
  \item{x}{A SciDB array object of class scidb or scidbdf.}
  \item{s}{A SciDB array object of class scidb, scidbdf, or a character string representation of the output array schema.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\value{
A \code{scidb} object.
}
\note{
Use \code{eval=FALSE} to defer expression evaluation for efficiency. Results can be composed to form complex query expressions that can be evaluated in one transaction.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
# XXX
}}
