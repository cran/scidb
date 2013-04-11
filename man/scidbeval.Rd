\name{scidbeval}
\alias{scidbeval}
\title{
scidbeval
}
\description{
Evaluate a SciDB expression.
}
\usage{
scidbeval(expr, eval=TRUE, name, gc)
}
\arguments{
  \item{expr}{An R expression that evaluates to a `scidb` or `scidbdf` object.}
  \item{eval}{If TRUE, return a `scidb` or `scidbdf` object that has been evaluated by the databas and stored to a SciDB array.}
  \item{name}{An optional character value that names the stored SciDB array.}
  \item{gc}{An optional logical value that, when TRUE, ties the result to the R garbage collector.}
}
\value{
Either a \code{scidb} or \code{scidbdf} object.
}
\note{
Use \code{eval=FALSE} to defer expression evaluation for efficiency. Many functions in the SciDB package accept un-evaluated expresions. One can compose these objects to form complex query expressions that can be evaluated in one transaction.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
A <- as.scidb(matrix(rnorm(25),5))
B <- A + A   # Returns a SciDB array promise (not evaluated yet)
C <- scidbeval(A + A) # Returns a SciDB array that has been evaluated and stored
}}
