\name{schema}
\alias{schema}
\title{
schema
}
\description{
Return the SciDB array schema.
}
\usage{
schema(x)
}
\arguments{
  \item{x}{An object of class \code{scidb} or \code{scidbdf}}
}
\value{
The SciDB array schema.
}
\note{
This function is nearly equivalent to \code{x@schema}.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
# Upload data to create a 10x10 SciDB matrix
A <- as.scidb(matrix(rnorm(100),10))
schema(A)
A@schema
}}
