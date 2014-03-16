\name{scidb_attributes}
\alias{scidb_attributes}
\title{
scidb_attributes
}
\description{
Return a vector of SciDB array attribute names.
}
\usage{
scidb_attributes(x)
}
\arguments{
  \item{x}{An object of class \code{scidb} or \code{scidbdf}}
}
\value{
A character vector of SciDB array dimension names.
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
scidb_attributes(A)
}}
