\name{between}
\alias{between}
\title{
between
}
\description{
Use \code{between} to select contiguous subarrays in indexing
operations. The \code{between} function would not normally be used directly
but rather inside bracket indexing operations as shown in the example.
This function is designed to support efficient indexing of contiguous
subarrays for arrays with non-integer dimensions.
}
\usage{
between(a,b)
}
\arguments{
  \item{a}{A SciDB array range bound (numeric or string in the case of non-integer dimension)}
  \item{b}{A SciDB array range bound (numeric or string in the case of non-integer dimension)}
}
\value{
A function that evaluates to a list of the specified bounds.
}
\note{
Between requires argument values that correspond to the array dimension
types (no dimension casting is performed).
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
# The following are equivalent, but between avoids creating an
# indexing vector:
A[0:4,][]
A[between(0,4),][]
}}
