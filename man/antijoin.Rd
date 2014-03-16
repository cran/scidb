\name{antijoin}
\alias{antijoin}
\title{
antijoin
}
\description{
Given two SciDB arrays of same dimensionality, return any coordinates that do
NOT join.
}
\usage{
antijoin( array1, array2 )
}
\arguments{
  \item{array1}{ a \code{scidb} array or \code{scidbdf} data frame object.}
  \item{array2}{ a \code{scidb} array or \code{scidbdf} data frame object.}
}
\value{
\code{NULL} is returned when the two arrays join in all coordinates.
Otherwise, a \code{scidb} array is returned such that for all coordinates, the
single attribute shall equal to 1 if those coordinates exist only in
\code{array1}, or 2 if those coordinates exist only in \code{array2}.
}
\author{
A. Poliakov <apoliakov@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
set.seed(1)
# Generate sparse SciDB arrays
a <- as.scidb(Matrix::sparseMatrix(
               sample(10,100,replace=TRUE),sample(10,100,replace=TRUE),x=runif(100)))
b <- as.scidb(Matrix::sparseMatrix(
               sample(10,100,replace=TRUE),sample(10,100,replace=TRUE),x=runif(100)))
antijoin(a,b)[]

# Output looks like:
# [1,] 2 . . 1 2 1 . . . 2
# [2,] 2 . 2 2 2 1 1 1 2 2
# [3,] 2 . . 2 . 2 . . . 1
# [4,] . 2 1 . . . . 1 . 2
# [5,] . . . . . 1 . . . 2
# [6,] 1 2 . . 2 2 . 1 . 2
# [7,] . 1 . 1 . 1 . 1 . 1
# [8,] 2 . 1 . 1 1 1 . . .
# [9,] 2 . 1 . 2 . . 2 1 .
#[10,] 2 . . . 2 . 2 2 . .
}}
