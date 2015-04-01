\name{kmeans}
\docType{methods}
\alias{kmeans}
\alias{kmeans,scidb-method}
\title{
kmeans
}
\description{
Basic kmeans clustering using Lloyd's algorithm
}
\usage{
\S4method{kmeans}{scidb}(x,  centers, iter.max=30, nstart=1,
  algorithm="Lloyd")
}
\arguments{
  \item{x}{A scidb object representing a matrix.}
  \item{centers}{the number of clusters}
  \item{iter.max}{the maximum number of iterations allowed}
  \item{nstart}{Not (yet) used by this implementation}
  \item{algorithm}{Not (yet) used by this implementation (always Lloyd's)}
}
\value{
A list of class \code{kmeans}.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{kmeans}}
}
\examples{
\dontrun{
# Let's generate data with obvious clusters.
set.seed(1)
x <- c(x=rnorm(50),rnorm(50)+3);y = c(rnorm(50),rnorm(50)+3)
A <- cbind(x,y)
# Plot the data (try to set the aspect ratio square)
# Let a be an m x n matrix.
# Each row of the matrix A is a point in an n-dimensional Euclidean space.
library("scidb")
scidbconnect()
A = as.scidb(A)
k = kmeans(A, 2, 10)
plot(A[], asp=1,xlab="x",ylab="y")
points(A[k$cluster \%==\%0, ][], pch=19,col=4)
}}
