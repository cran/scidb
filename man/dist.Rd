\name{dist}
\docType{methods}
\alias{dist}
\alias{dist,scidb-method}
\concept{dissimilarity}
\title{Distance Matrix Computation}
\description{
  Compute and return a distance matrix that contains the Euclidean
  distance between rows of a data matrix.
}
\usage{
\S4method{dist}{scidb}(x, method=c("euclidean","manhattan","maximum"))
}
\arguments{
  \item{x}{a numeric matrix object of class \code{scidbdf}.}
  \item{method}{the distance measure to be used. This must be one of
          \code{euclidean}, \code{maximum}, or \code{manhattan}.
          Any unambiguous substring can be given.  }
}
\details{
The \code{(i,j)} position of
the output matrix contains the distance between rows
\code{i} and \code{j} of the data matrix \code{x} as measured
by the specified norm. The function only returns values below
the diagonal as a sparse SciDB matrix.

Missing values are not allowed.
}
\value{
  \code{dist} returns a distance matrix object of class \code{"scidb"}.
}
\seealso{
  \code{\link{hclust}}.
  \code{\link{dist}}.
}
