\name{as.scidb}
\alias{as.scidb}
\title{
Upload an R vector, matrix or data.frame to a SciDB array.
}
\description{
Upload an R vector, matrix or data.frame into SciDB, returning a reference scidb object.
}
\usage{
as.scidb(X, name = tmpnam(), chunksize, overlap, start, chunkSize, gc=TRUE, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{X}{ A vector, matrix or sparse matrix of double-precision floating point values or a data.frame. }
  \item{name}{ The name of the SciDB array to create.  }
  \item{chunksize}{A vector of optional SciDB array chunk sizes (a vector of length 1 for data frames and vectors, length 2 for matrices)}
  \item{overlap}{(Not yet supported)}
  \item{start}{A vector of SciDB array starting dimension numeric index value or values.}
  \item{chunkSize}{(Same as \code{chunksize} argument, for backwards compatibility -- use \code{chunksize} instead)}
  \item{gc}{Set to TRUE to remove SciDB array when R object is garbage collected or R exists. FALSE means SciDB array persists.}
  \item{\dots}{ additional arguments to pass to \code{df2scidb} (see \code{\link{df2scidb}}).}
}
\details{
Used with a matrix  or vector argument,
the \code{as.scidb} function creates a single-attribute SciDB array named
\code{name} and copies the data from \code{X} into it, returning a \code{scidb}
object reference to the new array. The SciDB array will be 1-D if \code{X} is a
vector, and 2-D if \code{X} is a matrix. Data are transferred using a binary
representation.

If \code{X} is a data.frame, then \code{as.scidb} creates a one-dimensional
multi-attribute SciDB array, with SciDB attributes representing each column
of the data.frame. A utitlity function called \code{df2scidb} is used for
data.frame uploads--the functions \code{as.scidb} and {df2scidb} are equivalent
in this use case. Note that data frame data are transferred using a text
representation.

This function supports double-precision, integer (32-bit), logical, and single-character
array attribute types.
}
\note{
The \code{as.scidb} function is not a very efficient way to upload lots of data
to SciDB. Consider using the SciDB parallel bulk loader tools for that.
}
\value{A \code{scidb} reference object. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}

\seealso{
\code{\link{scidb}}
\code{\link{df2scidb}}
}
\examples{
\dontrun{
X <- matrix(runif(20),5)
A <- as.scidb(X)
as.scidb(iris)
scidblist()
print(A)
}
}
