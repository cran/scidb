\name{as.scidb}
\alias{as.scidb}
\title{
Upload an R matrix or data.frame to a SciDB array.
}
\description{
Cast an R matrix or data.frame to a SciDB array, returning a reference scidb object.
}
\usage{
as.scidb(X, name = ifelse(exists(as.character(match.call()[2])), as.character(match.call()[2]), tmpnam("array")), rowChunkSize = 1000L, colChunkSize = 1000L, start = c(0L,0L), gc=FALSE, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{X}{ A matrix of double-precision floating point values or a data.frame. }
  \item{name}{ The name of the SciDB array to create, defaulting to the R variable name if available.  }
  \item{rowChunkSize}{Maximum SciDB chunk size for the 1st array dimension.}
  \item{colChunkSize}{Maximum SciDB chunk size for the 2nd array dimension (ignored for vectors and data.frames). }
  \item{start}{Starting dimension numeric index value or values.}
  \item{gc}{Set to TRUE to remove SciDB array when R object is garbage collected or R exists. FALSE means SciDB array persists.}
  \item{\dots}{ additional arguments to pass to \code{df2scidb} (see \code{\link{df2scidb}}).}
}
\details{
Used with a matrix  or vector argument,
the \code{as.scidb} function creates a single-attribute SciDB array named
\code{name} and copies the data from \code{X} into it, returning a \code{scidb}
object reference to the new array. The SciDB array will be 1-D if \code{X} is a
vector, and 2-D if \code{X} is a matrix.

If \code{X} is a data.frame, then \code{as.scidb} creates a one-dimensional
multi-attribute SciDB array, with SciDB attributes representing each column
of the data.frame. The functions \code{as.scidb} and {df2scidb} are equivalent
in this use case.

The SciDB array row and column chunk sizes are set to the minimum of the number
of rows and columns of \code{X} and the specified \code{rowChunkSize} and
\code{colChunkSize} arguments, respectively. The column chunk size argument is
ignored if the \code{X} is a vector.

This function supports double-precision, integer (32-bit), logical, and single-character
array attribute types.
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
