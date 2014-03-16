\name{persist}
\alias{persist}
\title{
persist
}
\description{
Mark a \code{scidb} or \code{scidbdf} object and any SciDB arrays in its
dependency graph persistent.
}
\usage{
persist(x, remove = FALSE, ...)
}
\arguments{
  \item{x}{ A scidb or scidbdf object}
  \item{remove}{ A logical value. See details.}
  \item{...}{ Optional arguments not used by the default method, but reserved for future use.}
}
\value{
NULL is invisibly returned. Use this function for its side effects.
}
\details{

Most \code{scidb} objects are ephemeral unless the \code{gc=FALSE} option is
explicitly selected. Use the \code{persist} function with \code{remove=FALSE}
to mark an array and all of its dependencies persistent. This is useful when
you want to save a \code{scidb} object to an R data file and make sure its data
is there when re-loaded.

Specify \code{remove=TRUE} to 'unpersist' an object and all of its dependencies.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- as.scidb(iris)
y <- unique(x$Species)
persist(y)
save(y, file="y.rdata")

# Now, *both* x and y can be removed without deleting the backing SciDB
# array. Without persist or setting gc=FALSE on x, the array would have
# been removed.
rm(x,y)
gc()

# Since the array is still there, we can get it back.
load("y.rdata")
y[]
}}
