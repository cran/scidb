\name{all.equal}
\alias{all.equal.scidb}
\alias{all.equal.scidbdf}
\title{
Test if two SciDB arrays are equal in content
}
\description{
Test if two SciDB arrays are equal in content, ignoring
SciDB chunk partitioning and overlap differences.
}
\usage{
\method{all.equal}{scidb}(target, current, ...)
\method{all.equal}{scidbdf}(target, current, ...)
}
\arguments{
  \item{target}{ a \code{scidb} or \code{scidbdf} object.}
  \item{current}{ a \code{scidb} or \code{scidbdf} object.}
  \item{...}{ optional arguments that might be used in a future version.}
}
\value{
Return \code{TRUE} if the \code{target} SciDB array has the same dimensions,
same attributes and types and same data at the same coordinates as the
\code{current} array.  Return \code{FALSE} otherwise.
}
\seealso{
  \code{\link{scidb}}
}
\author{
A. Poliakov <apoliakov@paradigm4>
}
\examples{
\dontrun{
# Let's build a constant-valued 10x10 array:
a <- build(pi, c(10,10), chunksize=c(5,7))

# The array is equal to itself!
all.equal(a,a)    # (returns TRUE)

# Let's build an identically-shaped and valued array but with different
# array chunk partitioning and overlap:
b <- build(pi, c(10,10), chunksize=c(9,10), overlap=c(1,1))

# all.equal does not care about SciDB chunk sizes or overlap differences:
all.equal(a,b)    # (returns TRUE)

# But differences in value or dimension matter:
all.equal(a, b*2)  # (returns FALSE)
}
}
