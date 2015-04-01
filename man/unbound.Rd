\name{unbound}
\alias{unbound}
\alias{bound}
\title{
Return a bounded or unbounded version of a SciDB array
}
\description{
Return a new sparse SciDB array identical to the input array but with each
dimension upper bound set to '*' (unbound) or maximum non-empty coordinate
(bound).
}
\usage{
unbound (x)
bound (x)
}
\arguments{
  \item{x}{A \code{scidb}  or \code{scidbdf} object.}
}
\value{
A new sparse \code{scidb} or \code{scidbdf} object.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- build("i+j", c(10,10), type="double")
y <- unbound(x)
z <- bound(y)
# Now compare schema of x, y, and z:
schema(x)
schema(y)
schema(z)
}}
