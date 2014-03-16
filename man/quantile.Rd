\name{quantile}
\alias{quantile.scidb}
\alias{quantile.scidbdf}
\title{
Sample Quantiles
}
\description{
Compute sample quantiles of a single-attribute SciDB array.
}
\usage{
\method{quantile}{scidb}(x, probs=seq(0,1,0.25), type=7, ...)
\method{quantile}{scidbdf}(x, probs=seq(0,1,0.25), type=7, ...)
}
\arguments{
  \item{x}{ a \code{scidb} or \code{scidbdf} object.}
  \item{probs}{ a numeric vector of probabilities with value in [0,1].}
  \item{type}{ either 1 or 7, selecting the two available quantile algorithms
               (see help for \code{\link{quantile}} for more details).}
  \item{...}{ optional arguments that might be used in a future version.}
}
\value{
A two-attribute SciDB data frame-like array with \code{length(probs)} rows is
returned.  The first attribute contains \code{probs} and the second attribute
the sample quantiles.
}
\details{
If the input array contains more than one attribute, the first
numeric attribute will be chosen (with a warning). Use \code{project} to
select a single attribute.

The SciDB \code{quantile} function only supports types 1 and 7 at the time
of this writing. Quantile type 1 can be used with non-numeric values, as long
as they can be ordered. See \code{\link{quantile}} for much more detail on
the calculation.
}
\seealso{
  \code{\link{quantile}}
}
\author{
A. Poliakov <apoliakov@paradigm4>
}
\examples{
\dontrun{
x <- as.scidb(rnorm(1000))
quantile(x)[]

quantile(x,type=1)[]
}
}
