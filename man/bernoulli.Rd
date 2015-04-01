\name{bernoulli}
\alias{bernoulli}
\title{
Sample from populated cells in a SciDB array.
}
\description{
Return a sparse SciDB array that samples from populated cells in the SciDB array \code{x}.
}
\usage{
bernoulli (x, prob, seed=sample(2^32 - 1 - 2^31, 1))
}
\arguments{
  \item{x}{A \code{scidb}  or \code{scidbdf} object.}
  \item{prob}{A number in the interval (0, 1] 
              indicating the probability of a cell being included
              in the sample.}
  \item{seed}{A 32-bit signed integer random seed value for SciDB's bernoulli operator.}
}
\value{
A new sparse \code{scidb} or \code{scidbdf} object.
}
\details{
This R function corresponds to SciDB's \code{bernoulli} operator.

For each non-empty cell in the input array \code{x}, generate a number
sampled fom a uniform random distribution. If the number is less than
or equal to \code{prob}, then include the cell in the output array.
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
y <- bernoulli(x, 0.5) # Sample about 1/2 the cells of x
count(y)

# Note that y is lazily evaluated *and* its value is probablistic. Thus
# its value will change upon each evaluation. For example:
count(y)
count(y)

# Use scidbeval to force evaluation of y once and for all:
y <- scidbeval(y)
count(y)
count(y)
}}

