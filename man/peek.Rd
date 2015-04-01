\name{peek}
\alias{peek}
\alias{iqdf}
\title{
Sample from populated cells in a SciDB array.
}
\description{
Return an R dataframe containing samples from an input array.
}
\usage{
peek (x, n=50L, prob=1)
iqdf (x, n=50L, prob=1)
}
\arguments{
  \item{x}{A \code{scidb}  or \code{scidbdf} object.}
  \item{n}{Return at most \code{n} elements.}
  \item{prob}{A number in the interval (0, 1] 
              indicating the probability of a cell being included
              in the sample.}
}
\value{
An R data frame.
}
\details{
Peek at non-empty values of an array, returning at most \code{n} values.
Use \code{peek} to get a sense of the data contained in an array.

The function always returns results in data frame form, including
the array coordinates as data frame variables.
}
\note{
The synonym \code{iqdf} is maintained for compatibility with existing programs.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{bernoulli}}
}
\examples{
\dontrun{
x <- as.scidb(iris)
peek(x)
}}
