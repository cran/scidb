\name{rank-methods}
\docType{methods}
\alias{rank}
\alias{rank,scidb-method}
\title{Methods for Function \code{rank} in Package \pkg{scidb}}
\description{
\code{rank}
     Returns the sample ranks of the values in a vector.  Ties (i.e.,
     equal values) and missing values can be handled in several ways.
}
\usage{
\S4method{rank}{scidb}(x, na.last = TRUE,
          ties.method = c("average", "first", "random", "max", "min"))
}
\arguments{
  \item{x}{A \code{scidb} vector object.}
  \item{na.last}{NOT USED--for compatability with native R functions only.}
  \item{ties.method}{SciDB supports average or random.}
}
\value{A \code{scidb} reference object.}
\note{
Select either \code{average} or \code{random} for \code{ties.method}.
If all components are different (and no 'NA's), the ranks are well
defined, with values in 'seq_len(x)'.  With some values equal
(called 'ties'), the argument 'ties.method' determines the result
at the corresponding indices.
The \code{random} method puts these in random order whereas the default,
\code{average}, replaces them by their mean.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
x <- scidb("build(<v:double>[i=1:10,10,0],random()%100)")

a <- order.scidb(x)
print(a)
}
}
