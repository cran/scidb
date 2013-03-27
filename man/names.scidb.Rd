\name{names.scidb}
\alias{names.scidb}
\title{
Dimension names
}
\description{
Return 2nd dimension names of a SciDB array with two or more dimensions,
or just the single dimension names of a 1-d SciDB array reference object.
Warning! Huge SciDB arrays can retun too much. See 
\code{\link{scidb}} for an example alternative approach.
}
\usage{
\method{names}{scidb}(x)
}
\arguments{
  \item{x}{ A SciDB array reference object.
}
}
\value{
Returns a vector of dimension names.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
