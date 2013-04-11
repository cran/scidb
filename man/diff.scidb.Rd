\name{diff.scidb}
\alias{diff.scidb}
\title{
diff
}
\description{
Apply a finite difference operator to a numeric series.
}
\usage{
\method{diff}{scidb}(x,lag=1,...)
}
\arguments{
  \item{x}{A \code{scidb} object with a numeric attribute.}
  \item{lag}{An intereger lag value (see description).}
  \item{...}{Optional arguments, not presently used.}
}
\value{
If 'x' is a vector of length 'n' and starting index 1, then the
computed result is equal to the difference
'x[(1+lag):n] - x[1:(n-lag)]'.

If 'x' is a matrix then the difference operations are carried out
on each column separately.

SciDB missing (NULL) and NaN values propagate.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
