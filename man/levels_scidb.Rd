\name{levels_scidb}
\alias{levels_scidb}
\title{
Hybrid R/SciDB factors
}
\description{
Return levels for this factor vector from a SciDB dimension array.
}
\usage{
levels_scidb(x)
}
\arguments{
  \item{x}{
An R factor vector created by \code{factor_scidb}.
}
}
\value{
The SciDB dimension array levels associated with the factors in this vector.
Those levels may differ from the enumeration used for the R factor levels
returned by \code{levels(x)}.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
