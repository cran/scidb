\name{subarray}
\alias{subarray}
\title{
Low-level SciDB subarray operator
}
\description{
Return a new SciDB subarray object.
}
\usage{
subarray(x, limits, schema, between=FALSE, `eval`=FALSE)
}
\arguments{
  \item{x}{A \code{scidb}  object.}
  \item{limits}{A SciDB-style character string representation of the coordinate limits.}
  \item{schema}{An optional SciDB array object or SciDB schema string that defines
                the coordinate limits.}
  \item{between}{If TRUE, form a SciDB between-style query, otherwise use subarray.}
  \item{eval}{If TRUE, execute the query and store the reult array. Otherwise defer evaluation.}
}
\value{
A new \code{scidb} object.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
