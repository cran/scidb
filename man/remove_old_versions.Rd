\name{remove_old_versions}
\alias{remove_old_versions}
\title{
Remove all but most recent SciDB versions of an array.
}
\description{
Remove all but most recent SciDB versions of an array.
}
\usage{
  remove_old_versions(stored_array)
}
\arguments{
  \item{stored_array}{ A \code{scidb} or \code{scidbdf} object that refers
  to a named, non-temporary SciDB array.}
}
\value{
NULL is invisibly returned on success, otherwise an error is thrown.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
