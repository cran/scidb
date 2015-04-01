\name{na.locf-methods}
\docType{methods}
\alias{na.locf}
\alias{na.locf,scidb-method}
\alias{na.locf,scidbdf-method}
\title{Methods for Function \code{na.locf} in Package \pkg{scidb}}
\description{
Generic function for replacing each missing or empty value with the most recent
non-missing value prior to it.
}

\usage{
\S4method{na.locf}{scidb}(object, along=dimensions(object)[1],fill_sparse=FALSE,`eval`=FALSE)
}
\arguments{
  \item{object}{A \code{scidb} or \code{scidbdf} object.}
  \item{along}{A single SciDB array dimension name to replace missing values along.}
  \item{fill_sparse}{Specify TRUE to impute sparse values, leave as the default FALSE
    to only impute missing (SciDB NULL) values.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\details{
Unlike the usual \code{na.locf} function from the \code{zoo} package, the SciDB
\code{na.locf} function fills in both missing (SciDB \code{null} values) and
empty (SciDB sparse) values with the last non-missing and non-sparse value
along the indicated dimension.

Time series represented in SciDB are often sparse arrays. The \code{na.locf}
function defines a conveniet way to  fill in all missing values along the time
coordinate axis down to the time resolution.

Caution! If fill_sparse=TRUE then
the output array is a mostly dense, filled-in version of the input
array. If the time resolution is very fine and the input array very sparse,
then the output array can be huge. Consider using \code{regrid} first on
very fine time scales to reduce their resolution.
}
\note{
The default \code{na.locf} method in the \code{zoo} package unforunately
overrides this function (it uses \code{ANY} in its method signature). If
you need to use SciDB arrays and the \code{zoo} package, prefix SciDB's
version with \code{scidb::na.locf(...)}.
}
\value{A \code{scidb} reference object. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}
