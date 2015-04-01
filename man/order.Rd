\name{order.scidb-methods}
\docType{methods}
\alias{order,scidb-method}
\alias{order,ANY-method}
\alias{order.scidb,scidb-method}
\alias{order.scidb}
\title{Methods for Function \code{order} in Package \pkg{scidb}}
\description{
\code{order}
returns a permutation which rearranges its first argument
into ascending or descending order, breaking ties by further
arguments. 
}

\usage{
\S4method{order.scidb}{scidb}(..., na.last = TRUE, decreasing = FALSE)
}
\arguments{
  \item{...}{A \code{scidb} vector object.}
  \item{decreasing}{A logical value: TRUE means sort in decreasing order.}
  \item{na.last}{NOT USED--for compatability with native R functions only.}
}
\value{A \code{scidb} reference object.}
\note{
Due to technical difficulties encountered when dispatching on \code{...}, the
\code{order.scidb} method must be called by its full name.
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
