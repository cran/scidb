\name{rename}
\alias{rename}
\title{
Rename a SciDB array.
}
\description{
Rename a SciDB array, returning a new reference object and optionally
setting persistence of the object.
}
\usage{
  rename(A, name=A@name, gc)
}
\arguments{
  \item{A}{ A \code{scidb} or \code{scidbdf} object.  }
  \item{name}{An optional new name for the object.}
  \item{gc}{An optional logical value indicating object persistence. If
TRUE, then the backing SciDB array will be deleted by the R garbage collector
when corresponding R references are deleted.}
}
\value{
A new \code{scidb} or \code{scidbdf} object.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
