\name{hist-methods}
\docType{methods}
\alias{hist}
\alias{hist,scidb-method}
\alias{hist,scidbdf-method}
\title{Methods for Function \code{hist} in Package \pkg{scidb}}
\description{
The generic function 'hist' computes a histogram of the given data
values.  If 'plot = TRUE', the resulting object of class
\code{histogram} is plotted by \code{plot.histogram}, before it is
returned.
}

\usage{
\S4method{hist}{scidb}(x, breaks=10, right=FALSE, materialize=TRUE, `eval`=FALSE, `plot`=TRUE, ...)
\S4method{hist}{scidbdf}(x, breaks=10, right=FALSE, materialize=TRUE, `eval`=FALSE, `plot`=TRUE, ...)
}
\arguments{
  \item{x}{A \code{scidb} or \code{scidbdf} object.}
  \item{breaks}{a single number giving the number of cells for the histogram.}
  \item{right}{logical; if \code{TRUE}, the histogram cells are right-closed (left open) intervals.}
  \item{materialize}{logical; if \code{TRUE}, the histogram results are materialized into R vectors and returned as a standard object of class \code{histogram}. Otherwise the results are returned as SciDB objects.}
  \item{eval}{Optional, logical; If \code{TRUE}, execute the query and store the reult array. Otherwise defer evaluation.}
  \item{plot}{Optional, logical; If \code{TRUE}, plot result before returning.}
  \item{...}{Optional; additional arguments for plotting.}
}
\details{
This histogram function only supports equidistant breaks. If 'right = TRUE'
(default), the histogram cells are intervals of the form '(a, b]', i.e., they
include their right-hand endpoint, but not their left one.
}
\value{A \code{histogram} object, which is a list of components:

\itemize{
  \item breaks: the n+1 cell boundaries.
  \item counts: n integers; for each cell, the number of \code{x[]} inside.
  \item density: estimated density values.
  \item mids: the n cell midpoints.
}
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
