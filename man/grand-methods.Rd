\name{grand-methods}
\docType{methods}
\alias{sum}
\alias{sum,scidb-method}
\alias{sum,scidbdf-method}
\alias{mean}
\alias{mean,scidb-method}
\alias{mean,scidbdf-method}
\alias{median}
\alias{median,scidb-method}
\alias{median,scidbdf-method}
\alias{min}
\alias{min,scidb-method}
\alias{min,scidbdf-method}
\alias{max}
\alias{max,scidb-method}
\alias{max,scidbdf-method}
\alias{count}
\alias{count,scidb-method}
\alias{count,scidbdf-method}
\alias{var}
\alias{var,scidb-method}
\alias{var,scidbdf-method}
\alias{sd}
\alias{sd,scidb-method}
\alias{sd,scidbdf-method}
\title{Miscellaneous methods from package \pkg{scidb}}
\description{
The methods described here compute a global aggregate on a
SciDB array object and return a scalar value to R.
}

\usage{
\S4method{mean}{scidb}(x)
\S4method{mean}{scidb}(x)
\S4method{median}{scidb}(x)
\S4method{median}{scidbdf}(x)
\S4method{sum}{scidbdf}(x)
\S4method{sum}{scidbdf}(x)
\S4method{min}{scidb}(x)
\S4method{min}{scidbdf}(x)
\S4method{max}{scidb}(x)
\S4method{max}{scidbdf}(x)
\S4method{count}{scidb}(x)
\S4method{count}{scidbdf}(x)
\S4method{sd}{scidb}(x)
\S4method{sd}{scidbdf}(x)
\S4method{var}{scidb}(x)
\S4method{var}{scidbdf}(x)
}
\arguments{
  \item{x}{A \code{scidb} or \code{scidbdf} object.}
}
\details{
{\code{sum}} computes the global sum of the SciDB array elements (applies only to numeric types);
{\code{mean}} computes the arithmetic average of the SciDB array elements;
{\code{median}} computes the median of the SciDB array elements;
{\code{min}} computes the minimum of the SciDB array elements;
{\code{max}} computes the maximum of the SciDB array elements;
{\code{count}} returns the number of non-empty cell values in the SciDB array;
{\code{sd}} computes the standard deviation of the SciDB array elements (numeric only);
{\code{var}} computes the variance of the SciDB array elements (numeric only).
}
\note{
SciDB convention dictates that global aggregates only apply to the first
listed attribute of a multi-attribute SciDB array (e.g., a scidbdf data
frame-like object). Use \code{project(array, attribute)} to select a
different attribute--see the examples.
}
\value{The methods documented here always return a scalar value.}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
data("iris")
x <- as.scidb(iris)
sum(x$Petal_Length)
}
}
