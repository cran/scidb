\name{subset-methods}
\docType{methods}
\alias{subset}
\alias{subset,scidb-method}
\alias{subset,scidbdf-method}
\title{Methods for Function \code{subset} in Package \pkg{scidb}}
\description{
SciDB \code{filter} operations.
}
\usage{
\S4method{subset}{scidb}(X,subset,eval)
\S4method{subset}{scidbdf}(X,subset,eval)
}
\arguments{
  \item{X}{A \code{scidb} or \code{scidbdf} object.}
  \item{subset}{A character string representing a SciDB filter expression.}
  \item{eval}{(Optional) If true, execute the query. Otherwise defer evaluation.}
}
\details{
Perform a SciDB \code{filter} operation on a SciDB array. The \code{subset} filter expression argument can be any valid SciDB filter operation.
}
\note{
We use the function name \code{subset} because the R function \code{subset} closely matches the native functionality of the SciDB operator named \code{filter}. The R \code{filter} function is an unrelated timeseries function.
}
\value{A \code{scidb}, \code{scidbdf} reference object. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
library("scidb")
scidbconnect()

# Create a copy of the iris data frame in a 1-d SciDB array named "iris."
# Note that SciDB attribute names will be changed to conform to SciDB
# naming convention.
x <- as.scidb(iris,name="iris")

y <- subset(x,"Species='setosa'")
}
}
