\name{sort-methods}
\docType{methods}
\alias{sort}
\alias{sort,scidb-method}
\alias{sort,scidbdf-method}
\title{Methods for Function \code{sort} in Package \pkg{scidb}}
\description{
Aggregate a SciDB array object grouped by a subset of its dimensions and/or attributes.
}

\usage{
\S4method{sort}{scidb}(x, decreasing=FALSE, attributes, eval)
\S4method{sort}{scidbdf}(x, decreasing=FALSE, attributes, eval)
}
\arguments{
  \item{x}{A \code{scidb} or \code{scidbdf} object.}
  \item{decreasing}{A logical value: TRUE means sort in decreasing order.}
  \item{attributes}{(Required) A character vector of attribute names from array \code{x} to sort on}
  \item{eval}{(Optional) If true, execute the query and store the result array. Otherwise defer evaluation.}
}
\details{
Create a new array reference object that represents the sorted version of array
\code{x}.  The output array reference has the same shape and number of
attributes as \code{x} but the specified attributes appear in sorted order.
}
\value{A \code{scidb} or \code{scidbdf} reference object.}
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

# Sort x by Petal_Width and Petal_Length:
a <- sort(x, attributes=c("Petal_Width","Petal_Length"))
}
}
