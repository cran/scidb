\name{merge-methods}
\docType{methods}
\alias{merge}
\alias{merge,scidb-method}
\alias{merge,scidbdf-method}
\title{Methods for Function \code{merge} in Package \pkg{scidb}}
\description{
SciDB \code{cross}, \code{cross_join}, and \code{join} operations.
}
\usage{
\S4method{merge}{scidb}(x,y, by, by.x, by.y, eval)
\S4method{merge}{scidbdf}(x,y, by, by.x, by.y, eval)
}
\arguments{
  \item{x}{A \code{scidb} or \code{scidbdf}}
  \item{y}{A \code{scidb} or \code{scidbdf}}
  \item{by}{An optional vector of common dimension or attribute names to join on. See details below.}
  \item{by.x}{An optional vector of dimension or attribute names of array \code{x} to join on. See deails.}
  \item{by.y}{An optional vector of dimension or attribute names of array \code{y} to join on. See deails.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\details{
Only one of either \code{by} or both \code{by.x} and \code{by.y} may be
specified.  If none of the \code{by*} arguments are specified, the result is
the Cartesian product of \code{x} and \code{y} computed with the SciDB
\code{cross} operator. (Note that this differs from the usual R \code{merge}
behavior!)

If only \code{by} is specified, the dimension names or attribute name in
\code{by} are assumed to be common across \code{x} and \code{y}.  Otherwise
dimension names or attribute names are matched across the names listed in
\code{by.x} and \code{by.y}, respectively.

If dimension names are specified and \code{by} contains all the dimensions
in each array, then the SciDB \code{join} operator is used, otherwise SciDB's
\code{cross_join} operator is used. In each either case, the output is a cross
product set of the two arrays along the specified dimensions.

If \code{by} or each of \code{by.x} and \code{by.y} list a single dimension
name, the indicated attributes will be lexicographically ordered as categorical
variables and SciDB will redimension each array along new coordinate systems
defined by the attributes, and then those redimensioned arrays will be joined.
This method presently limits joins along attributes to a single attribute from
each array. The output array will contain additional columns showing the
attribute factor levels used to join the arrays.

This method is presently limited to SQL-like `natural joins`, a special
case of inner joins corresponding to the \code{all=FALSE} case in
the standard R \code{merge} function. A future version of this package
will include additional join cases.

The various SciDB \code{join} operators generally require that the arrays have
identical partitioning (coordinate system bounds, chunk size, etc.) in the
common dimensions.  The \code{merge} method attempts to rectify SciDB
arrays along the specified dimensions as required before joining.
}
\value{A \code{scidb} or \code{scidbdf} reference object. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
# Example
library("scidb")
scidbconnect()

# Create a copy of the iris data frame in a 1-d SciDB array named "iris."
# Note that SciDB attribute names will be changed to conform to SciDB
# naming convention.
x <- as.scidb(iris,name="iris")

a <- project(x, "Species")
b <- project(x, "Petal_Length")

c <- merge(a, b, by="row")
}
}
