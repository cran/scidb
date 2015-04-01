\name{merge-methods}
\docType{methods}
\alias{merge}
\alias{merge,scidb-method}
\alias{merge,scidbdf-method}
\title{Methods for Function \code{merge} in Package \pkg{scidb}}
\description{
SciDB \code{merge}, \code{cross_join}, and \code{join} operations.
}
\usage{
\S4method{merge}{scidb}(x,y, by=intersect(dimensions(x),dimensions(y)), by.x, by.y, merge, all)
\S4method{merge}{scidbdf}(x,y, by=intersect(dimensions(x),dimensions(y)), by.x, by.y, merge, all)
}
\arguments{
  \item{x}{A \code{scidb} or \code{scidbdf}}
  \item{y}{A \code{scidb} or \code{scidbdf}}
  \item{by}{(Optional) Vector of common dimension or attribute names or dimension indices
            to join on. See details below.}
  \item{by.x}{(Optional) Vector of dimension or attribute names or dimension indices
                of array \code{x} to join on. See deails.}
  \item{by.y}{(Optional) Vector of dimension or attribute names or dimension indices
                   of array \code{y} to join on. See deails.}
  \item{merge}{(Optional) If TRUE, perform a SciDB merge operation instead
               of join.}
  \item{all}{(Optional) If TRUE, perform outer join. Defaults to inner join.}
}
\details{
Only one of either \code{by} or both \code{by.x} and \code{by.y} may be
specified.  If none of the \code{by.x},\code{by.y} arguments are specified, and
\code{by=NULL} the result is the Cartesian cross product of \code{x} and
\code{y}.  The default value of \code{by} performs a \code{cross_join} or
\code{join} along common array dimensions. The \code{by} arguments may
be specified by name or 1-based integer dimension index.

If only \code{by} is specified, the dimension names or attribute name in
\code{by} are assumed to be common across \code{x} and \code{y}.  Otherwise
dimension names or attribute names are matched across the names listed in
\code{by.x} and \code{by.y}, respectively.

If dimension names are specified and \code{by} contains all the dimensions
in each array, then the SciDB \code{join} operator is used, otherwise SciDB's
\code{cross_join} operator is used. In each either case, the output is a cross
product set of the two arrays along the specified dimensions.

If \code{by} or each of \code{by.x} and \code{by.y} list a single attribute
name, the indicated attributes will be lexicographically ordered as categorical
variables and SciDB will redimension each array along new coordinate systems
defined by the attributes, and then those redimensioned arrays will be joined.
This method limits joins along attributes to a single attribute from
each array. The output array will contain additional columns showing the
attribute factor levels used to join the arrays.

Specify \code{merge=TRUE} to perform a SciDB merge operation instead
of a SciDB join.

If \code{all=FALSE} (the default), then a SQL-like `natural join` 
(an inner join) is performed. If \code{all=TRUE} then SQL-like `outer join`
is performed, but this case has some limitiations; in particular the
outer join is not available yet for the \code{merge=TRUE} case, for
joining on SciDB attributes, or for joining on subsets of dimensions.

The various SciDB \code{join} operators generally require that the arrays have
identical partitioning (coordinate system bounds, chunk size, etc.) in the
common dimensions.  The \code{merge} method attempts to rectify SciDB
arrays along the specified dimensions as required before joining. Those
dimensions must at least have common lower index bounds.

The merge function may rename SciDB attributes and dimensions as required
to avoid name conflicts in SciDB. See the last example for an
illustration.
}
\value{A \code{scidb} or \code{scidbdf} reference object. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
# Create a copy of the iris data frame in a 1-d SciDB array named "iris."
# Note that SciDB attribute names will be changed to conform to SciDB
# naming convention.
x <- as.scidb(iris,name="iris")

a <- x$Species
b <- x$Petal_Length

c <- merge(a, b, by="row")
merge(b, b, by="row", merge=TRUE)


# Here is an example that joins on SciDB array attributes instead of
# dimensions. It works by enumerating the attribute values and
# redimensioning along those.
set.seed(1)
a <- as.scidb(data.frame(a=sample(10,5),b=rnorm(5)))
b <- as.scidb(data.frame(u=sample(10,5),v=rnorm(5)))
merge(x=a, y=b, by.x="a", by.y="u")[]


# The following example joins on a subset of coordinate axes:
x <- build(5.5, c(3,3));                  print(schema(x))
y <- build(1.1, c(3,3),chunksize=c(2,1)); print(schema(y))
z <- merge(x, y, by="i")
print(schema(z))

}
}
