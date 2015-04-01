\name{redimension}
\alias{redimension}
\title{
redimension
}
\description{
The \code{redimension} function is a wrapper to the SciDB `redimension` operator.
}
\usage{
redimension(x, schema, dim, FUN)
}
\arguments{
  \item{x}{A SciDB array object of class scidb or scidbdf.}
  \item{schema}{An optional SciDB array object of class scidb, scidbdf,
           or a character string representation of the output array schema.}
  \item{dim}{An optional character vector or list of new dimension names
             from the *union* of dimension and attribute names of x, or
             an integer positional index of a dimension of x.  Exactly
             one of the \code{schema} and \code{dim} arguments must be specified.}
  \item{FUN}{An optional reduction function or character SciDB aggregation expression.}
}
\value{
A \code{scidb} object.
}
\details{
\code{Redimension} is a core SciDB operation. It can change the dimensionality,
shape, and partitioning of arrays, and transform array attributes into array
dimensions and vice versa. \code{Redimension} can also apply reduction functions
to values when dimensions are removed, similarly to group-by aggregation.

The R package \code{redimension} function presents several forms. The most
direct form takes a SciDB array reference \code{x} and an desired output schema
\code{s} and directly applies the SciDB \code{redimension} operator.

Alternatively, users may specify a character vector or list of \code{dim}
values that represent new array coordinate axes. These values should be a
subset of the union of attributes and dimension names in the input array
\code{x}. Note that they must also be valid \code{int64} types. If attributes
are selected that are not \code{int64}, then an attempt will be made to create
an auxilliary dimension array to enumerate unique attribute values.  References
to any created auxilliary arrays are placed in the dimnames list of the output
array (and can be inspected with \code{dimnames}, \code{rownames},
code{colnames}, etc.).

When \code{redimension} reduces the dimensionality of an array, it's likely
that multiple values may fall into the same output array cell. When this
occurs, SciDB's default behavior randomly selects one of the possible values
for output. Alternatively, users may specify a reduction function in the
\code{FUN} argument or explicitly specify reductions using SciDB syntax in the
schema argument. The indicated reduction function will be applied to all the
attributes. You can also explicitly specify a character-valued SciDB
aggregation expression.

If \code{FUN} contains a SciDB aggregation expression, it *must* declare
the name of each output attribute using the SciDB \code{AS} syntax. See
the examples.

Reductions involving database user-defined types or aggregation functions
must explicitly specify an output schema.

}
\note{
The option \code{eval} is deprecated and will be removed in the future. Use \code{scidbeval} instead.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
# Upload iris to SciDB:
x <- as.scidb(iris)

# bind an example new 'class' column:
y <- bind(x, "class", "iif(Petal_Width>2, int64(1), 0)")

# Data counts along a dimension:
z <- redimension(y, dim="class", FUN=count)
# Example output:
##  z[]
##    0   1 
##  127  23 

# Contingency table along two dimensions:
z <- redimension(y, dim=c("class", "Species"), FUN=count)
# Example output:
##  z[]
##  2 x 3 sparse Matrix of class "dgCMatrix"
##    setosa versicolor virginica
##  0     50         50        27
##  1      .          .        23

# More aggregation examples
set.seed(1)
A <- bind(as.scidb(matrix(rnorm(25),5)), "m", 2)
redimension(A, dim="i",
  FUN="avg(m) as mavg, count(val) as count, min(val) as minval, sum(m) as msum")[]
# Example output:
##   mavg count      minval msum
## 0    2     5 -0.82046838   10
## 1    2     5 -0.01619026   10
## 2    2     5 -0.83562861   10
## 3    2     5 -2.21469989   10
## 4    2     5 -0.30538839   10

}}
