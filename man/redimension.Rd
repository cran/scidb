\name{redimension}
\alias{redimension}
\title{
redimension
}
\description{
The \code{redimension} function is a wrapper to the SciDB `redimension` operator.
}
\usage{
redimension(x, schema, dim, FUN, `eval`=FALSE)
}
\arguments{
  \item{x}{A SciDB array object of class scidb or scidbdf.}
  \item{schema}{An optional SciDB array object of class scidb, scidbdf,
           or a character string representation of the output array schema.}
  \item{dim}{An optional character vector or list of new dimension names
             from the union of dimension and attribute names of x. Exactly
             one of the \code{s} and \code{dim} arguments must be specified.}
  \item{FUN}{An optional reduction function applied when \code{redimension}
             collapses multiple values.}
  \item{eval}{(Optional) If true, execute the query and store the reult array.
              Otherwise defer evaluation.}
}
\value{
A \code{scidb} object.
}
\details{
\code{Redimension} is a core SciDB operation. It can change the dimensionality,
shape, and partitioning of arrays, and transform array attributes into array
dimensions and vice versa. \code{Redimension} can also apply reduction functions
to values when dimensions are removed, similarly to grouped aggregates.

The R package \code{redimension} function presents several forms. The most
direct form takes a SciDB array reference \code{x} and an desired output schema
\code{s} and directly applies the SciDB \code{redimension} operator.

Alternatively, users may specify a character vector or list of \code{dim}
values that represent new array coordinate axes. These values should be a
subset of the union of attributes and dimension names in the input array
\code{x}. Note that they must also be valid \code{int64} types.

When \code{redimension} reduces the dimensionality of an array, it's likely
that multiple values may fall into the same output array cell. When this
occurs, SciDB's default behavior randomly selects one of the possible values
for output. Alternatively, users may specify a reduction function in the
\code{FUN} argument or explicitly specify reductions using SciDB syntax in
the schema argument. Note that the indicated reduction function will be
applied to all the attributes. If you need more control over the reduction,
explicitly specify the output schema instead.
}
\note{
Use \code{eval=FALSE} to defer expression evaluation for efficiency. Results
can be composed to form complex query expressions that can be evaluated in one
transaction.
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
z <- redimension(y, dim="class", FUN=max)
}}
