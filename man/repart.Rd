\name{repart}
\alias{repart}
\title{
repart
}
\description{
The \code{repart} function is a wrapper to the SciDB `repart` operator.
}
\usage{
repart(x, schema, upper, chunk, overlap, eval=FALSE)
}
\arguments{
  \item{x}{A SciDB array object of class scidb or scidbdf.}
  \item{schema}{An optional character array schema.}
  \item{upper}{An optional vector of new array upper bounds.}
  \item{chunk}{An optional vector of new array chunk sizes.}
  \item{overlap}{An optional vector of new array overlap values.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\value{
A \code{scidb} object.
}
\note{
Repartition SciDB array data by chaning the array chunk sizes and/or
overlap values. This function is also used to change the upper bound of
an array coordinate axis.

If \code{schema} is specified, the remaining shape arguments are ignored.

The arguments \code{upper, chunk}, and \code{overlap} must each have the same
number of elements as the dimension of the SciDB array \code{x}. The
\code{upper} argument should contain double-precision integer values (standard
R array index values). The \code{chunk} and \code{overlap} arguments should
contain integer values.

Optional arguments left unspecified remain unchanged in the output array.

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
# Add chunk overlap to an array:

x <- as.scidb(iris)
y <- repart(x, overlap=3)
}}
