\name{dimension_rename}
\alias{dimension_rename}
\title{
Rename a dimension.
}
\description{
Rename a SciDB array dimension.
}
\usage{
dimension_rename(x, old, `new`, `eval`=FALSE)
}
\arguments{
  \item{x}{A \code{scidb}  or \code{scidbdf} object.}
  \item{old}{A character string representation of the old dimension name.}
  \item{new}{A character string representation of the new dimension name.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\value{
A new \code{scidb} or \code{scidbdf} object.
}
\note{
If you don't specify the \code{old} argument, it defaults to the dimension
names of \code{x}.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
  \code{\link{attribute_rename}}
  \code{\link{cast}}
}
\examples{
\dontrun{
x <- as.scidb(iris)
y <- dimension_rename(x, "row", "i")

str(y)
}}
