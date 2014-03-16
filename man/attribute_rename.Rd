\name{attribute_rename}
\alias{attribute_rename}
\title{
Rename an attribute.
}
\description{
Rename a SciDB array attribute.
}
\usage{
attribute_rename(x, old, `new`, `eval`=FALSE)
}
\arguments{
  \item{x}{A \code{scidb}  or \code{scidbdf} object.}
  \item{old}{A character string representation of the old attribute name.}
  \item{new}{A character string representation of the new attribute name.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\value{
A new \code{scidb} or \code{scidbdf} object.
}
\note{
If you don't specify the \code{old} argument, it will default to the attribute
names of \code{x}.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
  \code{\link{dimension_rename}}
  \code{\link{cast}}
}
\examples{
\dontrun{
x <- as.scidb(iris)
y <- attribute_rename(x, "Species", "Group")

print(head(y))
}}

