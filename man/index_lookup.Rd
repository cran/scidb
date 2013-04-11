\name{index_lookup}
\alias{index_lookup}
\title{
index_lookup
}
\description{
The \code{index_lookup} function is a wrapper to the SciDB `index_lookup` operator.
It produces a new SciDB array that joins the unqiue indices defined in the array
\code{I} with values looked up in array \code{X} for attribute \code{attr}. Use
the \code{index_lookup} with the \code{unique} and \code{sort} functions.
}
\usage{
index_lookup(X, I, attr, new_attr, eval=FALSE)
}
\arguments{
  \item{X}{A scidb or scidbdf object}
  \item{I}{A scidb or scidbdf object}
  \item{attr}{A character string attribute name from the \code{X} array}
  \item{new_attr}{An optional character name for the new attribute, defaults to \code{attr} + "_index"}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\value{
A \code{scidb}  or \code{scidbdf} object.
}
\note{
Use \code{eval=FALSE} to defer expression evaluation for efficiency.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
library("scidb")
scidbconnect()

x <- as.scidb(iris)

# Create a unique list of elements of the "Species" attribute.
# Note that we choose to defer evaluation of this expression.
y <- unique(sort(project(x,"Species")), eval=FALSE)

# Append a new attribute to the array x called "Species_index" that
# enumerates the unique values of the "Species" attribute:
z <- index_lookup(x, y, "Species", eval=FALSE)

print(head(z))
}}
