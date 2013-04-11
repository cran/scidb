\name{project}
\alias{project}
\title{
project
}
\description{
The \code{project} function is a wrapper to the SciDB `project` operator.
It creates a new array that is a subset of the input array limited
to a set of specified attributes.
}
\usage{
project(X, attributes, eval)
}
\arguments{
  \item{X}{A scidb or scidbdf object}
  \item{attributes}{A character vector of attributes to project on to}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\value{
A \code{scidb} object.
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
x <- as.scidb(iris)
y <- project(x, c("Sepal_Length","Petal_Length","Species"))

print(head(y))
}}
