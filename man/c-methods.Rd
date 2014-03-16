\name{c-methods}
\docType{methods}
\alias{c}
\alias{c,scidbdf-method}
\title{Concatenate SciDB arrays}
\description{
Concatenate SciDB arrays along one coordinate axis.
}
\usage{
\S4method{c}{scidbdf}(x,y,`eval`=FALSE)
}
\arguments{
  \item{x}{A \code{scidbdf} object.}
  \item{y}{A \code{scidbdf} object with the same attribute pattern as \code{x}.}
  \item{eval}{(Optional) If true, execute the query. Otherwise defer evaluation.}
}
\details{
Concatenate the SciDB arrays along their single dimension. The output array
will include the entries of array \code{y} below the entries of \code{x}.
The input arrays must have the same number of attributes and attribute types.
}
\note{
This method will soon be extended to n-d arrays and to accept a list of more
than two input arrays.
}
\value{A \code{scidbdf} reference object. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
data("iris")
x <- as.scidb(iris)
c(x,x)
}
}
