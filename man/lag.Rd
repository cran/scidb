\name{lag}
\docType{methods}
\alias{lag}
\alias{lag,scidb-method}
\title{
lag
}
\description{
Lag or lead a time series
}
\usage{
\S4method{lag}{scidb}(x, k=1, dim=1, `eval`=FALSE)
}
\arguments{
  \item{x}{A scidb array object.}
  \item{k}{Coordinate system units to lag by.}
  \item{dim}{SciDB array dimension to lag along.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\value{
A scidb array object with identical schema as \code{x}, but whose values are shifted along the coordinate axis \code{dim} by the lag value \code{k}.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- build("i",dim=10,names=c("x","i"),start=1,type="double")
y <- lag(x)
merge(x,y,by="i",all=TRUE)
}}
