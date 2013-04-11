\name{dimnames.scidb}
\alias{dimnames.scidb}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
dimnames
}
\description{
Return a list of dimension label arrays.
}
\usage{
\method{dimnames}{scidb}(x)
}
\arguments{
  \item{x}{ SciDB array reference object.  }
}
\details{
The scidb package can label SciDB array coordinate systems with
indices from other SciDB arays.
}
\value{
A list of dimension label arrays.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{dimnames<-}, \link{rownames}, \link{colnames}}
}
\examples{
\dontrun{
A <- as.scidb(matrix(rnorm(20),nrow=5))
colnames(A) <- as.scidb(data.frame(letters[1:4]))
dimnames(A)
}}
