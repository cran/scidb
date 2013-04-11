\name{cumulate}
\alias{cumulate}
\title{
cumulate
}
\description{
Use \code{cumulate} function to compute running operations along data,
for example cumulative sums.
}
\usage{
cumulate(x, expression, dimension, eval)
}
\arguments{
  \item{x}{A scidb or scidbdf object}
  \item{expression}{Any valid SciDB aggregate expression, expressed as a character string}
  \item{dimension}{An optional array dimension name (character) to run along. The default is to use the first dimension.}
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
  \code{\link{aggregate}}
}
\examples{
\dontrun{
x <- as.scidb(iris)
y <- cumulate(x, "sum(Petal_Width)")

print(head(y))
}}
