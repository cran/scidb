\name{slice}
\alias{slice}
\title{
Low-level SciDB slice operator
}
\description{
Slice a SciDB array along a coordinate value returning a new SciDB array of lower dimension.
}
\usage{
slice(x, d, n, `eval`=FALSE)
}
\arguments{
  \item{x}{A \code{scidb}  object.}
  \item{d}{A character string representation of the coordinate axis to slice along, or
           logical or numeric index of the axis.}
  \item{n}{The value to slice on.}
  \item{eval}{(Optional) If true, execute the query and store the reult array.
               Otherwise defer evaluation.}
}
\value{
A new \code{scidb} object of lower dimension.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
# Build a 5x5 matrix:
x <- build("random()%10", dim=c(5,5))
print(x[])

# Slice the 2nd row (i=1 since SciDB matrices start with zero index by default)
y <- slice(x, "i", 1)
print(y[])
}}
