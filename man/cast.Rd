\name{cast}
\alias{cast}
\title{
Low-level SciDB cast operator
}
\description{
Apply the low-level SciDB cast operator to a SciDB array.
}
\usage{
cast(x, s, `eval`=FALSE)
}
\arguments{
  \item{x}{A \code{scidb}  or \code{scidbdf} object.}
  \item{s}{A character string representation of a SciDB array schema.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\note{
The low-level SciDB cast operator can be used to change attribute and dimension names of an arary.
}
\value{
A new \code{scidb} or \code{scidbdf} object.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- as.scidb(iris)
y <- cast(x,"<a:double,b:double,c:double,e:double,f:string>[row=1:150,150,0]")

print(head(y))
}}
