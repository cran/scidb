\name{sweep-methods}
\docType{methods}
\alias{sweep}
\alias{sweep,scidb-method}
\title{Methods for Function \code{sweep} in Package \pkg{scidb}}
\description{
Return an array obtained from an input array by sweeping out a
summary statistic.
}
\usage{
\S4method{sweep}{scidb}(x, MARGIN, STATS, FUN = "-", eval = FALSE, name)
}
\arguments{
  \item{x}{a \code{scidb} object.}
  \item{MARGIN}{a vector of indices giving the extent(s) of 'x' which
          correspond to 'STATS'.}
  \item{STATS}{the summary statistic which is to be swept out.}
  \item{FUN}{the function to be used to carry out the sweep.
         (Default is "-".)}
  \item{eval}{(Optional) If TRUE, execute the query and store the reult array.
               Otherwise defer evaluation.}
  \item{name}{If \code{x} is a multi-attribute array, specify the attribute
              to work on, default to the first listed array attribute.}
}
\details{
\code{STATS} must be a vector whose length matches the dimension size
of the specified \code{MARGIN}. Note that this is a more restrictive
condition than the usual R \code{sweep} function.

The default value of \code{FUN} is "-". \code{FUN} must be a
quoted binary operator.
}
\value{A \code{scidb} object. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
set.seed(1)
A <- as.scidb(matrix(rnorm(20),nrow=5))
# Center the matrix
B <- sweep(A, MARGIN=2, STATS=apply(A,2,mean))
# Let's see:
apply(B,2,mean)[]
}
}
