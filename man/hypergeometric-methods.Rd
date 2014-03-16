\name{hypergeometric-methods}
\docType{methods}
\alias{phyper}
\alias{qhyper}
\alias{phyper,scidb_or_scidbdf-method}
\alias{qhyper,scidb_or_scidbdf-method}
\title{Hypergeometric functions in Package \pkg{scidb}}
\description{
Distribution and quantile functions 
for the hypergeometric distribution.
}
\usage{
\S4method{phyper}{scidb_or_scidbdf}(x, q, m, n, k, new="p",`eval`=FALSE)
\S4method{qhyper}{scidb_or_scidbdf}(x, p, m, n, k, new="q",`eval`=FALSE)
}
\arguments{
  \item{x}{A \code{scidb} or \code{scidbdf} object.}
  \item{q}{A SciDB attribute name (character) or numeric constant representing the 
          quantile, for example the number of white balls drawn without replacement
          from an urn which contains both black and white balls.}
  \item{m}{A scidb attribute name or numeric constant representing
           the number of white balls in the urn.}
  \item{n}{A scidb attribute name or numeric constant representing
           the number of black balls in the urn.}
  \item{k}{A scidb attribute name or numeric constant representing
            the number of balls drawn from the urn.}
  \item{p}{A scidb attribute name or numeric constant representing
           probability between 0 and 1.}
  \item{new}{A character name for the result attribute.}
  \item{eval}{(Optional) If true, execute the query. Otherwise defer evaluation.}
}
\details{
  The hypergeometric distribution is used for sampling \emph{without}
  replacement.  The density of this distribution with parameters
  \code{m}, \code{n} and \code{k} is given by

  \deqn{
    p(x) = \left. {m \choose x}{n \choose k-x} \right/ {m+n \choose k}%
  }{p(x) =      choose(m, x) choose(n, k-x) / choose(m+n, k)}

  for \eqn{x = 0, \ldots, k}.

  The quantile is defined as the smallest value \eqn{x} such that
  \eqn{F(x) \ge p}, where \eqn{F} is the distribution function.

  \code{phyper} gives the distribution
  function, \code{qhyper} gives the quantile function.
}
\value{A \code{scidb}, \code{scidbdf} reference object. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
# Write me
}
}
\keyword{distribution}
