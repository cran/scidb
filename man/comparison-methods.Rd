\name{comparison-methods}
\docType{methods}
\alias{\%<\%}
\alias{\%<\%,scidb-method}
\alias{\%<\%,scidbdf-method}
\alias{\%>\%}
\alias{\%>\%,scidb-method}
\alias{\%>\%,scidbdf-method}
\alias{\%<=\%}
\alias{\%<=\%,scidb-method}
\alias{\%<=\%,scidbdf-method}
\alias{\%>=\%}
\alias{\%>=\%,scidb-method}
\alias{\%>=\%,scidbdf-method}
\alias{\%==\%}
\alias{\%!=\%}
\alias{\%==\%,scidb-method}
\alias{\%==\%,scidbdf-method}
\alias{\%!=\%,scidb-method}
\alias{\%!=\%,scidbdf-method}
\title{Masking comparison methods from package \pkg{scidb}}
\description{
The binary operators described here perform comparison operations that
return a sparse SciDB array of the same shape as the input array but
only containing entries where the comparison evaluates to TRUE.

Compare this with standard R comparison operators on SciDB arrays,
that return an array populated with TRUE or FALSE values.

Masked comparison arrays can be used to efficiently index other
SciDB arrays.
}
\usage{
\S4method{\%<\%}{scidb}(x,y)
\S4method{\%<\%}{scidbdf}(x,y)
\S4method{\%>\%}{scidb}(x,y)
\S4method{\%>\%}{scidbdf}(x,y)
\S4method{\%<=\%}{scidb}(x,y)
\S4method{\%<=\%}{scidbdf}(x,y)
\S4method{\%>=\%}{scidb}(x,y)
\S4method{\%>=\%}{scidbdf}(x,y)
\S4method{\%==\%}{scidb}(x,y)
\S4method{\%==\%}{scidbdf}(x,y)
\S4method{\%!=\%}{scidb}(x,y)
\S4method{\%!=\%}{scidbdf}(x,y)
}
\arguments{
  \item{x}{A \code{scidb} or \code{scidbdf} object.}
  \item{y}{A scalar value.}
}
\details{
The comparisons outlined here are limited to scalars. A future
version will include element-wise comparison between arrays. For now,
use the \code{bind} and \code{merge} functions to manually perform
element-wise comparisons.

The standard R comparison operators by convention return an array of the
same size as \code{x} with \code{TRUE} or \code{FALSE} values indicating
the result of the comparison for each cell. That kind of output is
especially useful in subsequent aggregations, for example.

The alternate comparison methods outlined here return an array of the
same shape as \code{x}, but masked to only contain values in cells
where the
condition evaluates to \code{TRUE}. Remaining cells are empty.
This kind of comparison method is useful to quickly extract the values
that meet the condition, and also to use the masked array as an
index to subset other SciDB arrays.

The examples below illustrate each kind of comparison operator.
}
\value{
A \code{scidb} or \code{scidbdf} array of the same shape as \code{x}.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
> set.seed(1)
> x=as.scidb(rnorm(10))
> x[]
# [1] -0.6264538  0.1836433 -0.8356286  1.5952808  0.3295078 -0.8204684
# [7]  0.4874291  0.7383247  0.5757814 -0.3053884
> (x < 0)[]
# [1]  TRUE FALSE  TRUE FALSE FALSE  TRUE FALSE FALSE FALSE  TRUE
> (x %<% 0)[]
#sparse vector (nnz/length = 4/10) of class "dsparseVector"
# [1] -0.6264538          . -0.8356286          .          . -0.8204684
# [7]          .          .          . -0.3053884

# %<% is simply an alternate version of Filter:
> Filter("val < 0", x)[]
#sparse vector (nnz/length = 4/10) of class "dsparseVector"
# [1] -0.6264538          . -0.8356286          .          . -0.8204684
# [7]          .          .          . -0.3053884

# Sparse filtered output is useful to use to index SciDB arrays. The next
# example selects just the entries of the array that meet the condition:
> x[x %<% 0][]
# [1] -0.6264538 -0.8356286 -0.8204684 -0.3053884

# The TRUE/FALSE output array is useful to aggregate by groups defined
# by the condition. The next example computes the mean of the entries
# that are less than zero, and the mean of the entries that are greater
# than or equal to zero:
> aggregate(x, by=(x<0), FUN=mean)[]
#  condition_index    val_avg condition
#0               0  0.6516612     false
#1               1 -0.6469848      true
}}
