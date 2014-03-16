\name{aggregate-methods}
\docType{methods}
\alias{aggregate}
\alias{aggregate,scidb-method}
\alias{aggregate,scidbdf-method}
\title{Methods for Function \code{aggregate} in Package \pkg{scidb}}
\description{
Aggregate a SciDB array object grouped by a subset of its dimensions and/or attributes.
}

\usage{
\S4method{aggregate}{scidb}(x, by, FUN, eval, window, variable_window, unpack)
\S4method{aggregate}{scidbdf}(x, by, FUN, eval, window, variable_window, unpack)
}
\arguments{
  \item{x}{A \code{scidb} or \code{scidbdf} object.}
  \item{by}{(Optional) Either a single character string or a list of array dimension and/or attribute names to group by; or a SciDB array reference object to group by. Not required for \code{windowed} and grand aggregates--see details.}
  \item{FUN}{A character string representing a SciDB aggregation expression
             or a reduction function.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
  \item{window}{(Optional) If specified, perform a moving window aggregate along the specified coordinate windows--see details below.}
  \item{variable_window}{(Optional) If specified, perform a moving window aggregate over successive data values along the coordinate dimension axis specified by \code{by}--see details below.}
  \item{unpack}{(Optional) If TRUE, return an unpacked SciDB result as a
                 scidbdf dataframe-like object. It's sometimes useful to
                 set this to FALSE if the aggregated result needs to be
                 joined with another array. Default=FALSE.}
}
\details{
Group the \code{scidb}, or \code{scidbdf} array object \code{x} by dimensions
and/or attributes in the array.  applying the valid SciDB aggregation function
\code{FUN} expressed as a character string to the groups. Set eval to TRUE to
execute the aggregation and return a scidb object; set eval to FALSE to return
an unevaluated SciDB array promise, which is essentially a character string
describing the query that can be composed with other SciDB package functions.

If an R reduction function is speciied for \code{FUN}, it will be
transliterated to a SciDB aggregate.

The \code{by} argument must be a list of dimension names and/or attribute names
in the array \code{x} to group by, or a SciDB array reference object.  If
\code{by} is not specified and one of the \code{window} options is not
specified, then a grand aggregate is performed over all values in the array.

The argument \code{by} may be a list of dimension names and/or attributes of the
array \code{x}. Attributes that are not of type int64 will be `factorized` first
and replaced by enumerated int64 values that indicate each unique level (this
requires SciDB 13.6 or higher).

When \code{by} is a SciDB array it must contain one or more common dimensions
with \code{x}.  The two arrays will be joined (using SciDB
\code{cross_join(x,by)} and the resulting array will be grouped by the
attributes in the \code{by} array. This is similar to the usual R data.frame
aggregate method.

Perform moving window aggregates by specifying the optional \code{window} or
\code{variable_window} arguments. Use \code{window} to compute the aggregate
expression along a moving window specified along each coordinate axis as
\code{window=c(dimension_1_low, dim_1_high, dim_2_low,_dim_2_high, ...}.
Moving window aggregates along coordinates may be applied in multiple
dimensions.

Use \code{variable_window} to perform moving window aggregates over data
values in a single dimension specified by the \code{by} argument. See below
for examples. Moving window aggregates along data values are restricted
to a single array dimension.
}
\value{A \code{scidbdf} reference object. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
# Create a copy of the iris data frame in a 1-d SciDB array named "iris."
# Note that SciDB attribute names will be changed to conform to SciDB
# naming convention.
x <- as.scidb(iris,name="iris")

# Compute averages of each variable grouped by Species
a <- aggregate(x, by="Species", FUN=mean)

# Aggregation by an auxillary vector (which in this example comes from
# an R data frame)--also note any valid SciDB aggregation expression may
# be used:
y <- as.scidb(data.frame(sample(1:4,150,replace=TRUE)))
a <- aggregate(x, by=y, FUN="avg(Petal_Width) as apw, min(Sepal_Length) as msl")

# Use the window argument to perform moving window aggregates along coordinate
# systems. You need to supply a window across all the array dimesions.
set.seed(1)
A <- as.scidb(matrix(rnorm(20),nrow=5))
# Compute a moving window aggregate only along the rows summing two rows at
# a time (returning result to R). The notation (0,1,0,0) means apply the
# aggregate over the current row (0) and (1) following row, and just over
# the current column (that is, a window size of one).
aggregate(A,FUN="sum(val)",window=c(0,1,0,0))[]
# The above aggregate is equivalent to, for example:
apply(a,2,function(x) x+c(x[-1],0))

# Moving windows using the window= argument run along array coordinates.
# Moving windows using the variable_window= argument run along data values,
# skipping over empty array cells. The next example illustrates the
# difference.

# First, create an array with empty values:
B <- A>0
# Here is what B looks like:
B[]
# Now, run a moving window aggregate along the rows with window just like
# the above example:
aggregate(B,FUN="sum(val)",window=c(0,1,0,0))[]
# And now, a moving window along only the data values down the rows, note
# that we need to specify the dimension with by=:
aggregate(B,by="i",FUN="sum(val)",variable_window=c(0,1))[]
}
}
