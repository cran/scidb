\name{scidb_attributes}
\alias{scidb_attributes}
\alias{scidb_types}
\alias{scidb_nullable}
\alias{scidb_coordinate_bounds}
\alias{scidb_coordinate_overlap}
\alias{scidb_coordinate_chunksize}
\alias{scidb_coordinate_start}
\alias{scidb_coordinate_end}
\title{
Scidb schema utilities
}
\description{
Return a vector of SciDB array attribute names, types,
nullable-ness, coordinate bounds, overlap, and coordinate
start and end bounds.
}
\usage{
scidb_attributes(x)
scidb_types(x)
scidb_nullable(x)
scidb_coordinate_bounds(x)
scidb_coordinate_chunksize(x)
scidb_coordinate_overlap(x)
scidb_coordinate_start(x)
scidb_coordinate_end(x)
}
\arguments{
  \item{x}{An object of class \code{scidb} or \code{scidbdf}}
}
\value{
A character vector of SciDB array dimension names, types,
nullable-ness, coordinate bounds, chunk sizes, chunk overlap amount,
or coordinate starting or ending indices, respectively.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
# Upload data to create a 10x10 SciDB matrix
A <- as.scidb(matrix(rnorm(100),10))
scidb_attributes(A)
}}
