\name{chunk_map}
\alias{chunk_map}
\title{
chunk_map
}
\description{
Display a map of SciDB array partition counts
}
\usage{
chunk_map(array)
}
\arguments{
  \item{array}{(Optional character string)  a \code{scidb} array or \code{scidbdf} object or array name.}
}
\details{
Specify the optional array argument to get a more specific chunk map
list for a particular array and all its versios. If specified, \code{array}
must refer to a stored (named) SciDB array.
}
\value{
A data frame with a list of chunk counts and instance mapping for each
chunk of the specified array and all its versions,
or a summary of overall chunk map statistics
for all arrays if \code{array} is not specified.
}
\author{
A. Poliakov <apoliakov@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
library("scidb")
scidbconnect()

# Chunk map statistics for all arrays:
chunk_map()

# Chunk map statistics for a specific array:
}}
