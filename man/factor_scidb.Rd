\name{factor_scidb}
\alias{factor_scidb}
\title{
Hybrid R/SciDB factors
}
\description{
Create R factor variables that include SciDB dimension array index values.
}
\usage{
factor_scidb(x, levels)
}
\arguments{
  \item{x}{
An R vector or factor vector.
}
  \item{levels}{
A SciDB dimension array. This can be an array created with the SciDB
\code{index_lookup} function, or any one-dimensional SciDB array. If the
array has more than one SciDB attribute, the first attribute will be used
to look up values from \code{x}.
}
}
\value{
A factor vector with extra class \code{scidb_factor}, and two additional
attributes: \code{scidb_levels} contains an R vector of looked-up SciDB index
values from the \code{levels} array, and \code{scidb_index} contains a
reference to the \code{levels} array.
}
\note{
\code{scidb_factor} values are treated specially when uploaded
from R to SciDB. Normal R factor values upload their contents. But \code{scidb_factor}
values upload their index values corresponding to their SciDB dimension array (as
type int64). Those values can then be directly joined with the SciDB dimension 
array, or any SciDB array using the same dimension index. See the examples below.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}

\examples{
\dontrun{
# Consider a SciDB dimension array of Iris flower species, perhaps with
# some additional data (made up in this example).
set.seed(1)
species <- data.frame(
  species=c("albicans","flavescens","germanica","setosa", "variegata", "versicolor", "virginica"),
  additional_data = runif(7))

species <- as.scidb(species, dimlabel="index")
str(species)

# Fisher's iris data example contain a subset of these species.
str(iris$Species)

# Let's index the iris data with the SciDB array.
iris$Species <- factor_scidb(iris$Species, species)

# The Species variable in the data frame now contains *two* indices, the usual
# R enumeration of levels, and a new set of levels corresponding to the SciDB
# lookup array. It also contains a reference to the lookup array.

# Let's upload the newly indexed iris data to SciDB. Observe that the 'Species'
# values are uploaded as SciDB int64 index values! Those indices are join-able
# with the dimension array used to create the factor.
x <- as.scidb(iris)
str(x)

# The advantage is that the x$Species values can be joined or redimensioned
# conformably with the SciDB indexing array.

# The next example computes the average iris data values grouped by Species
# using SciDB's redimension function:
xr <- redimension(x, dim="Species", FUN=mean)

# That output is join-able with the SciDB dimension array species:
merge(xr,species, by.x="Species", by.y="index")[]

# ...the output should look something like this...
#  Sepal_Length Sepal_Width Petal_Length Petal_Width    species additional_data
#4        5.006       3.428        1.462       0.246     setosa       0.9082078
#6        5.936       2.770        4.260       1.326 versicolor       0.8983897
#7        6.588       2.974        5.552       2.026  virginica       0.9446753

}}
