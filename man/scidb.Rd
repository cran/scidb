\name{scidb}
\alias{scidb}
\title{
Create a \code{scidb} reference object.
}
\description{
Create an array-like R object reference to a SciDB array.
}
\usage{
scidb(name, attribute, gc, `data.frame`)
}
\arguments{
  \item{name}{Name of the SciDB array to reference. }
  \item{attribute}{Name of an attribute within the array.}
  \item{gc}{TRUE means SciDB array shall be removed when R object is garbage collected or R exits. FALSE means SciDB array persists.}
  \item{data.frame}{Return a data.frame-like object (requires 1D SciDB array).}
}
\details{
The referenced array may be any SciDB array. One-dimensional SciDB arrays
may be represented as data.frame-like objects in which the SciDB array
attributes appear as data.frame columns. Alternatively, 1-d SciDB arrays
may be represented as vectors by setting \code{data.frame=FALSE}.

SciDB arrays of dimension 2 or more appear as R arrays. In such cases, one of
the SciDB attributes is used as the primary attribute for display and
arithmetic computation.  If the SciDB array contains more than one attribute,
use the attribute argument to specify which attribute to use in the reference
object. The first listed attribute is used by default if no attribute is
specified.

Data frame like representations use the \code{scidbdf} class.
N-d array objects and vectors use the \code{scidb} class.

The \code{scidb} class supports sparse and dense SciDB arrays of any dimension.
Attribute types real, integer (32-bit), logical, and single-character (one
byte) are directly supported and may be downloaded to R. Other SciDB attribute
types are indirectly supported.

R does not have a native 64-bit integer type. SciDB uses signed 62-bit integer
dimensions.  The \code{scidb} package uses R double-precision floating point
integers to index SciDB integer dimensions, restricting R to dimension values
below 2^(53).

With the exception of the empty indexing operation, \code{[]}, subarray
indexing operations return new SciDB reference array objects.  Use the empty
indexing operation to materialize data from the SciDB backing array into a
normal R array.

Sparse SciDB matrices (2-d arrays) are materialized to R as sparse matrices.
Higher dimensional sparse arrays are returned as lists of indices and values.
See the vignette examples for a more compete discussion of sparsity and various
indexing operations.
}
\section{Indexing}{
The \code{scidb} and \code{scidbdf} classes generally follow SciDB database
indexing convention, which exhibits some differences with standard R indexing.
In particular, note that the starting SciDB integer index is arbitrary, but
often zero. The upper-left corner of R arrays is always indexed by [1,1,...].
Subarray indexing operations use the SciDB convention. Thus, zero and negative
indices are literally interpreted and passed to SciDB. In particular, negative
indices do not indicate index omission, unlike standard R arrays.

Additional indexing notes:
\itemize{
\item Use empty brackets, \code{[]}, to materialize data back to R. Otherwise,
indexing operations produce new SciDB array objects.
\item Use numeric indices in any dimension in the units of the underlying SciDB
array coordinate system. Note that SciDB arrays generally are zero-indexed
and may even have negative indices.
\item Numeric indexing may include contiguous ranges or vectors of distinct
coordinate values, but repeated coordinate values in a single dimension are
not allowed. Examples of valid index ranges include
\code{[1:4, c(3,1,5), -10:15]}, but not \code{[c(1,3,1)]} for example.
\item The \code{scidbdf} class represents 1d SciDB arrays as data frame
objects with array attributes as columns. Use either positional numeric
or name-based indexing along columns, either with the dollar-sign notation
or string indexing. See examples.
\item The \code{scidb} class supports labeled dimension indexing using
R \code{rownames, colnames}, or \code{dimnames} settings. Labels assigned
in this way must be provided by 1-d SciDB arrays that map the integer
coordinates to character label values. See the examples.
\item The \code{scidb} class supports indexing by other SciDB arrays
to achieve the effect of filtering by boolean expressions and similar
operations, also illustrated below in the examples section.
\item Use the utility between function to avoid forming large sequences to
represent huge indexing ranges. For example, use \code{[between(1,1e9)]}
instead of \code{[1:1e9]}.
\item The \code{diag} function is supported for matrices and vectors.
}
Indexing by array to select a vector of cell entries is not yet supported.
}
\value{
A \code{scidb} object that references the indicated
SciDB array.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
\code{\link{between},\link{diag},\link{slice}}
}

\examples{
\dontrun{
scidbconnect()
# A 1-d array representation as a data frame:
data("iris")
A <- as.scidb(iris)
# Inspect the backing SciDB array object details with str:
str(A)

# Subsetting returns a new SciDB array:
A[1:2,]

# Materialize data to R with empty brackets:
A[1:2,][]

# Subset data frame-like object columns with 1-based positional or character
# indices. The following are all the same:
A[,"Species"]
A$Species
A[,5]

# Represent the 1-d array as a vector-like object instead:
a <- scidb(A, data.frame=FALSE)
# Interrogate the SciDB array properties with str:
str(a)

# A matrix:
set.seed(1)
X <- as.scidb( matrix(rnorm(20), nrow=5) )
# Diagonal entries of X:
diag(X)
# A sparse matrix with just the diagonal of X:
D = diag(diag(X))
# Materialize this sparse array to R:
D[]

# Produce a sparse matrix of filtered entries and materialize to R:
subset(X, "val > 0")[]

# Short-hand for the same effect:
(X > 0)[]

# Assign row labels to X. Note! We make sure that the index array starts at the
# same starting index as the matrix (zero in this example):
rownames(X) <- as.scidb(data.frame(letters[1:5]),start=0)
# Index by label:
X[c("c","a"), ]

# Filter X by an auxillary SciDB array condition (we use the rownames array),
# returning the result to R:
X[rownames(X) > "'b'", ][]


# A 3-d array:
X <- build(dim=c(3,2,2),names=c("x","i","j","k"),data="i+j+k")

# A sparse 3-d array filtered with subset:
Y <- subset(X, "x>1")
count(Y)
Y[]
}}
