\name{scidb}
\alias{scidb}
\title{
Create a \code{scidb} reference object.
}
\description{
Create an array-like R object reference to a SciDB array.
}
\usage{
scidb(name, attribute = "", `data.frame`, gc)
}
\arguments{
  \item{name}{Name of the SciDB array to reference. }
  \item{attribute}{Name of an attribute within the array.}
  \item{data.frame}{Return a data.frame-like object (requires 1D SciDB array).}
  \item{gc}{TRUE means SciDB array shall be removed when R object is garbage collected or R exists. FALSE means SciDB array persists.}
}
\details{
The referenced array may be any SciDB array. One-dimensional SciDB arrays
may be references as data.frame-like objects in which the SciDB array
attributes appear as data.frame columns, or as one-column matrices.

SciDB arrays of dimension 2 or more appear as R arrays. In such cases, only one
attribute may be referenced per object. (The SciDB array may contain multiple
attributes.)

If the SciDB array contains more than one attribute, use the attribute argument
to specify which attribute to use in the reference object. The first listed
attribute is used by default if no attribute is specified.

The \code{scidb} class supports sparse and dense SciDB arrays of any dimension.
Attribute types real, integer (32-bit), logical, and single-character (one
byte) are supported. Integer and non-integer dimensions are supported,
with some limitations described below.

R does not have a native 64-bit integer type. SciDB uses 64-bit integer dimensions.
The \code{scidb} package uses R double-precision floating point integers to index
SciDB integer dimensions, restricting R to dimension values below 2^(53).

String-valued SciDB non-integer dimensions are also supported. Other types of
SciDB non-integer dimensions are not supported yet by the \code{scidb} package.

The \code{scidb} class generally follows SciDB indexing convention, which sometimes
differs from R indexing conventions. In particular,
note that the starting SciDB integer index is arbitrary, but often zero. (The
upper-left corner of R arrays is always indexed by [1,1,...].)  SciDB
integer dimension indices will be displayed as R dimension names. Subarray indexing
operations use the SciDB convention. Thus, zero and negative indices are
literally interpreted and passed to SciDB. In particular, negative indices do
not indicate index omission, unlike standard R arrays.

With the exception of the empty indexing operation, \code{[]}, subarray
indexing operations return new SciDB reference array objects.  Use the empty
indexing operation to materialize data from the SciDB backing array into a
normal R array.

The \code{scidb} class omits dimension indices with all empty cells in sparse SciDB
arrays when materializing data as R arrays with the \code{[]} operator.

Use the \code{\link{between}} function to efficiently index rectangular contiguous
subarrays.

Subsets of sparse SciDB arrays are bounded by the data extent and returned to R
as dense arrays with a default fill-in value in the empty cells. The default empty
value may be specified during indexing with the \code{default=} argument (see the
examples below), and gloablly using the package option
\code{options(scidb.default.value=NA)} (see \code{\link{scidb-package}}).
See the vignette examples for a more compete discussion of sparsity.
}
\value{
A \code{scidb} object that references the indicated
SciDB array.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
\code{\link{between}}
}

\examples{
\dontrun{
scidbconnect()
# A basic 1-d array, showing selection of attribute:
df2scidb(iris,nullable=FALSE)
A <- scidb("iris", attribute="Petal_Width")
dim(A)

# A sparse 3-d array example:
scidbremove(A, error=warning)
iquery("store(build_sparse(<val:double>[i=0:9,10,0,j=0:9,5,0,k=0:9,2,0],k,k<99 and (j=1 or j=3 or j=5 or j=7)),A)")
A <- scidb("A")

# Indexing operations return new SciDB arrays:
A[,2:3,5:8]

# But, their data can be materialized into an R array with []:
A[,2:3,5:8] []

# A sparse 2-d array.
scidbremove(c("A"), error=warning)
iquery("store(build_sparse(<val:double>[i=0:9,5,0,j=0:9,5,0],i,i=j),A)")
A <- scidb("A")


# The materialization process from sparse SciDB arrays to R uses a default
# fill-in value globally specified in options("scidb.default.value").  Or, use
# the 'default' argument to change the default:
scidbremove(c("A","B"),error=warning)
iquery("store(build_sparse(<x:double>[i=0:9,5,0,j=0:9,5,0],1,i=(j-1)),A)")
iquery("create array B<x:double>[a(string)=10,5,0,b(string)=10,5,0]")
iquery("redimension_store(apply(A,a,'a'+string(i),b,'b'+string(j)),B)")
B <- scidb("B")

B[]
B[,default=0]

}}
