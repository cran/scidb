\name{between}
\alias{between}
\title{
between
}
\description{
Use \code{between} to select contiguous subarrays in indexing
operations. The \code{between} function would not normally be used directly
but rather inside bracket indexing operations as shown in the example.
This function is designed to support efficient indexing of contiguous
subarrays for arrays with non-integer dimensions.
}
\usage{
between(a,b)
}
\arguments{
  \item{a}{A SciDB array range bound (numeric or string in the case of non-integer dimension)}
  \item{b}{A SciDB array range bound (numeric or string in the case of non-integer dimension)}
}
\value{
A function that evaluates to a list of the specified bounds.
}
\note{
Between requires argument values that correspond to the array dimension
types (no dimension casting is performed).
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
# Build a two-dimensional array called 'yikes' with a non-integer dimension
scidbremove(c("cazart","yikes"),error=warning)
iquery("store(apply(build_sparse(<val:double>[i=0:9,5,1,j=0:20,2,0],random()/1000000000,i<j or j=20),a,format(i,'a%g')),cazart)")
iquery("create array yikes<val:double>[a(string)=*,5,1,j=0:20,2,0]")
iquery("redimension_store(cazart,yikes)")

yikes <- scidb("yikes")
yikes[between('a3','a8'),] []

}}
