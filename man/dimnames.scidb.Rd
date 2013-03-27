\name{dimnames.scidb}
\alias{dimnames.scidb}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
dimnames
}
\description{
Return a list of dimension names. Warning! Huge SciDB arrays
may return enormous lists.
}
\usage{
\method{dimnames}{scidb}(x)
}
\arguments{
  \item{x}{ SciDB array reference object.  }
}
\details{
Don't use this function for huge SciDB arrays. Instead, iterate
through SciDB dimension values using iquery as shown in the examples.
}
\value{
A list of dimension names.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{iquery}}
}
\examples{
\dontrun{
# Build a two-dimensional array called 'yikes' with a non-integer dimension
scidbremove(c("cazart","yikes"),error=warning)
iquery("store(apply(build_sparse(<val:double>[i=0:9,5,1,j=0:20,2,0],random()/1000000000,i<j or j=20),a,format(i,'a%g')),cazart)")
iquery("create array yikes<val:double>[a(string)=*,5,1,j=0:20,2,0]")
iquery("redimension_store(cazart,yikes)")

yikes <- scidb("yikes")

dimnames(yikes)

# Here is an alternate approach that iterates through dimension names
# along one dimension for huge arrays:
i <- iquery("yikes:a", return=TRUE, iterative=TRUE)
nextElem(i)


}}
