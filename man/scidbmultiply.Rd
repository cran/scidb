\name{scidbmultiply}
\alias{scidbmultiply}
\title{
XXX  ~~function to do ... ~~
}
\description{
XXX  ~~ A concise (1-5 lines) description of what the function does. ~~
}
\usage{
scidbmultiply(e1, e2)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{e1}{
XXX     ~~Describe \code{e1} here~~
}
  \item{e2}{
XXX     ~~Describe \code{e2} here~~
}
}
\details{
XXX  ~~ If necessary, more details than the description above ~~
}
\value{
XXX  ~Describe the value returned
XXX  If it is a LIST, use
XXX  \item{comp1 }{Description of 'comp1'}
XXX  \item{comp2 }{Description of 'comp2'}
XXX ...
}
\references{
XXX ~put references to the literature/web site here ~
}
\author{
XXX  ~~who you are~~
}
\note{
XXX  ~~further notes~~
}

\seealso{
XXX ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function (e1, e2) 
{
    x = basename(tempfile(pattern = "array"))
    scidbquery(paste("store(multiply(", e1@name, ",", e2@name, 
        "),", x, ")", sep = ""))
    return(scidb(x))
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
