\name{str.scidb}
\alias{str.scidb}
\title{
Show the structure of a scidb object.
}
\description{
Print a summary of the dimension structure of a scidb array object.
}
\usage{
\method{str}{scidb}(object, ...)
}
\arguments{
  \item{object}{ a scidb array object.
}
  \item{\dots}{ additional objects for str (see \code{\link{str}}).
}
}
\value{
\code{str} does not return anything, for efficiency reasons.  The
obvious side effect is output to the terminal.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
