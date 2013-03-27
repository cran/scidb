\name{scidbconnect}
\alias{scidbconnect}
\title{Connect to a SciDB database.  }
\description{
Connect to a SciDB database.
}
\usage{
scidbconnect(host = "localhost", port = 8080L)
}
\arguments{
  \item{host}{
The host name or I.P. address of the SciDB database
instance to connect to (character).
}
  \item{port}{
The port number of the SciDB database simple HTTP service
to connect to (integer).
}
}
\details{
The SciDB connection state is maintained internally to
the \code{scidb} package. We internalize state to
facilitate operations involving \code{scidb} objects.

Thus, only one open SciDB connection is supported at
a time.

One may connect to and use multiple SciDB databases by
sequentially calling \code{scidbconnect} between
operations. Note that \code{scidb} objects are not
valid across different SciDB databases.
}
\note{
Disconnection is automatically handled by the package.
}
\value{
NULL is invisibly returned. SciDB connection errors
are propagated to R and may be handled with the usual
R error handling mechanisms.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
