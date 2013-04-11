\name{scidbconnect}
\alias{scidbconnect}
\title{Connect to a SciDB database.  }
\description{
Connect to a SciDB database.
}
\usage{
scidbconnect(host = "localhost", port = 8080L, username, password)
}
\arguments{
  \item{host}{
The host name or I.P. address of the SciDB database
instance to connect to (character).
}
  \item{port}{
The integer port number of the SciDB database simple HTTP service.
The default shim ports are 8080 (open) and 8083 (SSL encrypted).
}
  \item{username}{Optional authentication username (character).}
  \item{password}{Optional authentication password (character).}
}
\details{
The SciDB connection state is maintained internally to the \code{scidb}
package. We internalize state to facilitate operations involving \code{scidb}
objects.

Thus, only one open SciDB connection is supported at
a time.

One may connect to and use multiple SciDB databases by sequentially calling
\code{scidbconnect} between operations. Note that \code{scidb} objects are not
valid across different SciDB databases.

Use the optional \code{username} and \code{password} arguments to authenticate
the connection with the shim service. Authenticated connections require 
an encrypted connection with shim, available by default on port 8083.

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
