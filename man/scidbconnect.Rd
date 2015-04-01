\name{scidbconnect}
\alias{scidbconnect}
\title{Connect to a SciDB database.  }
\description{
Connect to a SciDB database.
}
\usage{
scidbconnect(host=options("scidb.default_shim_host")[[1]],
             port=options("scidb.default_shim_port")[[1]],
             username, password,
             auth_type=c("pam","digest"),
             protocol=c("http","https"))
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
  \item{auth_type}{Optinal authentication method. The
\code{auth_type} parameter is only used when a \code{username} and
\code{password} are supplied. It defaults to 'pam'
which uses local Linux user names and passwords on the SciDB system.
Select 'digest' to use basic HTTP digest authentication. See the
\code{shim} documentation for details
\url{http://htmlpreview.github.io/?https://raw.github.com/Paradigm4/shim/master/wwwroot/help.html}.}
  \item{protocol}{Optional transport protocol, defaults to 'http.'
Select 'https' for TLS encryption. Note that the 'pam' authenitication
type automatically uses TLS encryption.}
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
the connection with the shim service. PAM-authenticated connections require 
an encrypted connection with shim, available by default on port 8083.

Use the optional \code{username} and \code{password} arguments with
\code{auth_type} set to "digest" to use HTTP digest authentication (see the
shim documentation to configur this).  Digest authentication may use either
"http" or "https" transports selected by the \code{protocol} setting.
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
