\name{scidbdf-class}
\Rdversion{1.1}
\docType{class}
\alias{scidbdf-class}
\alias{is.scidbdf,scidbdf-method}
\alias{is.scidbdf,ANY-method}
\alias{print,scidbdf-method}
\alias{head,scidbdf-method}
\alias{tail,scidbdf-method}
\alias{show,scidbdf-method}
\alias{sin,scidbdf-method}
\alias{abs,scidbdf-method}
\alias{cos,scidbdf-method}
\alias{tan,scidbdf-method}
\alias{asin,scidbdf-method}
\alias{acos,scidbdf-method}
\alias{atan,scidbdf-method}

\title{Class \code{"scidbdf"}}
\description{A class that represents 1D SciDB arrays similarly to R data.frames.}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("scidbdf", ...)},
\code{scidb("ARRAY_NAME", ...)}, or \code{as.scidb(R_DATA_FRAME, ...)}.
}
\section{Slots}{
  \describe{
    \item{\code{name}:}{Object of class \code{"character"} scidb array name. }
    \item{\code{D}:}{Object of class \code{"list"} List of scidb dimension information. }
    \item{\code{dim}:}{Object of class \code{"numericOrNULL"} Vector of dimension lengths.}
    \item{\code{length}:}{Object of class \code{"numeric"} Length of array object. }
    \item{\code{attributes}:}{Object of class \code{"character"} Vector of all available attributes for the SciDB array. }
    \item{\code{nullable}:}{Object of class \code{"logical"} Is the attribute nullable (TRUE/FALSE)? }
    \item{\code{schema}:}{Object of class \code{"character"} SciDB array schema.} 
    \item{\code{types}:}{Object of class \code{"character"} Vector of SciDB types for all the array attributes. }
    \item{\code{colClasses}:}{Object of class \code{"character"} Optional vector of read.table-style column classes used to parse messages from SciDB. }
    \item{\code{gc}:}{Object of class \code{"environment"} An environment used to link the SciDB array to the R garbage collector. }
    \item{\code{.S3Class}:}{Object of class \code{"character"} ~~ }
  }
}
\section{Methods}{
  \describe{
    \item{is.scidbdf}{\code{signature(x = "scidbdf")}: ... }
    \item{is.scidbdf}{\code{signature(x = "ANY")}: ... }
    \item{print}{\code{signature(x = "scidbdf")}: ... }
    \item{head}{\code{signature(x = "scidbdf")}: ... }
    \item{tail}{\code{signature(x = "scidbdf")}: ... }
    \item{show}{\code{signature(object = "scidbdf")}: ... }
    \item{sin}{\code{signature(x = "scidbdf")}: \code{sin(x)}
      Return an array with the sine of each element of the array \code{x}. The first listed attribute is used and should be numeric. The returned array contains a new attibute with "_sin" appended containing the result. Additional trig functions (\code{cos, tan, asin, acos, atan}) act similarly.
    }
    \item{abs}{\code{signature(x = "scidbdf")}: \code{cos(x)} See doc. for \code{sin}.}
    \item{cos}{\code{signature(x = "scidbdf")}: \code{cos(x)} See doc. for \code{sin}.}
    \item{tan}{\code{signature(x = "scidbdf")}: \code{tan(x)} See doc. for \code{sin}.}
    \item{asin}{\code{signature(x = "scidbdf")}: \code{asin(x)} See doc. for \code{sin}.}
    \item{acos}{\code{signature(x = "scidbdf")}: \code{acos(x)} See doc. for \code{sin}.}
    \item{atan}{\code{signature(x = "scidbdf")}: \code{atan(x)} See doc. for \code{sin}.}
   }
}
\section{Notes}{

Like the related \code{scidb} class, the \code{scidbdf} class represents
SciDB arrays as R objects. The \code{scidbdf} class presents 1-D SciDB arrays,
potentially with many SciDB attributes (variables), as a data.frame-like object.

}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\keyword{classes}
