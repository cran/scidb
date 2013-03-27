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
\alias{aggregate,scidbdf-method}

\title{Class \code{"scidbdf"}}
\description{A class that represents 1D SciDB arrays similarly to R data.frames.}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("scidbdf", ...)},
\code{scidb("ARRAY_NAME", ...)}, or \code{as.scidb(R_DATA_FRAME, ...)}.
}
\section{Slots}{
  \describe{
    \item{\code{call}:}{Object of class \code{"call"} How scidb was called. }
    \item{\code{name}:}{Object of class \code{"character"} scidb array name. }
    \item{\code{D}:}{Object of class \code{"list"} List of scidb dimension information. }
    \item{\code{dim}:}{Object of class \code{"numericOrNULL"} Vector of dimension lengths.}
    \item{\code{length}:}{Object of class \code{"numeric"} Length of array object. }
    \item{\code{attributes}:}{Object of class \code{"character"} Vector of all available attributes for the SciDB array. }
    \item{\code{nullable}:}{Object of class \code{"logical"} Is the attribute nullable (TRUE/FALSE)? }
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
    \item{aggregate}{\code{signature(x = "scidbdf")}: \code{aggregate(x, formula, FUN)}

Group the 1D SciDB array x by the indicated formula, applying the SciDB aggregate
expression FUN, specified as a character string, to the groups. The SciDB aggregate
expression must be named using the SciDB "as" keyword. This function will redimension
the SciDB array and apply the aggregate.  For example:
\code{A = as.scidb("iris"); g = aggregate(A, Petal_Length ~ Species, "avg(Petal_Length) as mean")}

}
    \item{show}{\code{signature(object = "scidbdf")}: ... }
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
