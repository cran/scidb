\name{scidb-class}
\Rdversion{1.1}
\docType{class}
\alias{scidb-class}
\alias{\%*\%,scidb,scidb-method}
\alias{\%*\%,ANY,scidb-method}
\alias{\%*\%,scidb,ANY-method}

\alias{crossprod,scidb,ANY-method}
\alias{tcrossprod,scidb,ANY-method}
\alias{crossprod,ANY,scidb-method}
\alias{tcrossprod,ANY,scidb-method}

\alias{diag,scidb-method}
\alias{sum,scidb-method}

\alias{is.scidb,ANY-method}
\alias{print,scidb-method}
\alias{head,scidb-method}
\alias{tail,scidb-method}
\alias{show,scidb-method}
\alias{filter}
\alias{filter,scidb,character-method}
\alias{image}
\alias{image,scidb-method}
\alias{count}
\alias{count,scidb-method}

\title{Class \code{"scidb"}}
\description{A class that represents SciDB arrays as R arrays.}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("scidb", ...)},
\code{scidb("ARRAY_NAME", ...)}, or \code{as.scidb(R_MATRIX, ...)}.
}
\section{Slots}{
  \describe{
    \item{\code{call}:}{Object of class \code{"call"} How scidb was called. }
    \item{\code{name}:}{Object of class \code{"character"} scidb array name. }
    \item{\code{D}:}{Object of class \code{"list"} List of scidb dimension information. }
    \item{\code{dim}:}{Object of class \code{"numericOrNULL"} Vector of dimension lengths.}
    \item{\code{length}:}{Object of class \code{"numeric"} Length of array object. }
    \item{\code{attribute}:}{Object of class \code{"character"} The SciDB array attribute in use by scidb array reference object (only one attribute may be referenced at a time). }
    \item{\code{attributes}:}{Object of class \code{"character"} Vector of all available attributes for the SciDB array. }
    \item{\code{nullable}:}{Object of class \code{"logical"} Is the attribute nullable (TRUE/FALSE)? }
    \item{\code{type}:}{Object of class \code{"character"} SciDB type of the referenced attribute. }
    \item{\code{types}:}{Object of class \code{"character"} Vector of SciDB types for all the array attributes. }
    \item{\code{gc}:}{Object of class \code{"environment"} An environment used to link the SciDB array to the R garbage collector. }
    \item{\code{.S3Class}:}{Object of class \code{"character"} ~~ }
  }
}
\section{Methods}{
  \describe{
    \item{\%*\%}{\code{signature(x = "ANY", y = "scidb")}: ... }
    \item{\%*\%}{\code{signature(x = "scidb", y = "ANY")}: ... }
    \item{\%*\%}{\code{signature(x = "scidb", y = "scidb")}: ... }
    \item{crossprod}{\code{signature(x = "scidb", y = "ANY")}: ... }
    \item{tcrossprod}{\code{signature(x = "scidb", y = "ANY")}: ... }
    \item{crossprod}{\code{signature(x = "ANY", y = "scidb")}: ... }
    \item{tcrossprod}{\code{signature(x = "ANY", y = "scidb")}: ... }
    \item{is.scidb}{\code{signature(x = "ANY")}: ... }
    \item{print}{\code{signature(x = "scidb")}: ... }
    \item{head}{\code{signature(x = "scidb")}: ... }
    \item{filter}{\code{signature(x = "scidb", y = "character")}: ... }
    \item{count}{\code{signature(x = "scidb")}: ... }
    \item{tail}{\code{signature(x = "scidb")}: ... }
    \item{image}{\code{signature(x = "scidb")}: \code{image(x, grid=c(m,n), op="sum(attribute)")}
Display a heatmap-like image of the 2-d scidb array reference object \code{x}.
\code{grid(m,n)} specifies the repartitioned array block sizes and \code{op} is a valid SciDB aggregation function applied to the repartitioned chunks. }
    \item{diag}{\code{signature(x = "scidb")}: Return the diagonal of a SciDB matrix as a new 1d SciDB array (a vector).}
    \item{show}{\code{signature(object = "scidb")}: ... }
    \item{sum}{\code{signature(object = "scidb")}: Return the sum of the SciDB array elements. }
  }
}
\section{Notes}{

SciDB arrays are general n-dimensional sparse arrays with integer or
non-integer dimensions. The \code{scidb} class represents SciDB arrays
in a way that mimics standard R arrays in many ways.

Matrix arithmetic operations are overloaded for 2-D arrays.

}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\keyword{classes}
