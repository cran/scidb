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
\alias{sqrt,scidbdf-method}
\alias{exp,scidbdf-method}
\alias{cos,scidbdf-method}
\alias{tan,scidbdf-method}
\alias{asin,scidbdf-method}
\alias{acos,scidbdf-method}
\alias{atan,scidbdf-method}
\alias{Filter,character,scidbdf-method}
\alias{unpack,scidbdf-method}
\alias{regrid,scidbdf-method}
\alias{xgrid,scidbdf-method}

\title{Class \code{"scidbdf"}}
\description{A class that represents 1D SciDB arrays similarly to R data.frames.}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("scidbdf", ...)},
\code{scidb("ARRAY_NAME", ...)}, or \code{as.scidb(R_DATA_FRAME, ...)}.
}
\section{Slots}{
  \describe{
    \item{\code{name}:}{Object of class \code{"character"} scidb array name. }
    \item{\code{attributes}:}{Object of class \code{"character"} Vector of SciDB attribute names. }
    \item{\code{dimensions}:}{Object of class \code{"character"} Vector of SciDB dimension names. }
    \item{\code{schema}:}{Object of class \code{"character"} SciDB array schema.} 
    \item{\code{logical_plan}:}{Object of class \code{"character"} SciDB array plan. }
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
    \item{Filter}{\code{signature(f="character", x = "scidbdf")}: \code{Filter(f,x)}
      'Filter' masks elements of a vector for which a predicate (logical) SciDB expression 'f' returns false by making them empty, returning a sparse version of the array.
    }
    \item{sin}{\code{signature(x = "scidbdf")}: \code{sin(x)}
      Return an array with the sine of each element of the array \code{x}. The first listed attribute is used and should be numeric. The returned array contains a new attibute with "_sin" appended containing the result. Additional trig functions (\code{cos, tan, asin, acos, atan}) act similarly.
    }
    \item{abs}{\code{signature(x = "scidbdf")}: \code{abs(x)} See doc. for \code{sin}.}
    \item{sqrt}{\code{signature(x = "scidbdf")}: \code{sqrt(x)} See doc. for \code{sin}.}
    \item{exp}{\code{signature(x = "scidbdf")}: \code{exp(x)} See doc. for \code{sin}.}
    \item{cos}{\code{signature(x = "scidbdf")}: \code{cos(x)} See doc. for \code{sin}.}
    \item{tan}{\code{signature(x = "scidbdf")}: \code{tan(x)} See doc. for \code{sin}.}
    \item{asin}{\code{signature(x = "scidbdf")}: \code{asin(x)} See doc. for \code{sin}.}
    \item{acos}{\code{signature(x = "scidbdf")}: \code{acos(x)} See doc. for \code{sin}.}
    \item{atan}{\code{signature(x = "scidbdf")}: \code{atan(x)} See doc. for \code{sin}.}
    \item{unpack}{\code{unpack(x = "scidbdf")}: \code{unpack(x)}
      Return a 1-D representation of an array using the SciDB array unpack operator.
    }
    \item{regrid}{\code{signature(x = "scidbdf")}: \code{regrid(x, grid, expr)}
      Decimate the n-d SciDB array \code{x} by binning its coordinate system according to the \code{grid} argument and applying the aggregate expression on each bin. The \code{grid} argument must be the same length as the dimension of the array \code{x}. For example if \code{x} is a 10x10 matrix, then \code{regrid(x, c(2,1))} returns a 5x10 matrix replacing the rows with the average of values along every two rows.
    }
    \item{xgrid}{\code{signature(x = "scidbdf")}: \code{xgrid(x, grid=c(m,...))}
      Prolong the coordinate system of the n-d SciDB array \code{x} by replicating its values according to the \code{grid} parameter. In some cases, \code{xgrid} can be the inverse operation of \code{regrid}. For example if \code{x} is a 10x10 matrix, then \code{xgrid(x, c(2,1))} returns a 20x10 matrix, replicating values twice along the row dimension.
    }



   }
}
\section{Notes}{

Like the related \code{scidb} class, the \code{scidbdf} class represents
SciDB arrays as R objects. The \code{scidbdf} class presents 1-D SciDB arrays,
potentially with many SciDB attributes (variables), as a data.frame-like object.

Use the special empty-bracket index \code{[]} to return data from SciDB to R.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\keyword{classes}
