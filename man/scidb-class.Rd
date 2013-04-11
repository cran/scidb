\name{scidb-class}
\Rdversion{1.1}
\docType{class}
\alias{scidb-class}
\alias{\%*\%,scidb,scidb-method}
\alias{\%*\%,matrix,scidb-method}
\alias{\%*\%,scidb,matrix-method}
\alias{\%*\%,numeric,scidb-method}
\alias{\%*\%,scidb,numeric-method}
\alias{\%*\%,scidb,scidbdf-method}

\alias{crossprod,scidb,scidb-method}
\alias{tcrossprod,scidb,missing-method}
\alias{crossprod,scidb,missing-method}
\alias{tcrossprod,scidb,scidb-method}

\alias{diag,scidb-method}
\alias{apply,scidb-method}
\alias{sweep,scidb-method}
\alias{unpack,scidb-method}
\alias{t,scidb-method}
\alias{sin,scidb-method}
\alias{abs,scidb-method}
\alias{cos,scidb-method}
\alias{tan,scidb-method}
\alias{asin,scidb-method}
\alias{acos,scidb-method}
\alias{atan,scidb-method}
\alias{regrid}
\alias{regrid,scidb-method}
\alias{xgrid}
\alias{xgrid,scidb-method}

\alias{is.scidb,ANY-method}
\alias{print,scidb-method}
\alias{head,scidb-method}
\alias{tail,scidb-method}
\alias{show,scidb-method}
\alias{image}
\alias{image,scidb-method}

\title{Class \code{"scidb"}}
\description{A class that represents SciDB arrays as R arrays.}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("scidb", ...)},
\code{scidb("ARRAY_NAME", ...)}, or \code{as.scidb(R_MATRIX, ...)}.
}
\section{Slots}{
  \describe{
    \item{\code{name}:}{Object of class \code{"character"} scidb array name. }
    \item{\code{D}:}{Object of class \code{"list"} List of scidb dimension information. }
    \item{\code{dim}:}{Object of class \code{"numericOrNULL"} Vector of dimension lengths.}
    \item{\code{length}:}{Object of class \code{"numeric"} Length of array object. }
    \item{\code{attribute}:}{Object of class \code{"character"} The SciDB array attribute in use by scidb array reference object (only one attribute may be referenced at a time). }
    \item{\code{attributes}:}{Object of class \code{"character"} Vector of all available attributes for the SciDB array. }
    \item{\code{nullable}:}{Object of class \code{"logical"} Is the attribute nullable (TRUE/FALSE)? }
    \item{\code{schema}:}{Object of class \code{"character"} SciDB array schema. }
    \item{\code{type}:}{Object of class \code{"character"} SciDB type of the referenced attribute. }
    \item{\code{types}:}{Object of class \code{"character"} Vector of SciDB types for all the array attributes. }
    \item{\code{gc}:}{Object of class \code{"environment"} An environment used to link the SciDB array to the R garbage collector. }
    \item{\code{.S3Class}:}{Object of class \code{"character"} ~~ }
  }
}
\section{Methods}{
  \describe{
    \item{\%*\%}{\code{signature(x = "scidb", y = "scidb")}: ... }
    \item{\%*\%}{\code{signature(x = "scidb", y = "matrix")}: ... }
    \item{\%*\%}{\code{signature(x = "matrix", y = "scidb")}: ... }
    \item{\%*\%}{\code{signature(x = "scidb", y = "numeric")}: ... }
    \item{\%*\%}{\code{signature(x = "numeric", y = "scidb")}: ... }
    \item{\%*\%}{\code{signature(x = "scidb", y = "scidbdf")}: ... }
    \item{crossprod}{\code{signature(x = "scidb", y = "scidb")}: ... }
    \item{tcrossprod}{\code{signature(x = "scidb", y = "scidb")}: ... }
    \item{crossprod}{\code{signature(x = "scidb", y = "missing")}: ... }
    \item{tcrossprod}{\code{signature(x = "scidb", y = "missing")}: ... }
    \item{is.scidb}{\code{signature(x = "ANY")}: ... }
    \item{print}{\code{signature(x = "scidb")}: ... }
    \item{head}{\code{signature(x = "scidb")}: ... }
    \item{tail}{\code{signature(x = "scidb")}: ... }
    \item{apply}{\code{signature(x = "scidb")}: ... }
    \item{sweep}{\code{signature(x = "scidb")}: ... }
    \item{regrid}{\code{signature(x = "scidb")}: \code{regrid(x, grid=c(m,...), expr="avg(attribute)")}
      Decimate the n-d SciDB array \code{x} by binning its coordinate system according to the \code{grid} argument and applying the aggregate expression on each bin. The \code{grid} argument must be the same length as the dimension of the array \code{x}. For example if \code{x} is a 10x10 matrix, then \code{regrid(x, c(2,1))} returns a 5x10 matrix replacing the rows with the average of values along every two rows.
    }
    \item{xgrid}{\code{signature(x = "scidb")}: \code{xgrid(x, grid=c(m,...))}
      Prolong the coordinate system of the n-d SciDB array \code{x} by replicating its values according to the \code{grid} parameter. In some cases, \code{xgrid} can be the inverse operation of \code{regrid}. For example if \code{x} is a 10x10 matrix, then \code{xgrid(x, c(2,1))} returns a 20x10 matrix, replicating values twice along the row dimension.
    }
    \item{image}{\code{signature(x = "scidb")}: \code{image(x, grid=c(m,n), op="sum(attribute)")}
Display a heatmap-like image of the 2-d scidb array reference object \code{x}.
\code{grid(m,n)} specifies the repartitioned array block sizes and \code{op} is a valid SciDB aggregation function applied to the repartitioned chunks. }
    \item{diag}{\code{signature(x = "scidb")}: Return the diagonal of a SciDB matrix as a new 1d SciDB array (a vector).}
    \item{show}{\code{signature(object = "scidb")}: ... }
    \item{t}{\code{signature(x = "scidb")}: \code{t(x)}
      Return the transpose of the matrix \code{x}.
    }
    \item{unpack}{\code{unpack(x = "scidb")}: \code{unpack(x)}
      Return a 1-D representation of an array using the SciDB array unpack operator.
    }
    \item{diag}{\code{signature(x = "scidb")}: Return the diagonal of a SciDB matrix as a new 1d SciDB array (a vector).}
    \item{sin}{\code{signature(x = "scidb")}: \code{sin(x)}
      Return an array with the sine of each element of the array \code{x}. The active attribute is used and should be numeric. The returned array contains a new attibute with "_sin" appended containing the result. Other trig functions (\code{cos, tan, asin, acos, atan}) act similarly.
    }
    \item{abs}{\code{signature(x = "scidb")}: \code{cos(x)} See doc. for \code{sin}.}
    \item{cos}{\code{signature(x = "scidb")}: \code{cos(x)} See doc. for \code{sin}.}
    \item{tan}{\code{signature(x = "scidb")}: \code{tan(x)} See doc. for \code{sin}.}
    \item{asin}{\code{signature(x = "scidb")}: \code{asin(x)} See doc. for \code{sin}.}
    \item{acos}{\code{signature(x = "scidb")}: \code{acos(x)} See doc. for \code{sin}.}
    \item{atan}{\code{signature(x = "scidb")}: \code{atan(x)} See doc. for \code{sin}.}
  }
}
\section{Notes}{

SciDB arrays are general n-dimensional sparse arrays with integer dimensions.
The \code{scidb} class represents SciDB arrays in a way that mimics standard R
arrays in many ways. The \code{scidbdf} class represents one-dimensional SciDB
arrays with one or more attributes that mimic R data frames.

Matrix arithmetic operations are overloaded for 2-D numeric arrays.

}
\seealso{
\code{\link{scidb}}
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\keyword{classes}
