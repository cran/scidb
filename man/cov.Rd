\docType{methods}
\name{cov,scidb-method}
\alias{cov,scidb-method}
\title{Covariance matrix
This function is more limited than R's default \code{cov} function. It can
only compute a covariance matrix from a data matrix without any missing value
handling by the procedure (in R notation)
\code{S0 <- sweep(x, 2, colMeans(x))}, 
\code{crossprod(S0)/(nrow(S0) - 1)   # (covariance matrix result)}}
\usage{
\S4method{cov}{scidb}(x, y = NULL, use = "everything", method = c("pearson", "kendall", "spearman"))
}
\arguments{
\item{x}{a 2-d scidb array with a single numeric attribute}

\item{y}{UNUSED, limited to correlation matrix in the SciDB case}

\item{use}{UNUSED, limited to "everything" in the SciDB case}

\item{method}{UNUSED, limited to "pearson" in the SciDB case}
}
\value{
covariance matrix of \code{x} (as a SciDB array)
}
\description{
Covariance matrix
This function is more limited than R's default \code{cov} function. It can
only compute a covariance matrix from a data matrix without any missing value
handling by the procedure (in R notation)
\code{S0 <- sweep(x, 2, colMeans(x))}, 
\code{crossprod(S0)/(nrow(S0) - 1)   # (covariance matrix result)}
}

