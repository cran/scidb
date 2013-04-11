\name{glm.fit}
\docType{methods}
\alias{glm.fit}
\alias{glm.fit,scidb-method}
\title{
Fitting generalized linear models
}
\description{
'glm.fit' is used to fit generalized linear models specified by a model
matrix and response vector.
}
\usage{
\S4method{glm.fit}{scidb}(x,y,weights=NULL,family=gaussian())
}
\arguments{
  \item{x}{a model matrix of dimension 'n * p'.}
  \item{y}{a response vector of length 'n'.}
  \item{weights}{an optional vector of 'prior weights' to be used in the
          fitting process.  Should be 'NULL' or a numeric vector.}
  \item{family}{a description of the error distribution and link function to
          be used in the model, supplied as the result of a call to
          a family function.}
}
\value{
A list of model output values:
\enumerate{
\item{\emph{coefficients}}{  model coefficient vector (SciDB array)}
\item{\emph{stderr}}{  vector of model coefficient standard errors (SciDB array)}
\item{\emph{tval}}{  vector of model coefficient t ratio values using estimated dispersion value (SciDB array)}
\item{\emph{pval}}{  vector of two-tailed p-values corresponding to the t ratio based on a Student t distribution. (It is possible that the dispersion is not known and there are no residual degrees of freedom from which to estimate it.  In that case the estimate is 'NaN'.)}
\item{\emph{aic}}{  a version of Akaike's \emph{An Information Criterion} value.}
\item{\emph{null.deviance}}{  the deviance for the null model, comparable with \code{deviance}.}
\item{\emph{res.deviance}}{  up to a constant, minus twice the maximized log-likelihood.}
\item{\emph{dispersion}}{  For binomial and Poison families the dispersion is
    fixed at one and the number of parameters is the number of
    coefficients. For gaussian, Gamma and inverse gaussian families the
    dispersion is estimated from the residual deviance, and the number
    of parameters is the number of coefficients plus one.  For a
    gaussian family the MLE of the dispersion is used so this is a valid
    value of AIC, but for Gamma and inverse gaussian families it is not. Other
    families set this value to \code{NA}}.
\item{\emph{df.null}}{  the residual degrees of freedom for the null model.}
\item{\emph{df.residual}}{  the residual degrees of freedom.}
\item{\emph{converged}}{  \code{FALSE} if the model did not converge.}
\item{\emph{totalObs}}{  total number of observations in the model.}
\item{\emph{nOK}}{  total number of observations corresponding to nonzero weights.}
\item{\emph{loglik}}{  converged model log-likelihood value.}
\item{\emph{rss}}{  residual sum of squares.}
\item{\emph{iter}}{  number of model iterations.}
\item{\emph{weights}}{  vector of weights used in the model (SciDB array).}
\item{\emph{family}}{  model family function.}
\item{\emph{y}}{  response vector (SciDB array).}
\item{\emph{x}}{  model matrix (SciDB array).}
}
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- as.scidb(matrix(rnorm(5000*20),nrow=5000))
y <- as.scidb(rnorm(5000))
M <- glm.fit(x, y)
print(coef(M))
}}
