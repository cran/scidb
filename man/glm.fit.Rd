\name{glm.fit}
\alias{glm.fit}
\alias{glm_scidb}
\alias{model_scidb}
\alias{glm.fit,scidb-method}
\alias{glm.fit,scidb,ANY,MNSN-method}
\title{
Fitting generalized linear models
}
\description{
\code{glm.fit} is used to fit generalized linear models specified by a model
matrix and response vector. \code{glm_scidb} is a simplified interface for
\code{scidbdf} objects similar (but much simpler than) \code{glm}.
}
\usage{
\S4method{glm.fit}{scidb}(x,y,weights=NULL,family=gaussian())
glm_scidb(formula, data, family=gaussian(), weights=NULL)
model_scidb(formula, data, factors=NULL)
}
\arguments{
  \item{x}{a model matrix of dimension 'n * p'.}
  \item{y}{a response vector of length 'n'.}
  \item{formula}{an object of class \code{formula} (or one that can be coerced to
          that class): a symbolic description of the model to be
          fitted. See details for limitations.}
  \item{data}{an object of class \code{scidbdf}.}
  \item{weights}{an optional vector of 'prior weights' to be used in the
          fitting process.  Should be 'NULL' or a numeric or scidb vector.}
  \item{family}{a description of the error distribution and link function to
          be used in the model, supplied as the result of a call to
          a family function.}
  \item{factors}{a list of factor encodings to use in the model matrix. See details.}
}
\value{
The \code{glm.fit} and \code{glm_scidb} functions return
a list of model output values described below. The \code{glm_scidb}
method uses an S3 class to additionally overload nice printing
and \code{summary} methods.
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
\item{\emph{factors}}{  a list of factor variable levels (SciDB arrays)
                        or NULL if no factors are present in the data.}
}

\code{model_scidb}  returns an output list with:
\itemize{
\item{\emph{formula}}{  the model forumual.}
\item{\emph{model}}{  the model matrix (SciDB array).}
\item{\emph{response}}{  the model response vector (SciDB array).}
\item{\emph{names}}{  an R character vector of variable names in the model matrix.}
\item{\emph{intercept}}{  a logical value; if TRUE the model includes an intercept term.}
\item{\emph{factors}}{  a list of factor variable levels (SciDB arrays)
                        or NULL if no factors are present in the data.}
}

}
\details{
The \code{glm_scidb} function works similarly to a limited version of
the usual \code{glm} function, but with a \code{scidbdf} data.frame-like
SciDB array instead of a standard data.frame.

Formulas in the \code{glm_scidb} function may only refer to variables present
in the \code{data} \code{scidbdf} object. And the indicated response must refer
to a single-column response term in the data (the two-column response form is
not accepted). Formulas may only list variables explicitly defined in the
\code{data}. That means that you should bind interaction and transformed terms
to your data before invoking the function.

Categorical (factor) variables in the data must be represented as strings. They
will be encoded as treatment-style contrast variables with the first listed
value set to the baseline value. No other automated contrast encodings are
available yet (you are free to build your own model matrix and use
\code{glm.fit} for that). All other variables will be coerced to
double-precision values.

Use the \code{model_scidb} function to build a model matrix from a formula and
a \code{scidbdf} data frame-like SciDB array.  The matrix is returned within an
output list as a sparse SciDB matrix of class \code{scidb} with character
string variables encoded as treatment contrasts as described above.
If you already have a list of factor-level codes for categorical variables
(for example from the output of \code{glm_scidb}, you can supply that in the
factor argument. See help for \code{predict} for an example.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
  \code{\link{predict.glm_scidb}}
}
\examples{
\dontrun{
# Using glm.fit
x <- as.scidb(matrix(rnorm(5000*20),nrow=5000))
y <- as.scidb(rnorm(5000))
M <- glm.fit(x, y)
coef(M)[]

# Using glm_scidb (similar to glm)
# From the 'glm' help:
## Dobson (1990) Page 93: Randomized Controlled Trial :
counts <- c(18,17,15,20,10,20,25,13,12)
outcome <- gl(3,1,9)
treatment <- gl(3,3)
d.AD <- data.frame(treatment, outcome, counts)
glm.D93 <- glm(counts ~ outcome + treatment, family = poisson(),data=d.AD)
summary(glm.D93)

# Compare with:
d.AD_sci = as.scidb(d.AD)
glm.D93_sci = glm_scidb(counts ~ outcome + treatment, family = poisson(), data=d.AD_sci)
summary(glm.D93_sci)
}}
