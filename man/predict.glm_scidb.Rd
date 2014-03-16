\name{predict.glm_scidb}
\alias{predict.glm_scidb}
\title{
Prediction for SciDB GLM fits
}
\description{
Obtains predictions and optionally estimates standard errors of
those predictions from a fitted generalized linear model object.
}
\usage{
\method{predict}{glm_scidb}(object, ...)
}
\arguments{
  \item{object}{ a \code{glm_scidb} model object.}
  \item{...}{ optional arguments \code{newdata}, \code{type}, and \code{se.fit} (see details).}
}
\details{
Optional arguments:
\itemize{
  \item{\code{newdata}}{ a \code{scidbdf} SciDB data frame in which to look
          for variables with which to predict.  If omitted, the fitted
          linear predictors are used.}
  \item{\code{type}}{ the type of prediction required.  The default is on the scale
          of the linear predictors; the alternative "response" is on
          the scale of the response variable.  Thus for a default
          binomial model the default predictions are of log-odds
          (probabilities on logit scale) and \code{type = "response"} gives
          the predicted probabilities.}
  \item{\code{se.fit}}{ logical switch indicating if standard errors are required.}
}
}
\value{
If \code{se.fit = FALSE}, a vector of predictions.

If \code{se.fit = TRUE}, a list with components
\item{fit}{ Predictions, as for \code{se.fit = FALSE}.}
\item{se.fit}{ Estimated standard errors.}
\item{residual.scale}{ A scalar giving the square root of the dispersion used
          in computing the standard errors.}

}
\seealso{
  \code{\link{scidb}}
  \code{\link{glm_scidb}}
}
\author{
B. W. Lewis <blewis@paradigm4.com>, adapted from the R \code{predict.glm} documentation.
}
\examples{
\dontrun{
## example adpted from Venables and Ripley (2002, pp. 190-2.):

# In R:
ldose   <- rep(0:5, 2)
numdead <- c(1, 4, 9, 13, 18, 20, 0, 2, 6, 10, 12, 16)
sex     <- factor(rep(c("M", "F"), c(6, 6)))
data    <- data.frame(sex, ldose)
data    <- Reduce(rbind, 
             lapply(1:length(numdead),
               function(j) rbind(cbind(alive=1,data[j,])[rep(1,numdead[j]),],
                                 cbind(alive=0,data[j,])[rep(1,20-numdead[j]),])))
rownames(data) <- NULL

r_model <- glm( alive ~ sex + ldose - 1, family=binomial(), data=data)

# Now in SciDB:
data_scidb <- as.scidb(data)
str(data_scidb)
scidb_model <- glm_scidb( alive ~ sex + ldose - 1, family=binomial(), data=data_scidb)

# New data for prediction:
ld <- seq(0,5,0.1)
newdata <- as.scidb(data.frame(ldose=ld, sex=rep("M",length(ld))))
head(newdata)

pred_scidb = predict(scidb_model, newdata=newdata, type="response")
head(pred_scidb)

require("graphics")
plot(c(1,32), c(0,1), type = "n", xlab = "dose",
          ylab = "prob", log = "x")
text(2^ldose, numdead/20, as.character(sex))
lines(2^ld, pred_scidb[],lwd=2,col=4)
}
}
