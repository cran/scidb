\name{scidb_fisher.test}
\alias{scidb_fisher.test}
\title{
scidb_fisher.test
}
\description{
Performs Fisher's exact test for testing the null of independence
of rows and columns in a contingency table with fixed marginals.
}
\usage{
scidb_fisher.test(a,x="x",m="m",n="n",k="k",alternative="two.sided", `eval`=FALSE)
}
\arguments{
  \item{a}{ a \code{scidb} array or \code{scidbdf} data frame object.}
  \item{x}{The x value attribute name (see details below).}
  \item{m}{The m marginal value attribute name (see details below).}
  \item{n}{The m marginal value attribute name (see details below).}
  \item{k}{The m marginal value attribute name (see details below).}
  \item{alternative}{indicates the alternative hypothesis and must be one of
          '"two.sided"', '"greater"' or '"less"'.}
  \item{eval}{(Optional) If TRUE, execute the query and store the reult array. Otherwise defer evaluation.}
}
\details{
For 2 by 2 tables, the null of conditional independence is equivalent to the
hypothesis that the odds ratio equals one. "Exact" inference can be based on
observing that in general, given all marginal totals fixed, the first element
of the contingency table has a non-central hypergeometric distribution with
non-centrality parameter given by the odds ratio (Fisher, 1935).

Consider the following 2x2 contingency table:
\tabular{lccc}{
\tab Class I YES\tab Class I NO\tab SUM\cr
Class II YES\tab x\tab a\tab k = x + a\cr
Class II NO\tab b\tab c\tab \cr
SUM\tab m = x + b\tab n = a + c\tab \cr
}
The \code{x} input value specifies the name of the SciDB array attribute that
indicates the number of 'yes' events in both classifications.
The \code{m} input value specifies the name of the SciDB array attribute that
indicates the marginal sum of the first column.
The \code{n} input value specifies the name of the SciDB array attribute that
indicates the marginal sum of the second column.
The \code{k} input value specifies the name of the SciDB array attribute that
indicates the marginal sum of the first row.
}
\value{
A new SciDB array with two new attributes is returned (note that the returned attribute
names may be adjusted to account for naming conflicts with existing array attributes):
\item{pvalue}{the p-value of the test.}
\item{estimate}{an estimate of the odds ratio.  Note that the conditional
          Maximum Likelihood Estimate (MLE) rather than the
          unconditional MLE (the sample odds ratio) is used.}
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
  \code{\link{phyper}}
  \code{\link{qhyper}}
  \code{\link{dhyper}}
}
\examples{
\dontrun{
# Create a test array:
a <- scidb("apply(build(<x:int64>[i=0:0,1,0],2),m,12,n,18,k,17)")
scidb_fisher.test(a)[]

# output looks like:
#   x  m  n  k         pval   estimate
# 0 2 12 18 17 0.0005367241 0.04693664

}}
