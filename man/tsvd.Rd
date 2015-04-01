\name{tsvd}
\alias{tsvd}
\alias{tsvd,scidb-method}
\title{
tsvd
}
\description{
Compute a partial singular value decomposition or principal components
of a sparse or dense SciDB matrix.
}
\usage{
tsvd(x, nu, tol = 0.1, maxit = 20, tx, v, pca = FALSE)
}
\arguments{
  \item{x}{A scidb object representing a matrix.}
  \item{nu}{Optional number of singular values/vectors to compute. See notes below.}
  \item{tol}{(Optional) estimation error tolerance (see details).}
  \item{maxit}{(Optional) maximum number of iterations in the method.}
  \item{tx}{(Optional) pre-computed transpose of matrix x.}
  \item{v}{(Optional) iteration starting vector.}
  \item{pca}{Set to TRUE to compute principal components (singular vectors of the
             centered matrix).}
}
\value{
A list of three \code{scidb} objects corresponding to a matrix of left
singular vectors, a vector of singular values, and a matrix of transposed
right singular vectors, respectively.
}
\details{
The \code{tsvd} method uses Baglama and Reichel's efficient agumented
implicitly restarted Lanczos method for computing a truncated SVD of large
dense or sparse matrices. The method works best for the hard case of close to
square matrices. For matrices where the row and column dimensions are very
different (tall, skinny or short, fat matrices), consider the computationally
cheaper eigenvalue decomposition of the small matrix cross product instead.

The method is an iterative one.  The \code{tol} parameter sets the error
tolerance as defined in the paper and the irlba package vignette available
on CRAN. Smaller values yield more accurate estimations at the cost of more
iterations.

The method requires multiplication by the matrix \code{x} and its
transpose. If you already have the transpose matrix stored in SciDB, you
can optionally specify that in the \code{tx} argument.

The starting vector \code{v} defaults to a vector of ones. If this vector
is near the matrix null space then the algorithm may not converge. Specify
an alternate starting vector as required.

Set the \code{pca} argument to \code{TRUE} to indicate that you desire
principal components. The method will efficiently compute the singular
vectors of the centered matrix in that case, without explicitly forming
it to preserve sparsity.
}
\references{"Augmented Implicitly Restarted Lanczos Bidiagonalization Methods",
     J. Baglama and L. Reichel, SIAM J. Sci. Comput. 2005.}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- as.scidb(matrix(rnorm(500*500),500))
y <- tsvd(x)
print(y)
}}
