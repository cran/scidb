\name{bind}
\alias{bind}
\title{
bind
}
\description{
The \code{bind} function is a wrapper to the SciDB `apply` operator.
We chose the name `bind` as this function most closely matches the
concept of the R \code{cbind} function in data frame contexts.

Similarly to \code{cbind}, the \code{bind} function adds a new
variable (a.k.a. SciDB `attribute`) to a SciDB array.
}
\usage{
bind(X, name, FUN, eval)
}
\arguments{
  \item{X}{A scidb or scidbdf object}
  \item{name}{A character name for the new attribute}
  \item{FUN}{A SciDB function or expression that defines the values for the new attribute (character)}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\value{
A \code{scidbdf} object.
}
\note{
Use \code{eval=FALSE} to defer expression evaluation for efficiency.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- as.scidb(iris)
y <- bind(x, "prod", "Petal_Length * Petal_Width")

# The new scidbdf object y contains a new variable named 'prod':
print(head(y))
}}
