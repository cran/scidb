\name{scidblist}
\alias{scidblist}
\alias{scidbls}
\title{
List SciDB database items.
}
\description{
List arrays and various other items available in a SciDB database.
}
\usage{
scidblist(pattern, type = c("arrays", "operators", "functions", "types",
                            "aggregates", "instances","queries","libraries"),
          verbose = FALSE, n = Inf)
}
\arguments{
  \item{pattern}{
Filter the list by a regular-expression style pattern.
}
  \item{type}{
A character string indicating the type of SciDB item to list--defaults to 'array.'
}
  \item{verbose}{
Include extra available information for some items (arrays, in particular).
}
  \item{n}{
Maximum number of rows to return.
}
}
\value{
A data frame of listed items.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}

\examples{
\dontrun{
# List information about the SciDB cluster we're connected to:
scidblist('instances')

# List all arrays:
scidblist()

# List all arrays beginning with "A:"
scidblist(pattern="^A")
}}
