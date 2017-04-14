[![CRAN version](http://www.r-pkg.org/badges/version/scidb)](https://cran.r-project.org/package=scidb)
![](http://cranlogs.r-pkg.org/badges/scidb)
[![codecov.io](https://codecov.io/github/Paradigm4/SciDBR/coverage.svg?branch=master)](https://codecov.io/github/Paradigm4/SciDBR?branch=master)

Install the package from CRAN with
```
install.packages("scidb")
```

The current development version of the package can be installed directly
from sources on  GitHub using the devtools package as follows (requires an R
development environment  and the R devtools package):
```
devtools::install_github("Paradigm4/SciDBR")  # development branch

# or

devtools::install_github("Paradigm4/SciDBR", ref="laboratory")  # experimental branch
```

The SciDB R package requires installation of a simple open-source HTTP network
service called on the computer that SciDB is installed on. This service only
needs to be installed on the SciDB machine, not on client computers that
connect to SciDB from R.  See http://github.com/paradigm4/shim  for source code
and installation instructions.

Developers please note that R CMD check-style unit tests are skipped unless a
system environment variable named SCIDB\_TEST\_HOST is set to the host name or
I.P. address of SciDB. See the tests directory for test code.

GitHub branch policy: the "devel" branch is for new ideas that have a good
chance of making it into the CRAN package, but all tests might not yet pass.
The "master" branch should pass all `R CMD check` tests against the current
R-devel version of R on all platforms, tested against the current SciDB release.


## Getting Started Documentation

https://Paradigm4.github.io/SciDBR


## Changes in package version 2.0.0

This is a major release that breaks API compatibility with previous package
releases.  Array objects have been removed. All SciDB arrays are now presented
as virtual data frames in R. This change was informed by the most common uses
we've seen.
