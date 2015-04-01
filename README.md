Thanks for trying out the SciDB package for R. I hope you enjoy using it.

Install the package from CRAN with
```
install.packages("scidb")
```

The current development version of the package can be installed directly from
sources on  GitHub using the devtools package as follows (requires an R
development environment  and the R tools package):
```
library('devtools')
install_github("SciDBR","paradigm4",quick=TRUE)
```

Note! The SciDBR package depends on the RCurl R package, which in turn requires
support for the curl library in your operating system. This might mean that
you need to install a libcurl development library RPM or deb package on your
OS. On RHEL and CentOS, this package is usually called `libcurl-devel` and on
Ubuntu it's called `libcurl4-gnutls-dev`.


The SciDB R package requires installation of a simple open-source HTTP network
service called on the computer that SciDB is installed on. This service only
needs to be installed on the SciDB machine, not on client computers that
connect to SciDB from R.  See http://github.com/paradigm4/shim  for source code
and installation instructions.

Developers please note that R CMD check-style unit tests are skipped unless a
system environment variable named SCIDB_TEST_HOST is set to the host name or
I.P. address of SciDB. See the tests directory for test code.

Wiki
===
Check out (and feel free to contribute to) examples in the wiki pages for
this project here:

https://github.com/Paradigm4/SciDBR/wiki/_pages

This project also has a pretty web page on Github here:

https://Paradigm4.github.io/SciDBR

New features
===

## Database-related updates

* Support for SciDB version 14.3--also supports older SciDB releases, but not actively tested
* Dropping arrays from other R sessions requires a non-default option facilitating multiple-user settings.
* Queries can be canceled with R user interrupts now (for example with CTRL + C or ESC).

## More functions

* Support for Paradigm4 glm and a basic in-database model matrix builder
* Support for Paradigm4 truncated SVD routine
* Added hist, quantile, all.equal, antijoin, c (SciDB concat-like) function and many others


## Better handling of missing values

The array-like and dataframe-like classes now handle missing values in a
uniform way. All SciDB missing codes are mapped to R NA values and R NA values
are mapped to SciDB missing code zero.

## Windowed and moving-window aggregates

Multidimensional windowed and moving-window aggregates are now supported with
simple syntax in the `aggregate` function. Windows can be defined along
coordinate axes or number of (sparse) data values.

## Labeled coordinates

SciDB arrays now support labeled coordinate indices using the standard R
rownames, colnames, or dimnames settings. Assigned labels are provided by 1-d
SciDB arrays that map the integer coordinate to a string label. Here is a
simple example:

```
# Upload a test matrix to SciDB:
X <- as.scidb( matrix(rnorm(20),nrow=5) )

# Assign rownames to the SciDB matrix X.  SciDB matrix objects like X default
# to zero-based indexing.  It's important that the label array have the same
# starting index:
rownames(X) <- as.scidb( data.frame(letters[1:5]), start=0)

# We can now use strings to select subarrays and otherwise index X:
X[c("b","a","d"), ]
```

## More indexing goodness

Indexing SciDB array objects by other SciDB array objects to achieve the effect
of filtering by boolean expressions and similar operations is now supported.
Here is a simple example:

```
# Create a five-element SciDB vector:
y <- as.scidb(runif(5))

# Pick out rows of the SciDB matrix X fromt the last example that correspond
# to entries of y that are positive:
X[y > 0, ]
```

## SciDB array promises
Most functions return objects that represent array promises--unevaluated SciDB
query expressions with a result schema. Use the new `scidbeval` function or the
optional `eval` function argument when available to force evaluation to a
materialized SciDB backing array. Otherwise use the objects normally, deferring
evaluation until required.

## R Sparse matrix support
The package now supports double-precision valued R sparse matrices
defined via the `Matrix` package. Sparse SciDB matrices that are
materialized to R are returned as sparse R matrices and vice-versa.

## Heatmaps
The package overloads the standard R `image` function to plot heatmaps
of SciDB array objects (only applies to objects of class `scidb`).
```
library("devtools")
install_github("SciDBR","Paradigm4")
library("scidb")
scidbconnect()      # Fill in your SciDB hostname as required

# Create a SciDB array with some random entries
iquery("store(build(<v:double>[i=0:999,100,0,j=0:999,250,0],random()%100),A)")

# The SciDBR `image` function overloads the usual R image function to produce
# heatmaps using SciDB's `regrid` aggregation operator. The 'grid' argument
# specifies the output array size, and the 'op' argument specifies the
# aggregation operator to apply.

X = image(A, grid=c(100,100), op="avg(v)", useRaster=TRUE)
```
![Example output](https://raw.github.com/Paradigm4/SciDBR/master/inst/misc/image.jpg "Example output")

```
# Image accepts all the standard arguments to the R `image` function in
# addition to the SciDB-specific `grid` and `op` arguments. The output axes are
# labeled in the original array units. The scidb::image function returns the
# interpolated heatmap array:

dim(X)
[1] 100 100
```

## Aggregation, merge, apply, sweep, bind, and related functions
The package has a completely new implementation of aggregation, merge, and
related database functions. The new functions apply to SciDB array and data
frame-like objects. A still growing list of the functions includes:

* aggregate
* bind  (SciDB `apply` operator--generalizes R's `cbind`)
* index_lookup
* merge (SciDB `join` and `cross\_join` operators)
* project
* subset (SciDB `filter` operator)
* sort
* unique
* sweep
* apply (the R-style apply, not the SciDB AFL apply--see `bind` for that)
* cumulate
* cast, slice, repart, redimension, build (wrappers to SciDB operators)

See for example `help("subset", package="scidb")` for help on the `subset`
function, or any of the other functions.

Perhaps the coolest new feature associated with the functions listed above is
that they can be composed in a way that defers computation in SciDB to avoid
unnecessary creation of intermediate arrays. The new functions all accept an
argument named `eval` which, when set to FALSE, returns a new SciDB array
promise object in place of evaluating the query and returning an array or data
frame object.

The eval argument is automatically set to FALSE when any of the above functions
are directly composed in R, unless manually overriden by explicitly setting
`eval=TRUE`. Consider the following example:

```
x = as.scidb(iris)
head(x)
  Sepal_Length Sepal_Width Petal_Length Petal_Width Species
1          5.1         3.5          1.4         0.2  setosa
2          4.9         3.0          1.4         0.2  setosa
3          4.7         3.2          1.3         0.2  setosa
4          4.6         3.1          1.5         0.2  setosa
5          5.0         3.6          1.4         0.2  setosa
6          5.4         3.9          1.7         0.4  setosa

a = aggregate(
      project(
        bind(x,name="PxP", FUN="Petal_Length*Petal_Width"),
        attributes=c("PxP","Species")),
      by="Species", FUN="avg(PxP)")

a[]
  Species_index PxP_avg    Species
0             0  0.3656     setosa
1             1  5.7204 versicolor
2             2 11.2962  virginica
```
The composed `aggregate(project(bind(...` functions were carried out in
the above example within a single SciDB transaction, storing only the result
of the composed query.
