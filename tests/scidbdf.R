# scidb array object tests
# Set the SCIDB_TEST_HOST system environment variable to the hostname or IP
# address of SciDB to run these tests. If SCIDB_TEST_HOST is not set the tests
# are not run.

check = function(a,b)
{
  print(match.call())
  stopifnot(all.equal(a,b,check.attributes=FALSE))
}

library("scidb")
host = Sys.getenv("SCIDB_TEST_HOST")
if(nchar(host)>0)
{
  scidbconnect(host)
  options(scidb.debug=TRUE)

# Upload
  data("iris")
  x = as.scidb(iris)
# Factor levels are lost
  i = iris
  i[,5] = as.character(i[,5])
  check(x[],i)

# cast
  cast(x,x)

# Selection by name
  check(x[,"Petal_Length"][], iris[,"Petal.Length"])
  check(x$Petal_Length[], iris$Petal.Length)
# Selection along rows
  check(x[1:5,"Petal_Length"][], iris[1:5,"Petal.Length"])

# Unique of a single-attribute array
  u = unique(x$Species)
  check(count(u), 3)

# Aggregation by a non-integer attribute with a project thrown in
  check(aggregate(iris$Petal.Length,by=list(iris$Species),FUN=mean)[,2],
        aggregate(project(x,c('Petal_Length','Species')), by = 'Species', FUN='avg(Petal_Length)')[][,1])
}
gc()
