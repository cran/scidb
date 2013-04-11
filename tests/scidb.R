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
# Upload dense matrix to SciDB
  set.seed(1)
  A = matrix(rnorm(50*40),50)
  B = matrix(rnorm(40*40),40)
  X = as.scidb(A, chunkSize=c(30,40))
  Y = as.scidb(B, chunkSize=c(17,20))
  check(X[],A)
  check(Y[],B)

# n-d array
  x = build("k",dim=c(3,3,3),names=c("x","i","j","k"))
  check(x[][,,2] , matrix(1,3,3))

# Dense matrix multiplication
  check(A%*%B, (X%*%Y)[])
# Transpose
  check(crossprod(A), (t(X)%*%X)[])
# Crossprod, tcrossprod
  check(crossprod(A), crossprod(X)[])
  check(tcrossprod(B), tcrossprod(B)[])
# Arithmetic on mixed R/SciDB objects
  x = rnorm(40);
  check((X%*%x)[,drop=FALSE], A%*%x)
# Scalar multiplication
  check(2*A, (2*X)[])
# Elementwise addition
  check(A+A, (X+X)[])
# SVD
  check(svd(A)$d, as.vector(svd(X)$d[]))

# Numeric array subsetting
  check((X %*% X[0,,drop=TRUE])[,drop=FALSE], A %*% A[1,])
  check(X[c(5,15,1),c(25,12,11)][], A[c(6,16,2),c(26,13,12)])
  check(as.vector(diag(Y)[]), diag(B))

# Filtering
  W = subset(X,"val>0")
# Sparse elementwise addition and scalar multiplication
  D = (W + 2*W)[]
  w = W[]
  w = w + 2*w
  check(sum(D-w), 0)

# Aggregation
  check( sweep(B,MARGIN=2,apply(B,2,mean)),
         sweep(Y,MARGIN=2,apply(Y,2,"avg(val)"))[])

# Join
  check(project(bind(merge(Y,diag(Y),by.x="i",by.y="i_1"),"v","val*val_1"),"v")[], diag(B)*B)

# Sparse, count
  S = Matrix::sparseMatrix(sample(100,200,replace=TRUE),sample(100,200,replace=TRUE),x=runif(200))
  Z = as.scidb(S)
  check(count(Z), nnzero(S))

# Misc
  z = atan(tan(abs(acos(cos(asin(sin(Z)))))))
  s = atan(tan(abs(acos(cos(asin(sin(S)))))))
  check(sum(s-z[]), 0)

# Labeled indices
  L = c(letters,LETTERS)
  i = as.scidb(data.frame(L[1:nrow(X)]), start=0)
  j = as.scidb(data.frame(L[1:ncol(X)]), start=0)
  rownames(X) = i
  colnames(X) = j
  rownames(A) = L[1:nrow(A)]
  colnames(A) = L[1:ncol(A)]
  check(X[c("F","v","f"),c("N","a","A")][], A[c("F","v","f"),c("N","a","A")])
}
gc()
