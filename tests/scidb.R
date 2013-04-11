# Basic unit tests for the scidb matrix object appear here.  The tests require
# a local SciDB database connection. If a connection is not available, tests
# are passed.

library("scidb")
OK = tryCatch(is.null(scidbconnect()), error=function(e) FALSE)

# expr must be a character string representing an expression that returns
# true upon success. If the expression throws an error, FALSE is returned.
test = function(expr)
{
  if(!OK) return(TRUE)  # SciDB is not available, pass.
  tryCatch(eval(parse(text=expr)), error=function(e) stop(e))
}


test("scidblist(); TRUE")
test("scidbremove('_RTEST',error=invisible);TRUE")
# Dense matrix tests
test("as.scidb(matrix(rnorm(50*40),50),rowChunkSize=3,colChunkSize=19,name='_RTEST');TRUE")
test("X=scidb('_RTEST');isTRUE(all.equal(0,sqrt(sum((crossprod(X)[] - crossprod(X[]))^2))))")
test("X=scidb('_RTEST');x=rnorm(50);isTRUE(all.equal(0,sqrt(sum((crossprod(x,X)[] - crossprod(x,X[]))^2))))")
test("scidbremove('_RTEST',error=invisible);TRUE")


# Please write more tests following this pattern...
