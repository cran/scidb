# cf glm.fit
glm_scidb = function(x,y,weights=NULL,family=gaussian())
{
  got_glm = length(grep("glm",.scidbenv$ops[,2]))>0
  if(is.null(weights))
  {
    weights = build(1.0,nrow(x),start=x@D$start[1],chunksize=x@D$chunk_interval[1])
  }
  if(!is.scidb(weights))
  {
    weights = as.scidb(weights,chunkSize=x@D$chunk_interval[1])
  }
  if(!is.scidb(y))
  {
    y = as.scidb(y)
  }
  if(!got_glm)
  {
    stop("The Paradigm4 glm operator was not found.")
  }
  dist = family$family
  link = family$link
# GLM has a some data partitioning requirements to look out for:
  if(x@D$chunk_interval[2]<dim(x)[2])
  {
    x = repart(x,chunk=c(x@D$chunk_interval[1],dim(x)[2]))
  }
  if((y@D$chunk_interval[1] != x@D$chunk_interval[1]) )
  {
    y = repart(y, chunk=x@D$chunk_interval[1])
  }
  query = sprintf("glm(%s,%s,%s,'%s','%s')",
           x@name, y@name, weights@name, dist, link)
  M = .scidbeval(query,eval=TRUE,gc=TRUE)
  m1 = M[,0][] # Cache 1st column
  ans = list(
    coefficients = M[0,],
    stderr = M[1,],
    tval = M[2,],
    pval = M[3,],
    aic = m1[12],
    null.deviance = m1[13],
    res.deviance = m1[15],
    dispersion = m1[5],
    df.null = m1[6],
    df.residual = m1[7],
    converged = m1[10]==1,
    totalObs = m1[8],
    nOK = m1[9],
    loglik = m1[14],
    rss = m1[16],
    iter = m1[18],
    weights = weights,
    family = family,
    y = y,
    x = x
  )
  ans
}
