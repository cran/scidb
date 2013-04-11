# Nifty aggregation-related functions

`sweep_scidb` = function(x, MARGIN, STATS, FUN="-", `eval`=FALSE, `name`)
{
  if(!is.scidb(x)) stop("x must be a scidb object")
  if(!is.scidb(STATS) && !is.scidbdf(STATS)) stop("STATS must be a scidb or scidbdf object")
  if(length(MARGIN)!=1) stop("MARGIN must indicate a single dimension")
  if(length(STATS@D$name)>1) stop("STATS must be a one-dimensional SciDB array")
  if(is.numeric(MARGIN)) MARGIN = x@D$name[MARGIN]
  if(missing(`name`)) `name` = x@attribute
  if(!(MARGIN %in% STATS@D$name))
  {
# Make sure coordinate axis along MARGIN are named the same in each array
    old = sprintf("%s=",STATS@D$name[1])
    new = sprintf("%s=",MARGIN)
    schema = gsub(old,new,STATS@schema)
    query = sprintf("cast(%s,%s)",STATS,schema)
    STATS = .scidbeval(query,eval=FALSE, depend=list(x))
  }
# Check for potential attribute name conflicts and adjust.
  if(length(intersect(x@attributes, STATS@attributes))>0)
  {
    STATS = cast(STATS,paste(build_attr_schema(STATS,"V_"),build_dim_schema(STATS)),`eval`=FALSE)
  }
  if(nchar(FUN)==1)
  {
    FUN = sprintf("%s %s %s",x@attribute, FUN, STATS@attribute)
  }
  substitute(
  attribute_rename(
    project(
      bind(
        merge(x,STATS,by=MARGIN,eval=FALSE)
        ,"_sweep",FUN,eval=FALSE),"_sweep",eval=FALSE),
    "_sweep", `name`, eval=FALSE), eval=`eval`)
}

# A very limited version of R's apply.
`apply_scidb` = function(X,MARGIN,FUN,`eval`=FALSE,`name`,...)
{
  if(!is.scidb(X)) stop("X must be a scidb object")
  if(length(MARGIN)!=1) stop("MARGIN must indicate a single dimension")
  if(is.numeric(MARGIN)) MARGIN = X@D$name[MARGIN]
  if(missing(`name`)) `name` = X@attribute
  Y = aggregate(X,MARGIN,FUN,eval=FALSE)
  a = Y@attributes[length(Y@attributes)]
  attribute_rename(project(Y,a,eval=FALSE),a, `name`, eval=`eval`)
}

# x:   A scidb, scidbdf object
# by:  A character vector of dimension and or attribute names of x, or,
#      a scidb or scidbdf object that will be cross_joined to x and then
#      grouped by attribues of by.
# FUN: A SciDB aggregation expresion
`aggregate_scidb` = function(x,by,FUN,`eval`=FALSE,window,variable_window)
{
  unpack = FALSE
  if(missing(`by`))
  {
    `by` = x@D$name[1]
  }
  b = `by`
  if(is.list(b)) b = b[[1]]
  if(class(b) %in% c("scidb","scidbdf"))
  {
# We are grouping by attributes in another SciDB array `by`. We assume that
# x and by have conformable dimensions to join along!
    j = intersect(x@D$name, b@D$name)
    x = merge(x,b,by=list(j,j),eval=FALSE,depend=list(x,b))
    n = by@attributes
    by = list(n)
  }
# A bug up to SciDB 13.6 unpack prevents us from using eval=FALSE
  if(!eval && !compare_versions(options("scidb.version")[[1]],13.9)) stop("eval=FALSE not supported by aggregate due to a bug in SciDB <= 13.6")

  b = `by`
  if(length(b)>1) unpack = TRUE
  new_dim_name = make.names_(c(unlist(b),"row"))
  new_dim_name = new_dim_name[length(new_dim_name)]
  if(!all(b %in% c(x@attributes, x@D$name)))
  {
# Check for numerically-specified coordinate axes and replace with dimension
# labels.
    for(k in 1:length(b))
    {
      if(is.numeric(b[[k]]))
      {
        b[[k]] = x@D$name[b[[k]]]
      }
    }
  } 
  if(!all(b %in% c(x@attributes, x@D$name))) stop("Invalid attribute or dimension name in by")
  a = x@attributes %in% b
  query = x@name
# Handle group by attributes with redimension. We don't use a redimension
# aggregate, however, because some of the other group by variables may already
# be dimensions.
  if(any(a))
  {
# First, we check to see if any of the attributes are not int64. In such cases,
# we use index_lookup to create a factorized version of the attribute to group
# by in place of the original specified attribute. This creates a new virtual
# array x with additional attributes.
    types = x@attributes[a]
    nonint = x@types != "int64" & a
    if(any(nonint))
    {
# Use index_lookup to factorize non-integer indices, creating new enumerated
# attributes to sort by. It's probably not a great idea to have too many.
      unpack = TRUE
      idx = which(nonint)
      oldatr = x@attributes
      for(j in idx)
      {
        atr     = oldatr[j]
# Adjust the FUN expression to include the original attribute
        FUN = sprintf("%s, min(%s) as %s", FUN, atr, atr)
# Factorize atr
        x       = index_lookup(x,unique(sort(project(x,atr)),sort=FALSE),atr)
# Name the new attribute and sort by it instead of originally specified one.
        newname = paste(atr,"index",sep="_")
        newname = make.unique_(oldatr,newname)
        b[which(b==atr)] = newname
      }
    }

# Reset in case things changed above
    a = x@attributes %in% b
    n = x@attributes[a]
# XXX XXX XXX
# XXX What about chunk sizes? Also insert reasonable upper bound instead of *? XXX Take care of all these issues...
# XXX XXX XXX
    redim = paste(paste(n,"=0:*,1000,0",sep=""), collapse=",")
    D = paste(build_dim_schema(x,FALSE),redim,sep=",")
    A = x
    A@attributes = x@attributes[!a]
    A@nullable   = x@nullable[!a]
    A@types      = x@types[!a]
    S = build_attr_schema(A)
    D = sprintf("[%s]",D)
    query = sprintf("redimension(substitute(%s,build(<v:int64>[_i=0:0,1,0],-1),%s),%s%s)",x@name,paste(n,collapse=","),S,D)
  }
  along = paste(b,collapse=",")

# We use unpack to always return a data frame (a 1D scidb array). EXCEPT when
# aggregating along a single integer coordinate axis (not along attributes or
# multiple axes).
  if(!missing(window))
  {
    unpack = FALSE
    query = sprintf("window(%s, %s, %s)",query,paste(window,collapse=","),FUN)
  } else if(!missing(variable_window))
  {
    unpack = FALSE
    query = sprintf("variable_window(%s, %s, %s, %s)",query,along,paste(variable_window,collapse=","),FUN)
  } else
  query = sprintf("aggregate(%s, %s, %s)",query, FUN, along)
  if(unpack) query = sprintf("unpack(%s,%s)",query,new_dim_name)
  .scidbeval(query,eval,gc=TRUE,depend=list(x))
}
