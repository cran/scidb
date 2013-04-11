#/*
#**
#* BEGIN_COPYRIGHT
#*
#* This file is part of SciDB.
#* Copyright (C) 2008-2013 SciDB, Inc.
#*
#* SciDB is free software: you can redistribute it and/or modify
#* it under the terms of the AFFERO GNU General Public License as published by
#* the Free Software Foundation.
#*
#* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
#* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
#* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
#* the AFFERO GNU General Public License for the complete license terms.
#*
#* You should have received a copy of the AFFERO GNU General Public License
#* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#*
#* END_COPYRIGHT
#*/

# This file contains general utility routines including most of the shim
# network interface.

# An environment to hold connection state
.scidbenv = new.env()

# Force evaluation of an expression that yields a scidb or scidbdf object,
# storing result to a SciDB array when eval=TRUE.
# name: (character) optional SciDB array name to store to
# gc: (logical) optional, when TRUE tie result to R garbage collector
scidbeval = function(expr, eval=TRUE, name, gc=TRUE)
{
  ans = eval(expr)
  if(!(inherits(ans,"scidb") || inherits(ans,"scidbdf"))) return(ans)
  .scidbeval(ans@name, `eval`=eval, name=name, gc=gc)
}


# Create a new scidb reference to an existing SciDB array.
# name (character): Name of the backing SciDB array
# attribute (character): Attribute in the backing SciDB array
#   (applies to n-d arrays, not data.frame-like 1-d arrays)
# gc (logical, optional): Remove backing SciDB array when R object is
#     garbage collected? Default is FALSE.
# data.frame (logical, optional): If true, return a data.frame-like object.
#   Otherwise an array.
`scidb` = function(name, attribute, gc, `data.frame`)
{
  if(missing(name)) stop("array or expression must be specified")
  if(missing(gc)) gc=FALSE
  if(is.scidb(name) || is.scidbdf(name))
  {
    return(.scidbeval(name@name, eval=FALSE, attribute=attribute, gc=gc, `data.frame`=`data.frame`, depend=list(name)))
  }
  query = sprintf("show('%s as array','afl')",gsub("'","\\\\'",name,perl=TRUE))
  schema = iquery(query,`return`=1)$schema
  obj = scidb_from_schemastring(schema, name, `data.frame`)
  if(!missing(attribute))
  {
    if(!(attribute %in% obj@attributes)) warning("Requested attribute not found")
    obj@attribute = attribute
  }
  if(gc)
  {
    obj@gc$name = name
    obj@gc$remove = TRUE
    reg.finalizer(obj@gc, function(e) if (e$remove) 
        tryCatch(scidbremove(e$name,async=TRUE), error = function(e) invisible()), 
            onexit = TRUE)
  } else obj@gc = new.env()
  obj
}

# An important internal convenience function that returns a scidb object.  If
# eval=TRUE, a new SciDB array is created the returned scidb object refers to
# that.  Otherwise, the returned scidb object represents a SciDB array promise.
#
# INPUT
# expr: (character) A SciDB expression or array name
# eval: (logical) If TRUE evaluate expression and assign to new SciDB array.
#                 If FALSE, infer output schema but don't evaluate.
# name: (optional character) If supplied, name for stored array when eval=TRUE
# gc: (optional logical) If TRUE, tie SciDB object to  garbage collector.
# depend: (optional list) An optional list of other scidb or scidbdf objects
#         that this expression depends on (preventing their garbage collection
#         if other references to them go away).
# attribute: (optional character) Set the attribute on a scidb object,
#             requires data.frame=TRUE
# data.frame: (optional, logical) If TRUE, return a data.frame object, false
#             return a scidb object. Default is missing, in which case an
#             automatic decision is made about the object return class.
#
# OUTPUT
# A `scidb` or `scidbdf` array object.
#
# NOTE
# AFL only in SciDB expressions--AQL is not supported.
`.scidbeval` = function(expr,eval,name,gc=TRUE, depend, attribute, `data.frame`)
{
  ans = c()
  if(missing(depend)) depend=c()
  if(!is.list(depend)) depend=list(depend)
  if(`eval`)
  {
    if(missing(name)) newarray = tmpnam()
    else newarray = name
    query = sprintf("store(%s,%s)",expr,newarray)
    scidbquery(query)
    ans = scidb(newarray,gc=gc,attribute=attribute,`data.frame`=`data.frame`)
  } else
  {
    ans = scidb(expr,gc=gc,attribute=attribute,`data.frame`=`data.frame`)
# Assign dependencies
    if(length(depend)>0)
    {
      assign("depend",depend,envir=ans@gc)
    }
  }

  ans
}


# Construct a scidb promise from a SciDB schema string.
scidb_from_schemastring = function(s,expr=character(), `data.frame`)
{
  a=strsplit(strsplit(strsplit(strsplit(s,">")[[1]][1],"<")[[1]][2],",")[[1]],":")
  attributes=unlist(lapply(a,function(x)x[[1]]))
  attribute=attributes[[1]]

  ts = lapply(a,function(x)x[[2]])
  nullable = rep(FALSE,length(ts))
  n = grep("null",ts,ignore.case=TRUE)
  if(any(n)) nullable[n]=TRUE

  types = gsub(" .*","",ts)
  type = types[1]

  d = gsub("\\]","",strsplit(s,"\\[")[[1]][[2]])
  d = strsplit(strsplit(d,"=")[[1]],",")
  dname = unlist(lapply(d[-length(d)],function(x)x[[length(x)]]))
  dtype = rep("int64",length(dname))
  chunk_interval = as.numeric(unlist(lapply(d[-1],function(x)x[[2]])))
  chunk_overlap = as.numeric(unlist(lapply(d[-1],function(x)x[[3]])))
  d = lapply(d[-1],function(x)x[[1]])

  dlength = unlist(lapply(d,function(x)diff(as.numeric(gsub("\\*",.scidb_DIM_MAX,strsplit(x,":")[[1]])))+1))
  dstart = unlist(lapply(d,function (x)as.numeric(strsplit(x,":")[[1]][[1]])))

  D = list(name=dname,
           type=dtype,
           start=dstart,
           length=dlength,
           chunk_interval=chunk_interval,
           chunk_overlap=chunk_overlap,
           low=rep(NA,length(dname)),
           high=rep(NA,length(dname))
           )
  if(missing(`data.frame`)) `data.frame` = ( (length(dname)==1) &&  (length(attributes)>1))
  if(length(dname)>1 && `data.frame`) stop("SciDB data frame objects can only be associated with 1-D SciDB arrays")

  if(`data.frame`)
  {
# Set default column types
    ctypes = c("int64",dtype)
    cc = rep(NA,length(ctypes))
    cc[ctypes=="datetime"] = "Date"
    cc[ctypes=="float"] = "double"
    cc[ctypes=="double"] = "double"
    cc[ctypes=="bool"] = "logical"
    st = grep("string",ctypes)
    if(length(st>0)) cc[st] = "character"
    return(new("scidbdf",
                schema=s,
                name=expr,
                attributes=attributes,
                types=types,
                nullable=nullable,
                D=D,
                dim=c(D$length,length(attributes)),
                colClasses=cc,
                gc=new.env(),
                length=length(attributes)
             ))
  }

  new("scidb",
      name=expr,
      schema=s,
      attribute=attribute,
      type=type,
      attributes=attributes,
      types=types,
      nullable=nullable,
      D=D,
      dim=D$length,
      gc=new.env(),
      length=prod(D$length)
  )
}

# store the connection information and obtain a unique ID
scidbconnect = function(host='localhost', port=8080L, username, password)
{
  scidbdisconnect()
  assign("host",host, envir=.scidbenv)
  assign("port",port, envir=.scidbenv)
  if(missing(username)) username=c()
  if(missing(password)) password=c()
# Check for login
  if(!is.null(username))
  {
    auth = GET(resource="login",list(username=username, password=password),header=FALSE)
    if(nchar(auth)<1) stop("Authentication error")
    assign("auth",auth,envir=.scidbenv)
    assign("authenv",new.env(),envir=.scidbenv)
    reg.finalizer(.scidbenv$authenv, function(e) scidblogout(), onexit=TRUE)
  }

# Use the query ID from a query as a unique ID for automated
# array name generation.
  x = scidbquery(query="setopt('precision','16')",release=1,resp=TRUE)
  id = strsplit(x$response, split="\\r\\n")[[1]]
  id = id[[length(id)]]
  assign("uid",id,envir=.scidbenv)
# Try to load the dense_linear_algebra library
  tryCatch(
    scidbquery(query="load_library('dense_linear_algebra')",
               release=1,resp=FALSE),
    error=invisible)
# Try to load the example_udos library (>= SciDB 13.6)
  tryCatch(
    scidbquery(query="load_library('example_udos')",release=1,resp=FALSE),
    error=invisible)
# Try to load the P4 library
  tryCatch(
    scidbquery(query="load_library('linear_algebra')",release=1,resp=FALSE),
    error=invisible)
# Save available operators
  assign("ops",iquery("list('operators')",return=TRUE),envir=.scidbenv)
# Update the scidb.version option
  v = scidbls(type="libraries")[1,]
  if("major" %in% names(v))
  {
    options(scidb.version=paste(v$major,v$minor,sep="."))
  }
  invisible()
}

scidblogout = function()
{
  if(exists("auth",envir=.scidbenv) && exists("host",envir=.scidbenv))
  {
    GET(resource="logout",header=FALSE)
  }
}

scidbdisconnect = function()
{
  if(exists("authenv",envir=.scidbenv)) rm("authenv",envir=.scidbenv)
  gc()
  if(exists("auth",envir=.scidbenv))  rm("auth",envir=.scidbenv)
  if(exists("host",envir=.scidbenv)) rm("host", envir=.scidbenv)
  if(exists("port",envir=.scidbenv)) rm("port", envir=.scidbenv)
}

make.names_ = function(x)
{
  gsub("\\.","_",make.names(x, unique=TRUE),perl=TRUE)
}

# x is vector of existing values
# y is vector of new values
# returns a set the same size as y with non-conflicting value names
make.unique_ = function(x,y)
{
  x = make.names(x,unique=TRUE)
  z = make.names(c(x,y),unique=TRUE)
  gsub("\\.","_",setdiff(union(x,z),x))
}

# Make a name from a prefix and a unique SciDB identifier.
tmpnam = function(prefix="R_array")
{
  salt = basename(tempfile(pattern=prefix))
  if(!exists("uid",envir=.scidbenv)) stop("Not connected...try scidbconnect")
  paste(salt,get("uid",envir=.scidbenv),sep="")
}

# Return a shim session ID or error
# This will also return an authenticaion token string if one is available.
getSession = function()
{
  session = GET("/new_session",header=FALSE)
  if(length(session)<1) stop("SciDB http session error; are you connecting to a valid SciDB host?")
  session = gsub("\r","",session)
  session = gsub("\n","",session)
  session
}

# Supply the base SciDB URI from the global host, port and auth
# parameters stored in the .scidbenv package environment.
# Every function that needs to talk to the shim interface should use
# this function to supply the URI.
# Arguments:
# resource (string): A URI identifying the requested service
# args (list): A list of named query parameters
URI = function(resource="", args=list())
{
  if(!exists("host",envir=.scidbenv)) stop("Not connected...try scidbconnect")
  if(exists("auth",envir=.scidbenv))
    args = c(args,list(auth=get("auth",envir=.scidbenv)))
  prot = "http://"
  if("username" %in% names(args) || "auth" %in% names(args)) prot = "https://"
  ans  = paste(prot, get("host",envir=.scidbenv),":",get("port",envir=.scidbenv),sep="")
  ans = paste(ans, resource, sep="/")
  if(length(args)>0)
    ans = paste(ans,paste(paste(names(args),args,sep="="),collapse="&"),sep="?")
  ans
}

# Send an HTTP GET message
GET = function(resource, args=list(), header=TRUE, async=FALSE)
{
  if(!(substr(resource,1,1)=="/")) resource = paste("/",resource,sep="")
  uri = URI(resource, args)
  uri = URLencode(uri)
  uri = gsub("\\+","%2B",uri,perl=TRUE)
  if(async)
  {
    getURI(url=uri,.opts=list(header=header,'ssl.verifypeer'=0),async=TRUE)
    return(NULL)
  }
  getURI(url=uri, .opts=list(header=header,'ssl.verifypeer'=0))
}


# Check if array exists
.scidbexists = function(name)
{
  Q = scidblist()
  return(name %in% Q)
}

# scidblist: A convenience wrapper for list().
# Input:
# type (character), one of the indicated list types
# verbose (boolean), include attribute and dimension data when type="arrays"
# n: maximum lines of output to return
# Output:
# A list.
scidblist = function(pattern,
type= c("arrays","operators","functions","types","aggregates","instances","queries","libraries"),
              verbose=FALSE, n=Inf)
{
  type = match.arg(type)
  Q = iquery(paste("list('",type,"')",sep=""), return=TRUE, n=n)

  if(dim(Q)[1]==0) return(NULL)
  z=Q[,-1,drop=FALSE]
  if(type=="arrays" && !verbose) {
    z=z[,1]
    if(!missing(pattern))
      z = grep(z,pattern=pattern,value=TRUE)
  }
  if(type=="arrays" && verbose) z=z[,3]
  z
}
scidbls = function(...) scidblist(...)

# Basic low-level query. Returns query id. This is an internal function.
# query: a character query string
# afl: TRUE indicates use AFL, FALSE AQL
# async: TRUE=Ignore return value and return immediately, FALSE=wait for return
# save: Save format query string or NULL. If async=TRUE, save is ignored.
# release: Set to zero preserve web session until manually calling release_session
# session: if you already have a SciDB http session, set this to it, otherwise NULL
# resp(logical): return http response
# Example values of save:
# save="dcsv"
# save="lcsv+"
# save="(double NULL, int32)"
#
# Returns the HTTP session in each case
scidbquery = function(query, afl=TRUE, async=FALSE, save=NULL, release=1, session=NULL, resp=FALSE)
{
  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE==options("scidb.debug")[[1]]) DEBUG=TRUE
  sessionid=session
  if(is.null(session))
  {
# Obtain a session from shim
    sessionid = getSession()
  }
  if(is.null(save)) save=""
  if(DEBUG)
  {
    cat(query, "\n")
    t1=proc.time()
  }
  if(async)
  {
    ans =tryCatch(
      GET("/execute_query",list(id=sessionid,release=release,query=query,async='1',afl=as.integer(afl)),async=TRUE),
      error=function(e) {
        GET("release_session", list(id=sessionid))
        stop("HTTP/1.0 500 ERROR")
      })
  } else
  {
    ans = tryCatch(
      {
      if(is.null(save))
        GET("/execute_query",list(id=sessionid,release=release,query=query,afl=as.integer(afl)))
      else
        GET("/execute_query",list(id=sessionid,release=release,save=save,query=query,afl=as.integer(afl)))
      },
      error=function(e) {
        GET("/release_session",list(id=sessionid))
        "HTTP/1.0 500 ERROR"
      })
    w = ans
    err = 200
    if(nchar(w)>9)
      err = as.integer(strsplit(substr(w,1,20)," ")[[1]][[2]])
    if(err>399)
    {
      w = paste(strsplit(w,"\r\n\r\n")[[1]][-1],collapse="\n")
      stop(w)
    }
  }
  if(DEBUG) print(proc.time()-t1)
  if(resp) return(list(session=sessionid, response=ans))
  sessionid
}

# scidbremove: Convenience function to remove one or more scidb arrays
# Input:
# x (character): a vector or single character string listing array names
# error (function): error handler. Use stop or warn, for example.
# async (optional boolean): If TRUE use expermental shim async option for speed
# Output:
# null
scidbremove = function(x, error=warning, async)
{
  if(is.null(x)) return(invisible())
  if(missing(async)) async=FALSE
  if(inherits(x,"scidb")) x = x@name
  if(!inherits(x,"character")) stop("Invalid argument. Perhaps you meant to quote the variable name(s)?")
  for(y in x) {
    if(grepl("\\(",y)) next
    tryCatch( scidbquery(paste("remove(",y,")",sep=""),async=async, release=1),
              error=function(e) error(e))
  }
  invisible()
}
scidbrm = function(x,error=warning,...) scidbremove(x,error,...)

# df2scidb: User function to send a data frame to SciDB
# Returns a scidbdf object
df2scidb = function(X,
                    name=tmpnam(),
                    dimlabel="row",
                    chunkSize,
                    rowOverlap=0L,
                    types=NULL,
                    nullable,
                    schema_only=FALSE,
                    gc, start)
{
  if(!is.data.frame(X)) stop("X must be a data frame")
  if(missing(start)) start=1
  if(missing(gc)) gc=FALSE
  if(missing(nullable)) nullable = as.vector(unlist(lapply(X,function(x) any(is.na(x)))))
  if(length(nullable)==1) nullable = rep(nullable, ncol(X))
  if(length(nullable)!=ncol(X)) stop ("nullable must be either of length 1 or ncol(X)")
  if(!is.null(types) && length(types)!=ncol(X)) stop("types must match the number of columns of X")
  n1 = nullable
  nullable = rep("",ncol(X))
  if(any(n1)) nullable[n1] = "NULL"
  anames = make.names(names(X),unique=TRUE)
  anames = gsub("\\.","_",anames,perl=TRUE)
  if(length(anames)!=ncol(X)) anames=make.names(1:ncol(X))
  if(!all(anames==names(X))) warning("Attribute names have been changed")
# Check for attribute/dimension name conflict
  old_dimlabel = dimlabel
  dimlabel = tail(make.names(c(anames,dimlabel),unique=TRUE),n=1)
  dimlabel = gsub("\\.","_",dimlabel,perl=TRUE)
  if(dimlabel!=old_dimlabel) warning("Dimension name has been changed")
  if(missing(chunkSize)) {
    chunkSize = min(nrow(X),10000)
  }
  m = ceiling(nrow(X) / chunkSize)

# Default type is string
  typ = rep(paste("string",nullable),ncol(X))
  args = "<"
  if(!is.null(types)) {
    for(j in 1:ncol(X)) typ[j]=paste(types[j],nullable[j])
  } else {
    for(j in 1:ncol(X)) {
      if("numeric" %in% class(X[,j])) 
        typ[j] = paste("double",nullable[j])
      else if("integer" %in% class(X[,j])) 
        typ[j] = paste("int32",nullable[j])
      else if("logical" %in% class(X[,j])) 
        typ[j] = paste("bool",nullable[j])
      else if("character" %in% class(X[,j])) 
        typ[j] = paste("string",nullable[j])
      else if("factor" %in% class(X[,j])) 
        typ[j] = paste("string",nullable[j])
      else if("POSIXct" %in% class(X[,j])) 
      {
        warning("Converting R POSIXct to SciDB datetime as UTC time. Subsecond times rounded to seconds.")
#        X[,j] = as.integer(X[,j])
        X[,j] = format(X[,j],tz="UTC")
        typ[j] = paste("datetime",nullable[j])
      }
    }  
  }
  for(j in 1:ncol(X)) {
    args = paste(args,anames[j],":",typ[j],sep="")
    if(j<ncol(X)) args=paste(args,",",sep="")
    else args=paste(args,">")
  }

  SCHEMA = paste(args,"[",dimlabel,"=",sprintf("%.0f",start),":",sprintf("%.0f",(nrow(X)+start-1)),",",sprintf("%.0f",chunkSize),",", rowOverlap,"]",sep="")

  if(schema_only) return(SCHEMA)

# Obtain a session from the SciDB http service for the upload process
  session = getSession()
  on.exit(GET("/release_session",list(id=session)) ,add=TRUE)

# Create SciDB input string from the data frame
  scidbInput = .df2scidb(X,chunkSize,start)

# Post the input string to the SciDB http service
  uri = URI("upload_file",list(id=session))
  tmp = postForm(uri=uri, uploadedfile=fileUpload(contents=scidbInput,filename="scidb",contentType="application/octet-stream"),.opts=curlOptions(httpheader=c(Expect=""),'ssl.verifypeer'=0))
  tmp = tmp[[1]]
  tmp = gsub("\r","",tmp)
  tmp = gsub("\n","",tmp)

# Load query
  query = sprintf("store(input(%s, '%s'),%s)",SCHEMA, tmp, name)
  scidbquery(query, async=FALSE, release=1, session=session)
  scidb(name,`data.frame`=TRUE,gc=gc)
}

# Sparse matrix to SciDB
.Matrix2scidb = function(X,name,rowChunkSize=1000,colChunkSize=1000,start=c(0,0),gc=TRUE,...)
{
  D = dim(X)
  N = Matrix::nnzero(X)
  rowOverlap=0L
  colOverlap=0L
  if(length(start)<1) stop ("Invalid starting coordinates")
  if(length(start)>2) start = start[1:2]
  if(length(start)<2) start = c(start, 0)
  start = as.integer(start)
  type = .scidbtypes[[typeof(X@x)]]
  if(is.null(type)) {
    stop(paste("Unupported data type. The package presently supports: ",
       paste(.scidbtypes,collapse=" "),".",sep=""))
  }
  if(type!="double") stop("Sorry, the package only supports double-precision sparse matrices right now.")
  schema = sprintf(
      "< val : %s >  [i=%.0f:%.0f,%.0f,%.0f, j=%.0f:%.0f,%.0f,%.0f]", type, start[[1]],
      nrow(X)-1+start[[1]], min(nrow(X),rowChunkSize), rowOverlap, start[[2]], ncol(X)-1+start[[2]],
      min(ncol(X),colChunkSize), colOverlap)
  schema1d = sprintf("<i:int64, j:int64, val : %s>[idx=0:*,100000,0]",type)
# Obtain a session from shim for the upload process
  session = getSession()
  if(length(session)<1) stop("SciDB http session error")
  on.exit(GET("/release_session",list(id=session)) ,add=TRUE)

# Compute the indices and assemble message to SciDB in the form
# double,double,double for indices i,j and data val.
  dp = diff(X@p)
  j  = rep(seq_along(dp),dp) - 1
# Upload the data
# XXX I couldn't get RCurl to work using the fileUpload(contents=x), with 'x'
# a raw vector. But we need RCurl to support SSL. As a work-around, we save
# the object. This copy sucks and must be fixed.
  fn = tempfile()
  bytes = writeBin(as.vector(t(matrix(c(X@i + start[[1]],j + start[[2]], X@x),length(X@x)))),con=fn)
  url = URI("/upload_file",list(id=session))
  ans = postForm(uri = url, uploadedfile = fileUpload(filename=fn),
                 .opts = curlOptions(httpheader = c(Expect = ""),'ssl.verifypeer'=0))
  unlink(fn)
  ans = ans[[1]]
  ans = gsub("\r","",ans)
  ans = gsub("\n","",ans)

  query = sprintf("store(redimension(input(%s,'%s',0,'(double,double,double)'),%s),%s)",schema1d, ans, schema, name)
  iquery(query)
  scidb(name,gc=gc)
}


iquery = function(query, `return`=FALSE,
                  afl=TRUE, iterative=FALSE,
                  n=Inf, excludecol, ...)
{
  if(!afl && `return`) stop("return=TRUE may only be used with AFL statements")
  if(iterative && !`return`) stop("Iterative result requires return=TRUE")
  if(is.scidb(query) || is.scidbdf(query))  query=query@name
  if(missing(excludecol)) excludecol=NA
  if(iterative)
  {
    sessionid = scidbquery(query,afl,async=FALSE,save="lcsv+",release=0)
    return(iqiter(con=sessionid,n=n,excludecol=excludecol,...))
  }
  qsplit = strsplit(query,";")[[1]]
  m = 1
  if(n==Inf) n = -1    # Indicate to shim that we want all the lines of output
  for(query in qsplit)
  {
    if(`return` && m==length(qsplit))
    {
      ans = tryCatch(
       {
        sessionid = scidbquery(query,afl,async=FALSE,save="lcsv+",release=0)
        result = GET("/read_lines",list(id=sessionid,n=as.integer(n+1)),header=FALSE)
# Handle escaped quotes
        result = gsub("\\\\'","''",result)
        result = gsub("\\\\\"","''",result)
        val = textConnection(result)
        ret = tryCatch({
                read.table(val,sep=",",stringsAsFactors=FALSE,header=TRUE,...)},
                error=function(e){ warning(e);c()})
        close(val)
        GET("/release_session",list(id=sessionid))
        chr = sapply(ret, function(x) "character" %in% class(x))
        if(any(chr))
        {
          for(j in which(chr)) ret[ret[,j]=="null",j] = NA
        }
        ret
       }, error = function(e)
           {
             stop(e)
           })
    } else ans = scidbquery(query,afl)
    m = m + 1
  }
  if(!(`return`)) return(invisible())
  ans
}

iqiter = function (con, n = 1, excludecol, ...)
{
  if(missing(excludecol)) excludecol=NA
  dostop = function(s=TRUE)
  {
    GET("/release_session",list(id=con))
    if(s) stop("StopIteration",call.=FALSE)
  }
  if (!is.numeric(n) || length(n) != 1 || n < 1)
    stop("n must be a numeric value >= 1")
  init = TRUE
  header = c()
  nextEl = function() {
    if (is.null(con)) dostop()
    if(init) {
      ans = tryCatch(
       {
        result = GET("/read_lines",list(id=con,n=n),header=FALSE)
        val = textConnection(result)
        ret=read.table(val,sep=",",stringsAsFactors=FALSE,header=TRUE,nrows=n,...)
        close(val)
        ret
       }, error = function(e) {dostop()},
          warning = function(w) {dostop()}
      )
      header <<- colnames(ans)
      init <<- FALSE
    } else {
      ans = tryCatch(
       {
        result = GET("/read_lines",list(id=con,n=n),header=FALSE)
        val = textConnection(result)
        ret = read.table(val,sep=",",stringsAsFactors=FALSE,header=FALSE,nrows=n,...)
        close(val)
        ret
       }, error = function(e) {dostop()},
          warning = function(w) {dostop()}
      )
      colnames(ans) = header
    }
    if(!is.na(excludecol) && excludecol<=ncol(ans))
    {
      rownames(ans) = ans[,excludecol]
      ans=ans[,-excludecol, drop=FALSE]
    }
    ans
  }
  it = list(nextElem = nextEl, gc=new.env())
  class(it) = c("abstractiter", "iter")
  it$gc$remove = TRUE
  reg.finalizer(it$gc, function(e) if(e$remove) 
                  tryCatch(dostop(FALSE),error=function(e) stop(e)),
                  onexit=TRUE)
  it
}


# Utility csv2scidb function
.df2scidb = function(X, chunksize, start=1)
{
  if(missing(chunksize)) chunksize = min(nrow(X),10000L)
#  scipen = options("scipen")
#  options(scipen=20)
#  buf = capture.output(
#         write.table(X, file=stdout(), sep=",",
#                     row.names=FALSE,col.names=FALSE,quote=FALSE)
#        )
#  options(scipen=scipen)
#  x = sapply(1:length(buf), function(j)
#    {
#      chunk = floor(j/chunksize)
#      if(j==length(buf))
#      {
#        if((j-1) %% chunksize == 0)
#          tmp = sprintf("{%.0f}[\n(%s)];",start + chunk*chunksize, buf[j])
#        else
#          tmp = sprintf("(%s)];",buf[j])
#      } else if((j-1) %% chunksize==0)
#      {
#        tmp = sprintf("{%.0f}[\n(%s),",start + chunk*chunksize, buf[j])
#      } else if((j) %% chunksize == 0)
#      {
#        tmp = sprintf("(%s)];",buf[j])
#      } else
#      {
#        tmp = sprintf("(%s),",buf[j])
#      }
#      tmp
#    }
#  )
#  x = paste(x,collapse="")
#  ans = paste(x,collapse="")

  ans = .Call("df2scidb",X,as.integer(chunksize), as.double(round(start)), "%.15f")
  ans
}

# Return a SciDB schema of a scidb object x.
# Explicitly indicate attribute part of schema with remaining arguments
`extract_schema` = function(x, at=x@attributes, ty=x@types, nu=x@nullable)
{
  if(!(inherits(x,"scidb") || inherits(x,"scidbdf"))) stop("Not a scidb object")
  op = options(scipen=20)
  nullable = rep("",length(nu))
  if(any(nu)) nullable[nu] = " NULL"
  attr = paste(at,ty,sep=":")
  attr = paste(attr, nullable,sep="")
  attr = sprintf("<%s>",paste(attr,collapse=","))
  dims = sprintf("[%s]",paste(paste(paste(paste(paste(paste(x@D$name,"=",sep=""),x@D$start,sep=""),x@D$start+x@D$length-1,sep=":"),x@D$chunk_interval,sep=","),x@D$chunk_overlap,sep=","),collapse=","))
  options(op)
  paste(attr,dims,sep="")
}

# Internal utility function, make every attribute of an array nullable
`make_nullable` = function(x)
{
  cast(x,sprintf("%s%s",build_attr_schema(x,nullable=TRUE),build_dim_schema(x)))
}

# Build the attribute part of a SciDB array schema from a scidb, scidbdf object.
# Set prefix to add a prefix to all attribute names.
# I: optional vector of dimension indices to use, if missing use all
# newnames: optional vector of new dimension names, must be the same length
#    as I.
# nullable: optional vector of new nullability expressed as FALSE or TRUE,
#    must be the same length as I.
`build_attr_schema` = function(A, prefix="", I, newnames, nullable)
{
  if(missing(I)) I = rep(TRUE,length(A@attributes))
  if(!(class(A) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  if(is.logical(I)) I = which(I)
  N = rep("", length(I))
  N[A@nullable[I]] = " NULL"
  if(!missing(nullable))
  {
    N = rep("", length(I))
    N[nullable] = " NULL"
  }
  N = paste(A@types[I],N,sep="")
  attributes = paste(prefix,A@attributes[I],sep="")
  if(!missing(newnames)) attributes = newnames
  S = paste(paste(attributes,N,sep=":"),collapse=",")
  sprintf("<%s>",S)
}

`noE` = function(w) sapply(w, function(x) sprintf("%.0f",x))

# Build the dimension part of a SciDB array schema from a scidb,
# scidbdf object.
# A: A scidb or scidbdf object
# bracket: if TRUE, enclose dimension expression in square brackets
# I: optional vector of dimension indices to use, if missing use all
# newnames: optional vector of new dimension names, must be the same length
#    as I.
# newlen: optional vector of new dimension lengths, must be the same length
#    as I.
# newstart: optional vector of new start values, must be the same length
#    as I.
`build_dim_schema` = function(A,bracket=TRUE, I, newnames, newlen, newstart)
{
  if(!(class(A) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  if(!missing(I))
  {
    A@D$type = A@D$type[I]
    A@D$name = A@D$name[I]
    A@D$start = A@D$start[I]
    A@D$length = A@D$length[I]
    A@D$low = A@D$low[I]
    A@D$high = A@D$high[I]
    A@D$chunk_interval = A@D$chunk_interval[I]
    A@D$chunk_overlap = A@D$chunk_overlap[I]
  }
  if(!missing(newnames))
  {
    A@D$name = newnames
  }
  if(!missing(newstart))
  {
    A@D$start = newstart
  }
  if(!missing(newlen))
  {
    A@D$length = newlen
  }

  low = noE(A@D$start)
  hi = noE(A@D$length - 1 + A@D$start)
  hi[as.numeric(hi)>=as.numeric(.scidb_DIM_MAX)] = "*"
  hi[is.na(hi)] = "*"
  R = paste(low,hi,sep=":")
  S = paste(A@D$name,R,sep="=")
  S = paste(S,noE(A@D$chunk_interval),sep=",")
  S = paste(S,noE(A@D$chunk_overlap),sep=",")
  S = paste(S,collapse=",")
  if(bracket) S = sprintf("[%s]",S)
  S
}

# SciDB rename wrapper
rename = function(A, name=A@name, gc)
{
  if(!(inherits(A,"scidb") || inherits(A,"scidbdf"))) stop("`A` must be a scidb object.")
  if(missing(gc)) gc = FALSE
  if(exists("remove",envir=A@gc)) A@gc$remove=FALSE
  if(A@name != name) scidbquery(sprintf("rename(%s,%s)",A@name, name))
  A@name = name
  if(gc)
  {
    A@gc$name = name
    A@gc$remove = TRUE
    reg.finalizer(A@gc, function(e) if (e$remove) 
        tryCatch(scidbremove(e$name, async=TRUE), error=function(e){invisible()}), 
            onexit = TRUE)
  } else A@gc = new.env()
  A
}

# .scidbdim is an internal function that retirieves dimension metadata from a
# scidb array called "name."
.scidbdim = function(name)
{
#  if(!.scidbexists(name)) stop ("not found") 
  d = iquery(paste("dimensions(",name,")"),return=TRUE)
# R is unfortunately interpreting 'i' as an imaginary unit I think.
  if(any(is.na(d))) d[is.na(d)] = "i"
  d
}

# Retrieve list of attributes for a named SciDB array (internal function).
.scidbattributes = function(name)
{
  x = iquery(paste("attributes(",name,")",sep=""),return=TRUE,colClasses=c(NA,"character",NA,NA))
# R is unfortunately interpreting 'i' as an imaginary unit I think.
  if(any(is.na(x))) x[is.na(x)] = "i"
  list(attributes=x[,2],types=x[,3],nullable=(x[,4]=="true"))
}

# If a scidb object has the "sparse" attribute set, return that. Otherwise
# interrogate the backing array to determine if it's sparse or not.
is.sparse = function(x)
{
  ans = attr(x,"sparse")
  if(is.null(ans))
  {
    return(count(x) < prod(dim(x)))
  }
  ans
}

# Returns TRUE if version string x is greater than or equal to than version y
compare_versions = function(x,y)
{
 as.logical(prod(as.numeric(strsplit(as.character(x),"\\.")[[1]]) >= as.numeric(strsplit(as.character(y),"\\.")[[1]])))
}

# Reset array coordinate system to zero-indexed origin
origin = function(x)
{
  N = paste(rep("null",2*length(x@D$name)),collapse=",")
  query = sprintf("sg(subarray(%s,%s),1,-1)",x@name,N)
  .scidbeval(query,`eval`=FALSE,depend=list(x),gc=TRUE)
}

# Silly function to return schema string from object
schema = function(x)
{
  if(!(inherits(x,"scidb") || inherits(x,"scidbdf"))) return(NULL)
  gsub("^array","",x@schema)
}
