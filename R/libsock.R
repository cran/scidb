.SOCK_CLOSE = function(socket)
{
  .Call('SOCK_CLOSE', as.integer(socket), PACKAGE='scidb')
}

.SOCK_RECV = function(socket, external_pointer=FALSE, buf_size=8192, max_buffer_size=2^24)
{
  .Call('SOCK_RECV', as.integer(socket), as.integer(external_pointer), as.integer(buf_size), as.numeric(max_buffer_size), PACKAGE='scidb')
}

# We trap the possibility of a SIGPIPE signal error during SOCK_SEND.
.SOCK_SEND = function(socket, msg)
{
  if(is.raw(msg)) return(
    tryCatch(.Call('SOCK_SEND', socket, msg, PACKAGE='scidb'),
      error=function(e) -1, interrupt=function(e) -1))
  if(is.character(msg))
    return(
      tryCatch(
        .Call('SOCK_SEND', socket, charToRaw(msg), PACKAGE='scidb'),
        error=function(e) -1, interrupt=function(e) -1))
  stop("msg must be of data type 'Raw'")
}

.SOCK_GETSOCKNAME = function(socket)
{
  .Call('SOCK_NAME', as.integer(socket), PACKAGE='scidb')
}

.SOCK_CONNECT = function(host, port)
{
  .Call('SOCK_CONNECT', as.character(host), as.integer(port),
        PACKAGE='scidb')
}
