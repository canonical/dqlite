@0x8c575a03d603a6d5;

annotation fieldgetset @0xf72bc690355d66de (file): Void;
$fieldgetset;

# Hold information about a request sent by dqlite client.
struct Request {
  union {
    helo      @0 :Helo;
    heartbeat @1 :Heartbeat;
  }
}

# Initial message that a client must send to register itself.
#
# The server will reply with a Cluster response.
struct Helo {
}

# Heartbeat message that a client must send periodically.
struct Heartbeat {
}

# Information about the cluster, sent as response to Register.
struct Cluster {
  # Address of the current cluster leader.
  leader    @0 :Text;

  # Timeout after which the connection will be killed if no
  # heartbeat is received from the client.
  heartbeat @1 :UInt8;
}
