@0x8c575a03d603a6d5;

annotation fieldgetset @0xf72bc690355d66de (file): Void;
$fieldgetset;

# Hold information about a request sent by dqlite client.
struct Request {
  union {
    leader    @0 :Leader;
    heartbeat @1 :Heartbeat;
  }
}

struct Leader {
}

struct Heartbeat {
}

# Hold information about a dqlite server.
struct Server {
  # Address the server is reachable at (IP or host name)
  address @0 :Text;
}
