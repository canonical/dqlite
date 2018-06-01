#ifndef CAPN_8C575A03D603A6D5
#define CAPN_8C575A03D603A6D5
/* AUTO GENERATED - DO NOT EDIT */
#include <capnp_c.h>

#if CAPN_VERSION != 1
#error "version mismatch between capnp_c.h and generated code"
#endif

#ifndef capnp_nowarn
# ifdef __GNUC__
#  define capnp_nowarn __extension__
# else
#  define capnp_nowarn
# endif
#endif


#ifdef __cplusplus
extern "C" {
#endif

struct Request;
struct Leader;
struct Heartbeat;
struct Server;

typedef struct {capn_ptr p;} Request_ptr;
typedef struct {capn_ptr p;} Leader_ptr;
typedef struct {capn_ptr p;} Heartbeat_ptr;
typedef struct {capn_ptr p;} Server_ptr;

typedef struct {capn_ptr p;} Request_list;
typedef struct {capn_ptr p;} Leader_list;
typedef struct {capn_ptr p;} Heartbeat_list;
typedef struct {capn_ptr p;} Server_list;
enum Request_which {
	Request_leader = 0,
	Request_heartbeat = 1
};

struct Request {
	enum Request_which which;
	capnp_nowarn union {
		Leader_ptr leader;
		Heartbeat_ptr heartbeat;
	};
};

static const size_t Request_word_count = 1;

static const size_t Request_pointer_count = 1;

static const size_t Request_struct_bytes_count = 16;

capnp_nowarn struct Leader {
};

static const size_t Leader_word_count = 0;

static const size_t Leader_pointer_count = 0;

static const size_t Leader_struct_bytes_count = 0;

capnp_nowarn struct Heartbeat {
};

static const size_t Heartbeat_word_count = 0;

static const size_t Heartbeat_pointer_count = 0;

static const size_t Heartbeat_struct_bytes_count = 0;

struct Server {
	capn_text address;
};

static const size_t Server_word_count = 0;

static const size_t Server_pointer_count = 1;

static const size_t Server_struct_bytes_count = 8;

capn_text Server_get_address(Server_ptr p);

void Server_set_address(Server_ptr p, capn_text address);

Request_ptr new_Request(struct capn_segment*);
Leader_ptr new_Leader(struct capn_segment*);
Heartbeat_ptr new_Heartbeat(struct capn_segment*);
Server_ptr new_Server(struct capn_segment*);

Request_list new_Request_list(struct capn_segment*, int len);
Leader_list new_Leader_list(struct capn_segment*, int len);
Heartbeat_list new_Heartbeat_list(struct capn_segment*, int len);
Server_list new_Server_list(struct capn_segment*, int len);

void read_Request(struct Request*, Request_ptr);
void read_Leader(struct Leader*, Leader_ptr);
void read_Heartbeat(struct Heartbeat*, Heartbeat_ptr);
void read_Server(struct Server*, Server_ptr);

void write_Request(const struct Request*, Request_ptr);
void write_Leader(const struct Leader*, Leader_ptr);
void write_Heartbeat(const struct Heartbeat*, Heartbeat_ptr);
void write_Server(const struct Server*, Server_ptr);

void get_Request(struct Request*, Request_list, int i);
void get_Leader(struct Leader*, Leader_list, int i);
void get_Heartbeat(struct Heartbeat*, Heartbeat_list, int i);
void get_Server(struct Server*, Server_list, int i);

void set_Request(const struct Request*, Request_list, int i);
void set_Leader(const struct Leader*, Leader_list, int i);
void set_Heartbeat(const struct Heartbeat*, Heartbeat_list, int i);
void set_Server(const struct Server*, Server_list, int i);

#ifdef __cplusplus
}
#endif
#endif
