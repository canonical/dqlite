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
struct Helo;
struct Heartbeat;
struct Cluster;
struct Servers;
struct Address;

typedef struct {capn_ptr p;} Request_ptr;
typedef struct {capn_ptr p;} Helo_ptr;
typedef struct {capn_ptr p;} Heartbeat_ptr;
typedef struct {capn_ptr p;} Cluster_ptr;
typedef struct {capn_ptr p;} Servers_ptr;
typedef struct {capn_ptr p;} Address_ptr;

typedef struct {capn_ptr p;} Request_list;
typedef struct {capn_ptr p;} Helo_list;
typedef struct {capn_ptr p;} Heartbeat_list;
typedef struct {capn_ptr p;} Cluster_list;
typedef struct {capn_ptr p;} Servers_list;
typedef struct {capn_ptr p;} Address_list;
enum Request_which {
	Request_helo = 0,
	Request_heartbeat = 1
};

struct Request {
	enum Request_which which;
	capnp_nowarn union {
		Helo_ptr helo;
		Heartbeat_ptr heartbeat;
	};
};

static const size_t Request_word_count = 1;

static const size_t Request_pointer_count = 1;

static const size_t Request_struct_bytes_count = 16;

capnp_nowarn struct Helo {
};

static const size_t Helo_word_count = 0;

static const size_t Helo_pointer_count = 0;

static const size_t Helo_struct_bytes_count = 0;

capnp_nowarn struct Heartbeat {
};

static const size_t Heartbeat_word_count = 0;

static const size_t Heartbeat_pointer_count = 0;

static const size_t Heartbeat_struct_bytes_count = 0;

struct Cluster {
	capn_text leader;
	uint16_t heartbeatTimeout;
};

static const size_t Cluster_word_count = 1;

static const size_t Cluster_pointer_count = 1;

static const size_t Cluster_struct_bytes_count = 16;

capn_text Cluster_get_leader(Cluster_ptr p);

uint16_t Cluster_get_heartbeatTimeout(Cluster_ptr p);

void Cluster_set_leader(Cluster_ptr p, capn_text leader);

void Cluster_set_heartbeatTimeout(Cluster_ptr p, uint16_t heartbeatTimeout);

struct Servers {
	Address_list addresses;
};

static const size_t Servers_word_count = 0;

static const size_t Servers_pointer_count = 1;

static const size_t Servers_struct_bytes_count = 8;

Address_list Servers_get_addresses(Servers_ptr p);

void Servers_set_addresses(Servers_ptr p, Address_list addresses);

struct Address {
	capn_text value;
};

static const size_t Address_word_count = 0;

static const size_t Address_pointer_count = 1;

static const size_t Address_struct_bytes_count = 8;

capn_text Address_get_value(Address_ptr p);

void Address_set_value(Address_ptr p, capn_text value);

Request_ptr new_Request(struct capn_segment*);
Helo_ptr new_Helo(struct capn_segment*);
Heartbeat_ptr new_Heartbeat(struct capn_segment*);
Cluster_ptr new_Cluster(struct capn_segment*);
Servers_ptr new_Servers(struct capn_segment*);
Address_ptr new_Address(struct capn_segment*);

Request_list new_Request_list(struct capn_segment*, int len);
Helo_list new_Helo_list(struct capn_segment*, int len);
Heartbeat_list new_Heartbeat_list(struct capn_segment*, int len);
Cluster_list new_Cluster_list(struct capn_segment*, int len);
Servers_list new_Servers_list(struct capn_segment*, int len);
Address_list new_Address_list(struct capn_segment*, int len);

void read_Request(struct Request*, Request_ptr);
void read_Helo(struct Helo*, Helo_ptr);
void read_Heartbeat(struct Heartbeat*, Heartbeat_ptr);
void read_Cluster(struct Cluster*, Cluster_ptr);
void read_Servers(struct Servers*, Servers_ptr);
void read_Address(struct Address*, Address_ptr);

void write_Request(const struct Request*, Request_ptr);
void write_Helo(const struct Helo*, Helo_ptr);
void write_Heartbeat(const struct Heartbeat*, Heartbeat_ptr);
void write_Cluster(const struct Cluster*, Cluster_ptr);
void write_Servers(const struct Servers*, Servers_ptr);
void write_Address(const struct Address*, Address_ptr);

void get_Request(struct Request*, Request_list, int i);
void get_Helo(struct Helo*, Helo_list, int i);
void get_Heartbeat(struct Heartbeat*, Heartbeat_list, int i);
void get_Cluster(struct Cluster*, Cluster_list, int i);
void get_Servers(struct Servers*, Servers_list, int i);
void get_Address(struct Address*, Address_list, int i);

void set_Request(const struct Request*, Request_list, int i);
void set_Helo(const struct Helo*, Helo_list, int i);
void set_Heartbeat(const struct Heartbeat*, Heartbeat_list, int i);
void set_Cluster(const struct Cluster*, Cluster_list, int i);
void set_Servers(const struct Servers*, Servers_list, int i);
void set_Address(const struct Address*, Address_list, int i);

#ifdef __cplusplus
}
#endif
#endif
