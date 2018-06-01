#include "protocol.capnp.h"
/* AUTO GENERATED - DO NOT EDIT */
#ifdef __GNUC__
# define capnp_unused __attribute__((unused))
# define capnp_use(x) (void) x;
#else
# define capnp_unused
# define capnp_use(x)
#endif

static const capn_text capn_val0 = {0,"",0};

Request_ptr new_Request(struct capn_segment *s) {
	Request_ptr p;
	p.p = capn_new_struct(s, 8, 1);
	return p;
}
Request_list new_Request_list(struct capn_segment *s, int len) {
	Request_list p;
	p.p = capn_new_list(s, len, 8, 1);
	return p;
}
void read_Request(struct Request *s capnp_unused, Request_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	s->which = (enum Request_which)(int) capn_read16(p.p, 0);
	switch (s->which) {
	case Request_leader:
	case Request_heartbeat:
		s->heartbeat.p = capn_getp(p.p, 0, 0);
		break;
	default:
		break;
	}
}
void write_Request(const struct Request *s capnp_unused, Request_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	capn_write16(p.p, 0, s->which);
	switch (s->which) {
	case Request_leader:
	case Request_heartbeat:
		capn_setp(p.p, 0, s->heartbeat.p);
		break;
	default:
		break;
	}
}
void get_Request(struct Request *s, Request_list l, int i) {
	Request_ptr p;
	p.p = capn_getp(l.p, i, 0);
	read_Request(s, p);
}
void set_Request(const struct Request *s, Request_list l, int i) {
	Request_ptr p;
	p.p = capn_getp(l.p, i, 0);
	write_Request(s, p);
}

Leader_ptr new_Leader(struct capn_segment *s) {
	Leader_ptr p;
	p.p = capn_new_struct(s, 0, 0);
	return p;
}
Leader_list new_Leader_list(struct capn_segment *s, int len) {
	Leader_list p;
	p.p = capn_new_list(s, len, 0, 0);
	return p;
}
void read_Leader(struct Leader *s capnp_unused, Leader_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
}
void write_Leader(const struct Leader *s capnp_unused, Leader_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
}
void get_Leader(struct Leader *s, Leader_list l, int i) {
	Leader_ptr p;
	p.p = capn_getp(l.p, i, 0);
	read_Leader(s, p);
}
void set_Leader(const struct Leader *s, Leader_list l, int i) {
	Leader_ptr p;
	p.p = capn_getp(l.p, i, 0);
	write_Leader(s, p);
}

Heartbeat_ptr new_Heartbeat(struct capn_segment *s) {
	Heartbeat_ptr p;
	p.p = capn_new_struct(s, 0, 0);
	return p;
}
Heartbeat_list new_Heartbeat_list(struct capn_segment *s, int len) {
	Heartbeat_list p;
	p.p = capn_new_list(s, len, 0, 0);
	return p;
}
void read_Heartbeat(struct Heartbeat *s capnp_unused, Heartbeat_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
}
void write_Heartbeat(const struct Heartbeat *s capnp_unused, Heartbeat_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
}
void get_Heartbeat(struct Heartbeat *s, Heartbeat_list l, int i) {
	Heartbeat_ptr p;
	p.p = capn_getp(l.p, i, 0);
	read_Heartbeat(s, p);
}
void set_Heartbeat(const struct Heartbeat *s, Heartbeat_list l, int i) {
	Heartbeat_ptr p;
	p.p = capn_getp(l.p, i, 0);
	write_Heartbeat(s, p);
}

Server_ptr new_Server(struct capn_segment *s) {
	Server_ptr p;
	p.p = capn_new_struct(s, 0, 1);
	return p;
}
Server_list new_Server_list(struct capn_segment *s, int len) {
	Server_list p;
	p.p = capn_new_list(s, len, 0, 1);
	return p;
}
void read_Server(struct Server *s capnp_unused, Server_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	s->address = capn_get_text(p.p, 0, capn_val0);
}
void write_Server(const struct Server *s capnp_unused, Server_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	capn_set_text(p.p, 0, s->address);
}
void get_Server(struct Server *s, Server_list l, int i) {
	Server_ptr p;
	p.p = capn_getp(l.p, i, 0);
	read_Server(s, p);
}
void set_Server(const struct Server *s, Server_list l, int i) {
	Server_ptr p;
	p.p = capn_getp(l.p, i, 0);
	write_Server(s, p);
}

capn_text Server_get_address(Server_ptr p)
{
	capn_text address;
	address = capn_get_text(p.p, 0, capn_val0);
	return address;
}

void Server_set_address(Server_ptr p, capn_text address)
{
	capn_set_text(p.p, 0, address);
}
