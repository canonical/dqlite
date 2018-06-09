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
	case Request_helo:
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
	case Request_helo:
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

Helo_ptr new_Helo(struct capn_segment *s) {
	Helo_ptr p;
	p.p = capn_new_struct(s, 0, 0);
	return p;
}
Helo_list new_Helo_list(struct capn_segment *s, int len) {
	Helo_list p;
	p.p = capn_new_list(s, len, 0, 0);
	return p;
}
void read_Helo(struct Helo *s capnp_unused, Helo_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
}
void write_Helo(const struct Helo *s capnp_unused, Helo_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
}
void get_Helo(struct Helo *s, Helo_list l, int i) {
	Helo_ptr p;
	p.p = capn_getp(l.p, i, 0);
	read_Helo(s, p);
}
void set_Helo(const struct Helo *s, Helo_list l, int i) {
	Helo_ptr p;
	p.p = capn_getp(l.p, i, 0);
	write_Helo(s, p);
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

Welcome_ptr new_Welcome(struct capn_segment *s) {
	Welcome_ptr p;
	p.p = capn_new_struct(s, 8, 1);
	return p;
}
Welcome_list new_Welcome_list(struct capn_segment *s, int len) {
	Welcome_list p;
	p.p = capn_new_list(s, len, 8, 1);
	return p;
}
void read_Welcome(struct Welcome *s capnp_unused, Welcome_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	s->leader = capn_get_text(p.p, 0, capn_val0);
	s->heartbeatTimeout = capn_read16(p.p, 0);
}
void write_Welcome(const struct Welcome *s capnp_unused, Welcome_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	capn_set_text(p.p, 0, s->leader);
	capn_write16(p.p, 0, s->heartbeatTimeout);
}
void get_Welcome(struct Welcome *s, Welcome_list l, int i) {
	Welcome_ptr p;
	p.p = capn_getp(l.p, i, 0);
	read_Welcome(s, p);
}
void set_Welcome(const struct Welcome *s, Welcome_list l, int i) {
	Welcome_ptr p;
	p.p = capn_getp(l.p, i, 0);
	write_Welcome(s, p);
}

capn_text Welcome_get_leader(Welcome_ptr p)
{
	capn_text leader;
	leader = capn_get_text(p.p, 0, capn_val0);
	return leader;
}

uint16_t Welcome_get_heartbeatTimeout(Welcome_ptr p)
{
	uint16_t heartbeatTimeout;
	heartbeatTimeout = capn_read16(p.p, 0);
	return heartbeatTimeout;
}

void Welcome_set_leader(Welcome_ptr p, capn_text leader)
{
	capn_set_text(p.p, 0, leader);
}

void Welcome_set_heartbeatTimeout(Welcome_ptr p, uint16_t heartbeatTimeout)
{
	capn_write16(p.p, 0, heartbeatTimeout);
}

Servers_ptr new_Servers(struct capn_segment *s) {
	Servers_ptr p;
	p.p = capn_new_struct(s, 0, 1);
	return p;
}
Servers_list new_Servers_list(struct capn_segment *s, int len) {
	Servers_list p;
	p.p = capn_new_list(s, len, 0, 1);
	return p;
}
void read_Servers(struct Servers *s capnp_unused, Servers_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	s->addresses.p = capn_getp(p.p, 0, 0);
}
void write_Servers(const struct Servers *s capnp_unused, Servers_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	capn_setp(p.p, 0, s->addresses.p);
}
void get_Servers(struct Servers *s, Servers_list l, int i) {
	Servers_ptr p;
	p.p = capn_getp(l.p, i, 0);
	read_Servers(s, p);
}
void set_Servers(const struct Servers *s, Servers_list l, int i) {
	Servers_ptr p;
	p.p = capn_getp(l.p, i, 0);
	write_Servers(s, p);
}

Address_list Servers_get_addresses(Servers_ptr p)
{
	Address_list addresses;
	addresses.p = capn_getp(p.p, 0, 0);
	return addresses;
}

void Servers_set_addresses(Servers_ptr p, Address_list addresses)
{
	capn_setp(p.p, 0, addresses.p);
}

Address_ptr new_Address(struct capn_segment *s) {
	Address_ptr p;
	p.p = capn_new_struct(s, 0, 1);
	return p;
}
Address_list new_Address_list(struct capn_segment *s, int len) {
	Address_list p;
	p.p = capn_new_list(s, len, 0, 1);
	return p;
}
void read_Address(struct Address *s capnp_unused, Address_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	s->value = capn_get_text(p.p, 0, capn_val0);
}
void write_Address(const struct Address *s capnp_unused, Address_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	capn_set_text(p.p, 0, s->value);
}
void get_Address(struct Address *s, Address_list l, int i) {
	Address_ptr p;
	p.p = capn_getp(l.p, i, 0);
	read_Address(s, p);
}
void set_Address(const struct Address *s, Address_list l, int i) {
	Address_ptr p;
	p.p = capn_getp(l.p, i, 0);
	write_Address(s, p);
}

capn_text Address_get_value(Address_ptr p)
{
	capn_text value;
	value = capn_get_text(p.p, 0, capn_val0);
	return value;
}

void Address_set_value(Address_ptr p, capn_text value)
{
	capn_set_text(p.p, 0, value);
}
