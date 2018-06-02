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

Cluster_ptr new_Cluster(struct capn_segment *s) {
	Cluster_ptr p;
	p.p = capn_new_struct(s, 8, 1);
	return p;
}
Cluster_list new_Cluster_list(struct capn_segment *s, int len) {
	Cluster_list p;
	p.p = capn_new_list(s, len, 8, 1);
	return p;
}
void read_Cluster(struct Cluster *s capnp_unused, Cluster_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	s->leader = capn_get_text(p.p, 0, capn_val0);
	s->heartbeat = capn_read8(p.p, 0);
}
void write_Cluster(const struct Cluster *s capnp_unused, Cluster_ptr p) {
	capn_resolve(&p.p);
	capnp_use(s);
	capn_set_text(p.p, 0, s->leader);
	capn_write8(p.p, 0, s->heartbeat);
}
void get_Cluster(struct Cluster *s, Cluster_list l, int i) {
	Cluster_ptr p;
	p.p = capn_getp(l.p, i, 0);
	read_Cluster(s, p);
}
void set_Cluster(const struct Cluster *s, Cluster_list l, int i) {
	Cluster_ptr p;
	p.p = capn_getp(l.p, i, 0);
	write_Cluster(s, p);
}

capn_text Cluster_get_leader(Cluster_ptr p)
{
	capn_text leader;
	leader = capn_get_text(p.p, 0, capn_val0);
	return leader;
}

uint8_t Cluster_get_heartbeat(Cluster_ptr p)
{
	uint8_t heartbeat;
	heartbeat = capn_read8(p.p, 0);
	return heartbeat;
}

void Cluster_set_leader(Cluster_ptr p, capn_text leader)
{
	capn_set_text(p.p, 0, leader);
}

void Cluster_set_heartbeat(Cluster_ptr p, uint8_t heartbeat)
{
	capn_write8(p.p, 0, heartbeat);
}
