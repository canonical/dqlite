#if defined(USE_SYSTEM_RAFT)

#include <raft.h>
#include <raft/uv.h>
#include <raft/fixture.h>

#elif !defined(RAFT_H)

#define RAFT_H

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include <uv.h>

#include "lib/sm.h"
#include "lib/queue.h"

#ifndef RAFT_API
#define RAFT_API __attribute__((visibility("default")))
#endif

#ifndef DQLITE_VISIBLE_TO_TESTS
#define DQLITE_VISIBLE_TO_TESTS __attribute__((visibility("default")))
#endif

/**
 * Version.
 */
#define RAFT_VERSION_MAJOR 0
#define RAFT_VERSION_MINOR 18
#define RAFT_VERSION_RELEASE 0
#define RAFT_VERSION_NUMBER                                          \
	(RAFT_VERSION_MAJOR * 100 * 100 + RAFT_VERSION_MINOR * 100 + \
	 RAFT_VERSION_RELEASE)

int raft_version_number(void);

/**
 * Error codes.
 */
enum {
	RAFT_NOMEM = 1,        /* Out of memory */
	RAFT_BADID,            /* Server ID is not valid */
	RAFT_DUPLICATEID,      /* Server ID already in use */
	RAFT_DUPLICATEADDRESS, /* Server address already in use */
	RAFT_BADROLE,          /* Server role is not valid */
	RAFT_MALFORMED,
	RAFT_NOTLEADER,
	RAFT_LEADERSHIPLOST,
	RAFT_SHUTDOWN,
	RAFT_CANTBOOTSTRAP,
	RAFT_CANTCHANGE,
	RAFT_CORRUPT,
	RAFT_CANCELED,
	RAFT_NAMETOOLONG,
	RAFT_TOOBIG,
	RAFT_NOCONNECTION,
	RAFT_BUSY,
	RAFT_IOERR,        /* File system or storage error */
	RAFT_NOTFOUND,     /* Resource not found */
	RAFT_INVALID,      /* Invalid parameter */
	RAFT_UNAUTHORIZED, /* No access to a resource */
	RAFT_NOSPACE,      /* Not enough space on disk */
	RAFT_TOOMANY       /* Some system or raft limit was hit */
};

/**
 * Size of human-readable error message buffers.
 */
#define RAFT_ERRMSG_BUF_SIZE 256

/**
 * Return the error message describing the given error code.
 */
RAFT_API const char *raft_strerror(int errnum);

typedef unsigned long long raft_id;

/**
 * Hold the value of a raft term. Guaranteed to be at least 64-bit long.
 */
typedef unsigned long long raft_term;

/**
 * Hold the value of a raft entry index. Guaranteed to be at least 64-bit long.
 */
typedef unsigned long long raft_index;

/**
 * Hold a time value expressed in milliseconds since the epoch.
 */
typedef unsigned long long raft_time;

/**
 * Hold the features a raft node is capable of.
 */
typedef uint64_t raft_flags;

/**
 * A data buffer.
 */
struct raft_buffer
{
	void *base; /* Pointer to the buffer data. */
	size_t len; /* Length of the buffer. */
};

/**
 * Server role codes.
 */
enum {
	RAFT_STANDBY, /* Replicate log, does not participate in quorum. */
	RAFT_VOTER,   /* Replicate log, does participate in quorum. */
	RAFT_SPARE    /* Does not replicate log, or participate in quorum. */
};

/**
 * Hold information about a single server in the cluster configuration.
 * WARNING: This struct is encoded/decoded, be careful when adapting it.
 */
struct raft_server
{
	raft_id id;    /* Server ID, must be greater than zero. */
	char *address; /* Server address. User defined. */
	int role;      /* Server role. */
};

/**
 * Hold information about all servers currently part of the cluster.
 * WARNING: This struct is encoded/decoded, be careful when adapting it.
 */
struct raft_configuration
{
	struct raft_server
	    *servers; /* Array of servers member of the cluster. */
	unsigned n;   /* Number of servers in the array. */
};

/**
 * Initialize an empty raft configuration.
 */
RAFT_API void raft_configuration_init(struct raft_configuration *c);

/**
 * Release all memory used by the given configuration object.
 */
RAFT_API void raft_configuration_close(struct raft_configuration *c);

/**
 * Add a server to a raft configuration.
 *
 * The @id must be greater than zero and @address point to a valid string.
 *
 * The @role must be either #RAFT_VOTER, #RAFT_STANDBY, #RAFT_SPARE.
 *
 * If @id or @address are already in use by another server in the configuration,
 * an error is returned.
 *
 * The @address string will be copied and can be released after this function
 * returns.
 */
RAFT_API int raft_configuration_add(struct raft_configuration *c,
				    raft_id id,
				    const char *address,
				    int role);

/**
 * Encode the given configuration object.
 *
 * The memory of the returned buffer is allocated using raft_malloc(), and
 * client code is responsible for releasing it when no longer needed.
 */
RAFT_API int raft_configuration_encode(const struct raft_configuration *c,
				       struct raft_buffer *buf);

/**
 * Hash function which outputs a 64-bit value based on a text and a number.
 *
 * This can be used to generate a unique ID for a new server being added, for
 * example based on its address and on the current time in milliseconds since
 * the Epoch.
 *
 * It's internally implemented as a SHA1 where only the last 8 bytes of the hash
 * value are kept.
 */
RAFT_API unsigned long long raft_digest(const char *text, unsigned long long n);

/**
 * Log entry types.
 */
enum {
	RAFT_COMMAND = 1, /* Command for the application FSM. */
	RAFT_BARRIER,     /* Wait for all previous commands to be applied. */
	RAFT_CHANGE       /* Raft configuration change. */
};

/**
 * A small fixed-size inline buffer that stores extra data for a raft_entry
 * that is different for each node in the cluster.
 *
 * A leader initializes the local data for an entry before passing it into
 * raft_apply. This local data is stored in the volatile raft log and also
 * in the persistent raft log on the leader. AppendEntries messages sent by
 * the leader never contain the local data for entries.
 *
 * When a follower accepts an AppendEntries request, it invokes a callback
 * provided by the FSM to fill out the local data for each new entry before
 * appending the entries to its log (volatile and persistent). This local
 * data doesn't have to be the same as the local data that the leader computed.
 *
 * When starting up, a raft node reads the local data for each entry for its
 * persistent log as part of populating the volatile log.
 */
struct raft_entry_local_data {
	/* Must be the only member of this struct. */
	uint8_t buf[16];
};

/**
 * A single entry in the raft log.
 *
 * An entry that originated from this raft instance while it was the leader
 * (typically via client calls to raft_apply()) should normally have a @buf
 * attribute referencing directly the memory that was originally allocated by
 * the client itself to contain the entry data, and the @batch attribute set to
 * #NULL.
 *
 * An entry that was received from the network as part of an AppendEntries RPC
 * or that was loaded from disk at startup should normally have a @batch
 * attribute that points to a contiguous chunk of memory that contains the data
 * of the entry itself plus possibly the data for other entries that were
 * received or loaded with it at the same time. In this case the @buf pointer
 * will be equal to the @batch pointer plus an offset, that locates the position
 * of the entry's data within the batch.
 *
 * When the @batch attribute is not #NULL the raft library will take care of
 * releasing that memory only once there are no more references to the
 * associated entries.
 *
 * This arrangement makes it possible to minimize the amount of memory-copying
 * when performing I/O.
 *
 * The @is_local field is set to `true` by a leader that appends an entry to its
 * volatile log. It is set to `false` by a follower that copies an entry received
 * via AppendEntries to its volatile log. It is not represented in the AppendEntries
 * message or in the persistent log. This field can be used by the FSM's `apply`
 * callback to handle a COMMAND entry differently depending on whether it
 * originated locally.
 *
 * Note: The @local_data and @is_local fields do not exist when we use an external
 * libraft, because the last separate release of libraft predates their addition.
 * The ifdef at the very top of this file ensures that we use the system raft headers
 * when we build against an external libraft, so there will be no ABI mismatch as
 * a result of incompatible struct layouts.
 */
struct raft_entry
{
	raft_term term;         /* Term in which the entry was created. */
	unsigned short type;    /* Type (FSM command, barrier, config change). */
	bool is_local;          /* Placed here so it goes in the padding after @type. */
	struct raft_buffer buf; /* Entry data. */
	struct raft_entry_local_data local_data;
	void *batch;            /* Batch that buf's memory points to, if any. */
};

/**
 * Hold the arguments of a RequestVote RPC.
 *
 * The RequestVote RPC is invoked by candidates to gather votes.
 */
struct raft_request_vote
{
	int version;
	raft_term term;            /* Candidate's term. */
	raft_id candidate_id;      /* ID of the server requesting the vote. */
	raft_index last_log_index; /* Index of candidate's last log entry. */
	raft_index last_log_term;  /* Term of log entry at last_log_index. */
	bool disrupt_leader; /* True if current leader should be discarded. */
	bool pre_vote;       /* True if this is a pre-vote request. */
};
#define RAFT_REQUEST_VOTE_VERSION 2

/**
 * Hold the result of a RequestVote RPC.
 */
struct raft_request_vote_result
{
	int version;
	raft_term
	    term; /* Receiver's current term (candidate updates itself). */
	bool vote_granted; /* True means candidate received vote. */
	bool pre_vote;     /* The response to a pre-vote RequestVote or not. */
};
#define RAFT_REQUEST_VOTE_RESULT_VERSION 2

/**
 * Hold the arguments of an AppendEntries RPC.
 *
 * The AppendEntries RPC is invoked by the leader to replicate log entries. It's
 * also used as heartbeat (figure 3.1).
 */
struct raft_append_entries
{
	int version;
	raft_term term;            /* Leader's term. */
	raft_index prev_log_index; /* Index of log entry preceeding new ones. */
	raft_term prev_log_term;   /* Term of entry at prev_log_index. */
	raft_index leader_commit;  /* Leader's commit index. */
	struct raft_entry *entries; /* Log entries to append. */
	unsigned n_entries;         /* Size of the log entries array. */
};
#define RAFT_APPEND_ENTRIES_VERSION 0

/**
 * Hold the result of an AppendEntries RPC (figure 3.1).
 */
struct raft_append_entries_result
{
	int version;
	raft_term term;      /* Receiver's current_term. */
	raft_index rejected; /* If non-zero, the index that was rejected. */
	raft_index
	    last_log_index;  /* Receiver's last log entry index, as hint. */
	raft_flags features; /* Feature flags. */
};
#define RAFT_APPEND_ENTRIES_RESULT_VERSION 1

typedef uint32_t checksum_t;
typedef uint32_t pageno_t;

struct page_checksum {
	pageno_t   page_no;
	checksum_t checksum;
};

/* page range [from, to], with to included */
struct page_from_to {
	pageno_t from;
	pageno_t to;
};

enum raft_result {
	RAFT_RESULT_OK = 0,
	RAFT_RESULT_UNEXPECTED = 1,
	RAFT_RESULT_DONE = 2,
};

/**
 * Hold the arguments of an InstallSnapshot RPC (figure 5.3).
 */
struct raft_install_snapshot
{
	int version;
	raft_term term;        /* Leader's term. */
	raft_index last_index; /* Index of last entry in the snapshot. */
	raft_term last_term;   /* Term of last_index. */
	struct raft_configuration conf; /* Config as of last_index. */
	raft_index conf_index;          /* Commit index of conf. */
	struct raft_buffer data;        /* Raw snapshot data. */
	enum raft_result result;
};
#define RAFT_INSTALL_SNAPSHOT_VERSION 0

struct raft_install_snapshot_result {
	int version;

	enum raft_result result;
};
#define RAFT_INSTALL_SNAPSHOT_RESULT_VERSION 0

struct raft_signature {
	int version;

	const char *db;
	struct page_from_to page_from_to;
	pageno_t cs_page_no;
	enum raft_result result;
	bool ask_calculated;
};
#define RAFT_SIGNATURE_VERSION 0

struct raft_signature_result {
	int version;

	const char *db;
	struct page_checksum *cs;
	unsigned int cs_nr;
	pageno_t cs_page_no;
	enum raft_result result;
	bool calculated;
};
#define RAFT_SIGNATURE_RESULT_VERSION 0

struct raft_install_snapshot_mv {
	int version;

	const char *db;
	struct page_from_to *mv;
	unsigned int mv_nr;
	enum raft_result result;
};
#define RAFT_INSTALL_SNAPSHOT_MV_VERSION 0

struct raft_install_snapshot_mv_result {
	int version;

	const char *db;
	pageno_t last_known_page_no; /* used for retries and message losses */
	enum raft_result result;
};
#define RAFT_INSTALL_SNAPSHOT_MV_RESULT_VERSION 0

struct raft_install_snapshot_cp {
	int version;

	const char *db;
	pageno_t page_no;
	struct raft_buffer page_data;
	enum raft_result result;
};
#define RAFT_INSTALL_SNAPSHOT_CP_VERSION 0

struct raft_install_snapshot_cp_result {
	int version;

	pageno_t last_known_page_no; /* used for retries and message losses */
	enum raft_result result;
};
#define RAFT_INSTALL_SNAPSHOT_CP_RESULT_VERSION 0

/**
 * Hold the arguments of a TimeoutNow RPC.
 *
 * The TimeoutNow RPC is invoked by leaders to transfer leadership to a
 * follower.
 */
struct raft_timeout_now
{
	int version;
	raft_term term;            /* Leader's term. */
	raft_index last_log_index; /* Index of leader's last log entry. */
	raft_index last_log_term;  /* Term of log entry at last_log_index. */
};
#define RAFT_TIMEOUT_NOW_VERSION 0

/**
 * Type codes for RPC messages.
 */
enum {
	RAFT_IO_APPEND_ENTRIES = 1,
	RAFT_IO_APPEND_ENTRIES_RESULT,
	RAFT_IO_REQUEST_VOTE,
	RAFT_IO_REQUEST_VOTE_RESULT,
	RAFT_IO_INSTALL_SNAPSHOT,
	RAFT_IO_TIMEOUT_NOW,
	RAFT_IO_SIGNATURE,
	RAFT_IO_SIGNATURE_RESULT,
	RAFT_IO_INSTALL_SNAPSHOT_RESULT,
	RAFT_IO_INSTALL_SNAPSHOT_MV,
	RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT,
	RAFT_IO_INSTALL_SNAPSHOT_CP,
	RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT,
};

/**
 * A single RPC message that can be sent or received over the network.
 *
 * The RPC message types all have a `version` field.
 * In the libuv io implementation, `version` is filled out during decoding
 * and is based on the size of the message on the wire, see e.g.
 * `sizeofRequestVoteV1`. The version number in the RAFT_MESSAGE_XXX_VERSION
 * macro needs to be bumped every time the message is updated.
 *
 * Notes when adding a new message type to raft:
 * raft_io implementations compiled against old versions of raft don't know the
 * new message type and possibly have not allocated enough space for it. When
 * such an application receives a new message over the wire, the raft_io
 * implementation will err out or drop the message, because it doesn't know how
 * to decode it based on its type.
 * raft_io implementations compiled against versions of raft that know the new
 * message type but at runtime are linked against an older raft lib, will pass
 * the message to raft, where raft will drop it.
 * When raft receives a message and accesses a field of a new message type,
 * the raft_io implementation must have known about the new message type,
 * so it was compiled against a modern enough version of raft, and memory
 * accesses should be safe.
 *
 * Sending a new message type with a raft_io implementation that doesn't know
 * the type is safe, the implementation should drop the message based on its
 * type and will not try to access fields it doesn't know the existence of.
 */
struct raft_message
{
	unsigned short type; /* RPC type code. */
	raft_id server_id;   /* ID of sending or destination server. */
	const char
	    *server_address; /* Address of sending or destination server. */
	union {              /* Type-specific data */
		struct raft_request_vote request_vote;
		struct raft_request_vote_result request_vote_result;
		struct raft_append_entries append_entries;
		struct raft_append_entries_result append_entries_result;
		struct raft_install_snapshot install_snapshot;
		struct raft_install_snapshot_result install_snapshot_result;
		struct raft_signature signature;
		struct raft_signature_result signature_result;
		struct raft_install_snapshot_cp install_snapshot_cp;
		struct raft_install_snapshot_cp_result install_snapshot_cp_result;
		struct raft_install_snapshot_mv install_snapshot_mv;
		struct raft_install_snapshot_mv_result install_snapshot_mv_result;
		struct raft_timeout_now timeout_now;
	};
};

/**
 * Hold the details of a snapshot.
 * The user-provided raft_buffer structs should provide the user with enough
 * flexibility to adapt/evolve snapshot formats.
 * If this struct would NEED to be adapted in the future, raft can always move
 * to a new struct with a new name and a new raft_io version.
 */
struct raft_snapshot
{
	/* Index and term of last entry included in the snapshot. */
	raft_index index;
	raft_term term;

	/* Last committed configuration included in the snapshot, along with the
	 * index it was committed at. */
	struct raft_configuration configuration;
	raft_index configuration_index;

	/* Content of the snapshot. When a snapshot is taken, the user FSM can
	 * fill the bufs array with more than one buffer. When a snapshot is
	 * restored, there will always be a single buffer. */
	struct raft_buffer *bufs;
	unsigned n_bufs;
};

/**
 * Asynchronous request to send an RPC message.
 */
struct raft_io_send;
typedef void (*raft_io_send_cb)(struct raft_io_send *req, int status);
struct raft_io_send
{
	void *data;         /* User data */
	raft_io_send_cb cb; /* Request callback */
};

/**
 * Asynchronous request to store new log entries.
 */
struct raft_io_append;
typedef void (*raft_io_append_cb)(struct raft_io_append *req, int status);
struct raft_io_append
{
	void *data;           /* User data */
	raft_io_append_cb cb; /* Request callback */
};

/**
 * Asynchronous request to store a new snapshot.
 */
struct raft_io_snapshot_put;
typedef void (*raft_io_snapshot_put_cb)(struct raft_io_snapshot_put *req,
					int status);
struct raft_io_snapshot_put
{
	void *data;                 /* User data */
	raft_io_snapshot_put_cb cb; /* Request callback */
};

/**
 * Asynchronous request to load the most recent snapshot available.
 */
struct raft_io_snapshot_get;
typedef void (*raft_io_snapshot_get_cb)(struct raft_io_snapshot_get *req,
					struct raft_snapshot *snapshot,
					int status);
struct raft_io_snapshot_get
{
	void *data;                 /* User data */
	raft_io_snapshot_get_cb cb; /* Request callback */
};

/**
 * Asynchronous work request.
 */
struct raft_io_async_work;
typedef int (*raft_io_async_work_fn)(struct raft_io_async_work *req);
typedef void (*raft_io_async_work_cb)(struct raft_io_async_work *req,
				      int status);
struct raft_io_async_work
{
	void *data; /* User data */
	raft_io_async_work_fn
	    work;                 /* Function to run async from the main loop */
	raft_io_async_work_cb cb; /* Request callback */
};

/**
 * Customizable tracer, for debugging purposes.
 */
struct raft_tracer
{
	/**
	 * Implementation-defined state object.
	 */
	void *impl;

	/**
	 * Whether this tracer should emit messages.
	 */
	bool enabled;

	/**
	 * Trace level.
	 */
	unsigned level;

	/**
	 * Emit the given trace message, possibly decorating it with the
	 * provided metadata.
	 */
	void (*emit)(struct raft_tracer *t,
		     const char *file,
		     unsigned int line,
		     const char *func,
		     unsigned int level,
		     const char *message);
};

struct raft_io; /* Forward declaration. */

/**
 * Callback invoked by the I/O implementation at regular intervals.
 */
typedef void (*raft_io_tick_cb)(struct raft_io *io);

/**
 * Callback invoked by the I/O implementation when an RPC message is received.
 */
typedef void (*raft_io_recv_cb)(struct raft_io *io, struct raft_message *msg);

typedef void (*raft_io_close_cb)(struct raft_io *io);

/**
 * version field MUST be filled out by user.
 * When moving to a new version, the user MUST implement the newly added
 * methods.
 */
struct raft_io
{
	int version; /* 1 or 2 */
	void *data;
	void *impl;
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int (*init)(struct raft_io *io, raft_id id, const char *address);
	void (*close)(struct raft_io *io, raft_io_close_cb cb);
	int (*load)(struct raft_io *io,
		    raft_term *term,
		    raft_id *voted_for,
		    struct raft_snapshot **snapshot,
		    raft_index *start_index,
		    struct raft_entry *entries[],
		    size_t *n_entries);
	int (*start)(struct raft_io *io,
		     unsigned msecs,
		     raft_io_tick_cb tick,
		     raft_io_recv_cb recv);
	int (*bootstrap)(struct raft_io *io,
			 const struct raft_configuration *conf);
	int (*recover)(struct raft_io *io,
		       const struct raft_configuration *conf);
	int (*set_term)(struct raft_io *io, raft_term term);
	int (*set_vote)(struct raft_io *io, raft_id server_id);
	int (*send)(struct raft_io *io,
		    struct raft_io_send *req,
		    const struct raft_message *message,
		    raft_io_send_cb cb);
	int (*append)(struct raft_io *io,
		      struct raft_io_append *req,
		      const struct raft_entry entries[],
		      unsigned n,
		      raft_io_append_cb cb);
	int (*truncate)(struct raft_io *io, raft_index index);
	int (*snapshot_put)(struct raft_io *io,
			    unsigned trailing,
			    struct raft_io_snapshot_put *req,
			    const struct raft_snapshot *snapshot,
			    raft_io_snapshot_put_cb cb);
	int (*snapshot_get)(struct raft_io *io,
			    struct raft_io_snapshot_get *req,
			    raft_io_snapshot_get_cb cb);
	raft_time (*time)(struct raft_io *io);
	int (*random)(struct raft_io *io, int min, int max);
	/* Field(s) below added since version 2. */
	int (*async_work)(struct raft_io *io,
			  struct raft_io_async_work *req,
			  raft_io_async_work_cb cb);
};

/**
 * version field MUST be filled out by user.
 * When moving to a new version, the user MUST initialize the new methods,
 * either with an implementation or with NULL.
 *
 * version 2:
 * introduces `snapshot_finalize`, when this method is not NULL, it will
 * always run after a successful call to `snapshot`, whether the snapshot has
 * been successfully written to disk or not. If it is set, raft will
 * assume no ownership of any of the `raft_buffer`s and the responsibility to
 * clean up lies with the user of raft.
 * `snapshot_finalize` can be used to e.g. release a lock that was taken during
 * a call to `snapshot`. Until `snapshot_finalize` is called, raft can access
 * the data contained in the `raft_buffer`s.
 *
 * version 3:
 * Adds support for async snapshots through the `snapshot_async` function.
 * When this method is provided, raft will call `snapshot` in the main loop,
 * and when successful, will call `snapshot_async` using the `io->async_work`
 * method, so blocking I/O calls are allowed in the implementation. After the
 * `snapshot_async` completes, `snapshot_finalize` will be called in the main
 * loop, independent of the return value of `snapshot_async`.
 * An implementation that does not use asynchronous snapshots MUST set
 * `snapshot_async` to NULL.
 * All memory allocated by the snapshot routines MUST be freed by the snapshot
 * routines themselves.
 */

struct raft_fsm
{
	int version; /* 1, 2 or 3 */
	void *data;
	int (*apply)(struct raft_fsm *fsm,
		     const struct raft_buffer *buf,
		     void **result);
	int (*snapshot)(struct raft_fsm *fsm,
			struct raft_buffer *bufs[],
			unsigned *n_bufs);
	int (*restore)(struct raft_fsm *fsm, struct raft_buffer *buf);
	/* Fields below added since version 2. */
	int (*snapshot_finalize)(struct raft_fsm *fsm,
				 struct raft_buffer *bufs[],
				 unsigned *n_bufs);
	/* Fields below added since version 3. */
	int (*snapshot_async)(struct raft_fsm *fsm,
			      struct raft_buffer *bufs[],
			      unsigned *n_bufs);
};

struct raft; /* Forward declaration. */

/**
 * State codes.
 */
enum { RAFT_UNAVAILABLE, RAFT_FOLLOWER, RAFT_CANDIDATE, RAFT_LEADER };

/**
 * State callback to invoke if raft's state changes.
 */
typedef void (*raft_state_cb)(struct raft *raft,
			      unsigned short old_state,
			      unsigned short new_state);

struct raft_progress;

/**
 * Close callback.
 *
 * It's safe to release the memory of a raft instance only after this callback
 * has fired.
 */
typedef void (*raft_close_cb)(struct raft *raft);

struct raft_change;   /* Forward declaration */
struct raft_transfer; /* Forward declaration */

struct raft_log;

/**
 * Hold and drive the state of a single raft server in a cluster.
 * When replacing reserved fields in the middle of this struct, you MUST use a
 * type with the same size and alignment requirements as the original type.
 */
struct raft
{
	void *data;                 /* Custom user data. */
	struct raft_tracer *tracer; /* Tracer implementation. */
	struct raft_io *io;         /* Disk and network I/O implementation. */
	struct raft_fsm *fsm;       /* User-defined FSM to apply commands to. */
	raft_id id;                 /* Server ID of this raft instance. */
	char *address;              /* Server address of this raft instance. */

	/*
	 * Cache of the server's persistent state, updated on stable storage
	 * before responding to RPCs (Figure 3.1).
	 */
	raft_term current_term; /* Latest term server has seen. */
	raft_id voted_for; /* Candidate that received vote in current term. */
	struct raft_log *log; /* Log entries. */

	/*
	 * Current membership configuration (Chapter 4).
	 *
	 * At any given moment the current configuration can be committed or
	 * uncommitted.
	 *
	 * If a server is voting, the log entry with index 1 must always contain
	 * the first committed configuration.
	 *
	 * At all times #configuration_committed_index is either zero or is the
	 * index of the most recent log entry of type #RAFT_CHANGE that we know
	 * to be committed. That means #configuration_committed_index is always
	 * equal or lower than #commit_index.
	 *
	 * At all times #configuration_uncommitted_index is either zero or is
	 * the index of an uncommitted log entry of type #RAFT_CHANGE. There can
	 * be at most one uncommitted entry of type #RAFT_CHANGE because we
	 * allow only one configuration change at a time.
	 *
	 * At all times #configuration_last_snapshot is a copy of the
	 * configuration contained the most recent snapshot, if any.
	 *
	 * The possible scenarios are:
	 *
	 * 1. #configuration_committed_index and
	 * #configuration_uncommitted_index are both zero. This should only
	 * happen when a brand new server starts joining a cluster and is
	 * waiting to receive log entries from the current leader. In this case
	 * #configuration and #configuration_last_snapshot must be empty and
	 * have no servers.
	 *
	 * 2. #configuration_committed_index is non-zero and
	 *    #configuration_uncommitted_index is zero. This means that
	 *    #configuration is committed and there is no pending configuration
	 *    change. The content of #configuration must match the one of the
	 * log entry at #configuration_committed_index.
	 *
	 * 3. #configuration_committed_index and
	 * #configuration_uncommitted_index are both non-zero, with the latter
	 * being greater than the former. This means that #configuration is
	 * uncommitted and represents a pending configuration change. The
	 * content of #configuration must match the one of the log entry at
	 * #configuration_uncommitted_index.
	 *
	 * When a snapshot is taken, a copy of the most recent configuration
	 * known to be committed (i.e. the configuration contained in the log
	 * entry at #configuration_committed_index) is saved in
	 * #configuration_last_snapshot, so it can be easily retrieved in case
	 * the log gets truncated because of compaction and does not contain the
	 * entry at #configuration_committed_index anymore. Likewise, if a
	 * snapshot is restored its associated configuration is saved in
	 * #configuration_last_snapshot.
	 */
	struct raft_configuration configuration;
	struct raft_configuration configuration_last_snapshot;
	raft_index configuration_committed_index;
	raft_index configuration_uncommitted_index;

	/*
	 * Election timeout in milliseconds (default 1000).
	 *
	 * From 3.4:
	 *
	 *   Raft uses a heartbeat mechanism to trigger leader election. When
	 *   servers start up, they begin as followers. A server remains in
	 * follower state as long as it receives valid RPCs from a leader or
	 *   candidate. Leaders send periodic heartbeats (AppendEntries RPCs
	 * that carry no log entries) to all followers in order to maintain
	 * their authority. If a follower receives no communication over a
	 * period of time called the election timeout, then it assumes there is
	 * no viable leader and begins an election to choose a new leader.
	 *
	 * This is the baseline value and will be randomized between 1x and 2x.
	 *
	 * See raft_change_election_timeout() to customize the value of this
	 * attribute.
	 */
	unsigned election_timeout;

	/*
	 * Heartbeat timeout in milliseconds (default 100). This is relevant
	 * only for when the raft instance is in leader state: empty
	 * AppendEntries RPCs will be sent if this amount of milliseconds
	 * elapses without any user-triggered AppendEntries RCPs being sent.
	 *
	 * From Figure 3.1:
	 *
	 *   [Leaders] Send empty AppendEntries RPC during idle periods to
	 * prevent election timeouts.
	 */
	unsigned heartbeat_timeout;

	/*
	 * When the leader sends an InstallSnapshot RPC to a follower it will
	 * consider the RPC as failed after this timeout and retry.
	 */
	unsigned install_snapshot_timeout;

	/*
	 * The fields below hold the part of the server's volatile state which
	 * is always applicable regardless of the whether the server is
	 * follower, candidate or leader (Figure 3.1). This state is rebuilt
	 * automatically after a server restart.
	 */
	raft_index commit_index; /* Highest log entry known to be committed */
	raft_index last_applied; /* Highest log entry applied to the FSM */
	raft_index last_stored;  /* Highest log entry persisted on disk */

	/*
	 * Current server state of this raft instance, along with a union
	 * defining state-specific values.
	 */
	unsigned short state;
	union {
		struct /* Follower */
		{
			unsigned
			    randomized_election_timeout; /* Timer expiration. */
			struct /* Current leader info. */
			{
				raft_id id;
				char *address;
			} current_leader;
			uint64_t append_in_flight_count;
			uint64_t reserved[7]; /* Future use */
		} follower_state;
		struct
		{
			unsigned
			    randomized_election_timeout; /* Timer expiration. */
			bool *votes;                     /* Vote results. */
			bool disrupt_leader;  /* For leadership transfer */
			bool in_pre_vote;     /* True in pre-vote phase. */
			uint64_t reserved[8]; /* Future use */
		} candidate_state;
		struct
		{
			struct raft_progress
			    *progress; /* Per-server replication state. */
			struct raft_change
			    *change;         /* Pending membership change. */
			raft_id promotee_id; /* ID of server being promoted. */
			unsigned short round_number; /* Current sync round. */
			raft_index
			    round_index; /* Target of the current round. */
			raft_time round_start; /* Start of current round. */
			queue requests; /* Outstanding client requests. */
			uint32_t
			    voter_contacts; /* Current number of voting nodes we
					       are in contact with */
			uint32_t reserved2; /* Future use */
			uint64_t reserved[7]; /* Future use */
		} leader_state;
	};

	/* Election timer start.
	 *
	 * This timer has different purposes depending on the state. Followers
	 * convert to candidate after the randomized election timeout has
	 * elapsed without leader contact. Candidates start a new election after
	 * the randomized election timeout has elapsed without a winner. Leaders
	 * step down after the election timeout has elapsed without contacting a
	 * majority of voting servers. */
	raft_time election_timer_start;

	/* In-progress leadership transfer request, if any. */
	struct raft_transfer *transfer;

	/*
	 * Information about the last snapshot that was taken (if any).
	 */
	struct
	{
		unsigned threshold; /* N. of entries before snapshot */
		unsigned trailing;  /* N. of trailing entries to retain */
		struct raft_snapshot pending;    /* In progress snapshot */
		struct raft_io_snapshot_put put; /* Store snapshot request */
		uint64_t reserved[8];            /* Future use */
	} snapshot;

	/*
	 * Callback to invoke once a close request has completed.
	 */
	raft_close_cb close_cb;

	/*
	 * Human-readable message providing diagnostic information about the
	 * last error occurred.
	 */
	char errmsg[RAFT_ERRMSG_BUF_SIZE];

	/* Whether to use pre-vote to avoid disconnected servers disrupting the
	 * current leader, as described in 4.2.3 and 9.6. */
	bool pre_vote;

	/* Limit how long to wait for a stand-by to catch-up with the log when
	 * its being promoted to voter. */
	unsigned max_catch_up_rounds;
	unsigned max_catch_up_round_duration;

	/* uint64_t because we used a reserved field. In reality this a pointer
	 * to a `struct raft_callbacks` that can be used to store e.g. various
	 * user-supplied callbacks. */
	uint64_t callbacks;

	/* Future extensions */
	uint64_t reserved[31];
};

RAFT_API int raft_init(struct raft *r,
		       struct raft_io *io,
		       struct raft_fsm *fsm,
		       raft_id id,
		       const char *address);

RAFT_API void raft_close(struct raft *r, raft_close_cb cb);

/**
 * This function MUST be called after raft_init and before raft_start.
 * @cb will be called every time the raft state changes.
 */
RAFT_API void raft_register_state_cb(struct raft *r, raft_state_cb cb);

/**
 * Bootstrap this raft instance using the given configuration. The instance must
 * not have been started yet and must be completely pristine, otherwise
 * #RAFT_CANTBOOTSTRAP will be returned.
 */
RAFT_API int raft_bootstrap(struct raft *r,
			    const struct raft_configuration *conf);

/**
 * Force a new configuration in order to recover from a loss of quorum where the
 * current configuration cannot be restored, such as when a majority of servers
 * die at the same time.
 *
 * This works by appending the new configuration directly to the log stored on
 * disk.
 *
 * In order for this operation to be safe you must follow these steps:
 *
 * 1. Make sure that no servers in the cluster are running, either because they
 *    died or because you manually stopped them.
 *
 * 2. Run @raft_recover exactly one time, on the non-dead server which has
 *    the highest term and the longest log.
 *
 * 3. Copy the data directory of the server you ran @raft_recover on to all
 *    other non-dead servers in the cluster, replacing their current data
 *    directory.
 *
 * 4. Restart all servers.
 */
RAFT_API int raft_recover(struct raft *r,
			  const struct raft_configuration *conf);

RAFT_API int raft_start(struct raft *r);

/**
 * Set the election timeout.
 *
 * Every raft instance is initialized with a default election timeout of 1000
 * milliseconds. If you wish to tweak it, call this function before starting
 * your event loop.
 *
 * From Chapter 9:
 *
 *   We recommend a range that is 10-20 times the one-way network latency, which
 *   keeps split votes rates under 40% in all cases for reasonably sized
 *   clusters, and typically results in much lower rates.
 *
 * Note that the current random election timer will be reset and a new one timer
 * will be generated.
 */
RAFT_API void raft_set_election_timeout(struct raft *r, unsigned msecs);

/**
 * Set the heartbeat timeout.
 */
RAFT_API void raft_set_heartbeat_timeout(struct raft *r, unsigned msecs);

/**
 * Set the snapshot install timeout.
 */
RAFT_API void raft_set_install_snapshot_timeout(struct raft *r, unsigned msecs);

/**
 * Number of outstanding log entries before starting a new snapshot. The default
 * is 1024.
 */
RAFT_API void raft_set_snapshot_threshold(struct raft *r, unsigned n);

/**
 * Enable or disable pre-vote support. Pre-vote is turned off by default.
 */
RAFT_API void raft_set_pre_vote(struct raft *r, bool enabled);

/**
 * Number of outstanding log entries to keep in the log after a snapshot has
 * been taken. This avoids sending snapshots when a follower is behind by just a
 * few entries. The default is 128.
 */
RAFT_API void raft_set_snapshot_trailing(struct raft *r, unsigned n);

/**
 * Set the maximum number of a catch-up rounds to try when replicating entries
 * to a stand-by server that is being promoted to voter, before giving up and
 * failing the configuration change. The default is 10.
 */
RAFT_API void raft_set_max_catch_up_rounds(struct raft *r, unsigned n);

/**
 * Set the maximum duration of a catch-up round when replicating entries to a
 * stand-by server that is being promoted to voter. The default is 5 seconds.
 */
RAFT_API void raft_set_max_catch_up_round_duration(struct raft *r,
						   unsigned msecs);

/**
 * Return a human-readable description of the last error occurred.
 */
RAFT_API const char *raft_errmsg(struct raft *r);

/**
 * Return the code of the current raft state (follower/candidate/leader).
 */
RAFT_API int raft_state(struct raft *r);

/**
 * Return the code of the current raft role (spare/standby/voter),
 * or -1 if this server is not in the current configuration.
 */
RAFT_API int raft_role(struct raft *r);

/**
 * Return the ID and address of the current known leader, if any.
 */
RAFT_API void raft_leader(struct raft *r, raft_id *id, const char **address);

/**
 * Return the index of the last entry that was appended to the local log.
 */
RAFT_API raft_index raft_last_index(struct raft *r);

/**
 * Return the index of the last entry that was applied to the local FSM.
 */
RAFT_API raft_index raft_last_applied(struct raft *r);

/**
 * Return the number of voting servers that the leader has recently been in
 * contact with. This can be used to help determine whether the cluster may be
 * in a degraded/at risk state.
 *
 * Returns valid values >= 1, because a leader is always in contact with
 * itself.
 * Returns -1 if called on a follower.
 *
 * Note that the value returned may be out of date, and so should not be relied
 * upon for absolute correctness.
 */
RAFT_API int raft_voter_contacts(struct raft *r);

/**
 * Common fields across client request types.
 * `req_id`, `client_id` and `unique_id` are currently unused.
 * `reserved` fields should be replaced by new members with the same size
 * and alignment requirements as `uint64_t`.
 */
#define RAFT__REQUEST          \
	void *data;            \
	int type;              \
	raft_index index;      \
	queue queue;           \
	uint8_t req_id[16];    \
	uint8_t client_id[16]; \
	uint8_t unique_id[16]; \
	uint64_t reserved[4]

/**
 * Asynchronous request to append a new command entry to the log and apply it to
 * the FSM when a quorum is reached.
 */
struct raft_apply;
typedef void (*raft_apply_cb)(struct raft_apply *req, int status, void *result);
struct raft_apply
{
	RAFT__REQUEST;
	raft_apply_cb cb;
};

/**
 * Propose to append commands to the log and apply them to the FSM once
 * committed.
 *
 * If this server is the leader, it will create @n new log entries of type
 * #RAFT_COMMAND using the given buffers as their payloads, append them to its
 * own log and attempt to replicate them on other servers by sending
 * AppendEntries RPCs.
 *
 * The memory pointed at by the @base attribute of each #raft_buffer in the
 * given array must have been allocated with raft_malloc() or a compatible
 * allocator. If this function returns 0, the ownership of this memory is
 * implicitly transferred to the raft library, which will take care of releasing
 * it when appropriate. Any further client access to such memory leads to
 * undefined behavior.
 *
 * The ownership of the memory of the @bufs array itself is not transferred to
 * the raft library, and, if allocated dynamically, must be deallocated by the
 * caller.
 *
 * If the command was successfully applied, r->last_applied will be equal to
 * the log entry index of the applied command when the cb is invoked.
 */
RAFT_API int raft_apply(struct raft *r,
			struct raft_apply *req,
			const struct raft_buffer bufs[],
			const struct raft_entry_local_data local_data[],
			const unsigned n,
			raft_apply_cb cb);

/**
 * Asynchronous request to append a barrier entry.
 */
struct raft_barrier;
typedef void (*raft_barrier_cb)(struct raft_barrier *req, int status);
struct raft_barrier
{
	RAFT__REQUEST;
	raft_barrier_cb cb;
};

/**
 * Propose to append a log entry of type #RAFT_BARRIER.
 *
 * This can be used to ensure that there are no unapplied commands.
 */
RAFT_API int raft_barrier(struct raft *r,
			  struct raft_barrier *req,
			  raft_barrier_cb cb);

/**
 * Asynchronous request to change the raft configuration.
 */
typedef void (*raft_change_cb)(struct raft_change *req, int status);
struct raft_change
{
	RAFT__REQUEST;
	raft_change_cb cb;
};

/**
 * Add a new server to the cluster configuration. Its initial role will be
 * #RAFT_SPARE.
 */
RAFT_API int raft_add(struct raft *r,
		      struct raft_change *req,
		      raft_id id,
		      const char *address,
		      raft_change_cb cb);

/**
 * Assign a new role to the given server.
 *
 * If the server has already the given role, or if the given role is unknown,
 * #RAFT_BADROLE is returned.
 */
RAFT_API int raft_assign(struct raft *r,
			 struct raft_change *req,
			 raft_id id,
			 int role,
			 raft_change_cb cb);

/**
 * Remove the given server from the cluster configuration.
 */
RAFT_API int raft_remove(struct raft *r,
			 struct raft_change *req,
			 raft_id id,
			 raft_change_cb cb);

/**
 * Asynchronous request to transfer leadership.
 */
typedef void (*raft_transfer_cb)(struct raft_transfer *req);
struct raft_transfer
{
	RAFT__REQUEST;
	raft_id id;               /* ID of target server. */
	raft_time start;          /* Start of leadership transfer. */
	struct raft_io_send send; /* For sending TimeoutNow */
	raft_transfer_cb cb;      /* User callback */
};

/**
 * Transfer leadership to the server with the given ID.
 *
 * If the target server is not part of the configuration, or it's the leader
 * itself, or it's not a #RAFT_VOTER, then #RAFT_BADID is returned.
 *
 * The special value #0 means to automatically select a voting follower to
 * transfer leadership to. If there are no voting followers, return
 * #RAFT_NOTFOUND.
 *
 * When this server detects that the target server has become the leader, or
 * when @election_timeout milliseconds have elapsed, the given callback will be
 * invoked.
 *
 * After the callback files, clients can check whether the operation was
 * successful or not by calling @raft_leader() and checking if it returns the
 * target server.
 */
RAFT_API int raft_transfer(struct raft *r,
			   struct raft_transfer *req,
			   raft_id id,
			   raft_transfer_cb cb);

/**
 * User-definable dynamic memory allocation functions.
 *
 * The @data field will be passed as first argument to all functions.
 */
struct raft_heap
{
	void *data; /* User data */
	void *(*malloc)(void *data, size_t size);
	void (*free)(void *data, void *ptr);
	void *(*calloc)(void *data, size_t nmemb, size_t size);
	void *(*realloc)(void *data, void *ptr, size_t size);
	void *(*aligned_alloc)(void *data, size_t alignment, size_t size);
	void (*aligned_free)(void *data, size_t alignment, void *ptr);
};

DQLITE_VISIBLE_TO_TESTS void *raft_malloc(size_t size);
DQLITE_VISIBLE_TO_TESTS void raft_free(void *ptr);
DQLITE_VISIBLE_TO_TESTS void *raft_calloc(size_t nmemb, size_t size);
DQLITE_VISIBLE_TO_TESTS void *raft_realloc(void *ptr, size_t size);
DQLITE_VISIBLE_TO_TESTS void *raft_aligned_alloc(size_t alignment, size_t size);
DQLITE_VISIBLE_TO_TESTS void raft_aligned_free(size_t alignment, void *ptr);

/**
 * Use a custom dynamic memory allocator.
 */
DQLITE_VISIBLE_TO_TESTS void raft_heap_set(struct raft_heap *heap);

/**
 * Use the default dynamic memory allocator (from the stdlib). This clears any
 * custom allocator specified with @raft_heap_set.
 */
DQLITE_VISIBLE_TO_TESTS void raft_heap_set_default(void);

/**
 * Return a reference to the current dynamic memory allocator.
 *
 * This is intended for use by applications that want to temporarily replace
 * and then restore the original allocator, or that want to defer to the
 * original allocator in some circumstances.
 *
 * The behavior of attempting to mutate the default allocator through the
 * pointer returned by this function, including attempting to deallocate
 * the backing memory, is undefined.
 */
DQLITE_VISIBLE_TO_TESTS const struct raft_heap *raft_heap_get(void);

#undef RAFT__REQUEST

struct raft_uv_transport;

/**
 * Configure the given @raft_io instance to use a libuv-based I/O
 * implementation.
 *
 * The @dir path will be copied, and its memory can possibly be released once
 * this function returns.
 *
 * Return #RAFT_NAMETOOLONG if @dir exceeds the size of the internal buffer
 * that should hold it
 *
 * Return #RAFT_NOTFOUND if @dir does not exist.
 *
 * Return #RAFT_INVALID if @dir exists but it's not a directory.
 *
 * The implementation of metadata and log persistency is virtually the same as
 * the one found in LogCabin [0].
 *
 * The disk files consist of metadata files, closed segments, and open
 * segments. Metadata files are used to track Raft metadata, such as the
 * server's current term, vote, and log's start index. Segments contain
 * contiguous entries that are part of the log. Closed segments are never
 * written to again (but may be renamed and truncated if a suffix of the log is
 * truncated). Open segments are where newly appended entries go. Once an open
 * segment reaches the maximum allowed size, it is closed and a new one is used.
 *
 * Metadata files are named "metadata1" and "metadata2". The code alternates
 * between these so that there is always at least one readable metadata file.
 * On boot, the readable metadata file with the higher version number is used.
 *
 * The format of a metadata file is:
 *
 * [8 bytes] Format (currently 1).
 * [8 bytes] Incremental version number.
 * [8 bytes] Current term.
 * [8 bytes] ID of server we voted for.
 *
 * Closed segments are named by the format string "%lu-%lu" with their
 * start and end indexes, both inclusive. Closed segments always contain at
 * least one entry; the end index is always at least as large as the start
 * index. Closed segment files may occasionally include data past their
 * filename's end index (these are ignored but a warning is logged). This can
 * happen if the suffix of the segment is truncated and a crash occurs at an
 * inopportune time (the segment file is first renamed, then truncated, and a
 * crash occurs in between).
 *
 * Open segments are named by the format string "open-%lu" with a unique
 * number. These should not exist when the server shuts down cleanly, but they
 * exist while the server is running and may be left around during a crash.
 * Open segments either contain entries which come after the last closed
 * segment or are full of zeros. When the server crashes while appending to an
 * open segment, the end of that file may be corrupt. We can't distinguish
 * between a corrupt file and a partially written entry. The code assumes it's
 * a partially written entry, logs a warning, and ignores it.
 *
 * Truncating a suffix of the log will remove all entries that are no longer
 * part of the log. Truncating a prefix of the log will only remove complete
 * segments that are before the new log start index. For example, if a
 * segment has entries 10 through 20 and the prefix of the log is truncated to
 * start at entry 15, that entire segment will be retained.
 *
 * Each segment file starts with a segment header, which currently contains
 * just an 8-byte version number for the format of that segment. The current
 * format (version 1) is just a concatenation of serialized entry batches.
 *
 * Each batch has the following format:
 *
 * [4 bytes] CRC32 checksum of the batch header, little endian.
 * [4 bytes] CRC32 checksum of the batch data, little endian.
 * [  ...  ] Batch (as described in @raft_decode_entries_batch).
 *
 * [0] https://github.com/logcabin/logcabin/blob/master/Storage/SegmentedLog.h
 */
RAFT_API int raft_uv_init(struct raft_io *io,
			  struct uv_loop_s *loop,
			  const char *dir,
			  struct raft_uv_transport *transport);

/**
 * Release any memory allocated internally.
 */
RAFT_API void raft_uv_close(struct raft_io *io);

/**
 * Set the block size that will be used for direct I/O.
 *
 * The default is to automatically detect the appropriate block size.
 */
RAFT_API void raft_uv_set_block_size(struct raft_io *io, size_t size);

/**
 * Set the maximum initial size of newly created open segments.
 *
 * If the given size is not a multiple of the block size, the actual size will
 * be reduced to the closest multiple.
 *
 * The default is 8 megabytes.
 */
RAFT_API void raft_uv_set_segment_size(struct raft_io *io, size_t size);

/**
 * Turn snapshot compression on or off.
 * Returns non-0 on failure, this can e.g. happen when compression is requested
 * while no suitable compression library is found.
 *
 * By default snapshots are compressed if the appropriate libraries are found.
 */
RAFT_API int raft_uv_set_snapshot_compression(struct raft_io *io,
					      bool compressed);

/**
 * Set how many milliseconds to wait between subsequent retries when
 * establishing a connection with another server. The default is 1000
 * milliseconds.
 */
RAFT_API void raft_uv_set_connect_retry_delay(struct raft_io *io,
					      unsigned msecs);

/**
 * Emit low-level debug messages using the given tracer.
 */
RAFT_API void raft_uv_set_tracer(struct raft_io *io,
				 struct raft_tracer *tracer);

/**
 * Enable or disable auto-recovery on startup. Default enabled.
 */
RAFT_API void raft_uv_set_auto_recovery(struct raft_io *io, bool flag);

/**
 * Callback invoked by the transport implementation when a new incoming
 * connection has been established.
 *
 * No references to @address must be kept after this function returns.
 *
 * Ownership of @stream is transferred to user code, which is responsible of
 * uv_close()'ing it and then releasing its memory.
 */
typedef void (*raft_uv_accept_cb)(struct raft_uv_transport *t,
				  raft_id id,
				  const char *address,
				  struct uv_stream_s *stream);

/**
 * Callback invoked by the transport implementation after a connect request has
 * completed. If status is #0, then @stream will point to a valid handle, which
 * user code is then responsible to uv_close() and then release.
 */
struct raft_uv_connect;
typedef void (*raft_uv_connect_cb)(struct raft_uv_connect *req,
				   struct uv_stream_s *stream,
				   int status);

/**
 * Handle to a connect request.
 */
struct raft_uv_connect
{
	void *data;            /* User data */
	raft_uv_connect_cb cb; /* Callback */
};

/**
 * Callback invoked by the transport implementation after a close request is
 * completed.
 */
typedef void (*raft_uv_transport_close_cb)(struct raft_uv_transport *t);

/**
 * Interface to establish outgoing connections to other Raft servers and to
 * accept incoming connections from them.
 */

struct raft_uv_transport
{
	/**
	 * Keep track of struct version, MUST be filled out by user.
	 * When moving to a new version, the user MUST implement the newly added
	 * methods.
	 * Latest version is 1.
	 */
	int version;

	/**
	 * User defined data.
	 */
	void *data;

	/**
	 * Implementation-defined state.
	 */
	void *impl;

	/**
	 * Human-readable message providing diagnostic information about the
	 * last error occurred.
	 */
	char errmsg[RAFT_ERRMSG_BUF_SIZE];

	/**
	 * Initialize the transport with the given server's identity.
	 */
	int (*init)(struct raft_uv_transport *t,
		    raft_id id,
		    const char *address);

	/**
	 * Start listening for incoming connections.
	 *
	 * Once a new connection is accepted, the @cb callback passed in the
	 * initializer must be invoked with the relevant details of the
	 * connecting Raft server.
	 */
	int (*listen)(struct raft_uv_transport *t, raft_uv_accept_cb cb);

	/**
	 * Connect to the server with the given ID and address.
	 *
	 * The @cb callback must be invoked when the connection has been
	 * established or the connection attempt has failed. The memory pointed
	 * by @req can be released only after @cb has fired.
	 */
	int (*connect)(struct raft_uv_transport *t,
		       struct raft_uv_connect *req,
		       raft_id id,
		       const char *address,
		       raft_uv_connect_cb cb);

	/**
	 * Close the transport.
	 *
	 * The implementation must:
	 *
	 * - Stop accepting incoming connections. The @cb callback passed to
	 * @listen must not be invoked anymore.
	 *
	 * - Cancel all pending @connect requests.
	 *
	 * - Invoke the @cb callback passed to this method once it's safe to
	 * release the memory of the transport object.
	 */
	void (*close)(struct raft_uv_transport *t,
		      raft_uv_transport_close_cb cb);
};

/**
 * Init a transport interface that uses TCP sockets.
 */
RAFT_API int raft_uv_tcp_init(struct raft_uv_transport *t,
			      struct uv_loop_s *loop);

/**
 * Release any memory allocated internally.
 */
RAFT_API void raft_uv_tcp_close(struct raft_uv_transport *t);

/**
 * Set the IP address and port that the listening socket will bind to.
 *
 * By default the socket will bind to the address provided in
 * raft_init(), which may be inconvenient if running your application in a
 * container, for example.
 *
 * The @address argument must be an IPv4 dotted quad IP address and port, e.g.
 * "0.0.0.0:8080". If you do not provide a port, the default of 8080 will be
 * used. The port given here *must* match the port given to raft_init().
 *
 * Must be called before raft_init().
 */
RAFT_API int raft_uv_tcp_set_bind_address(struct raft_uv_transport *t,
					  const char *address);

/**
 * Raft cluster test fixture, using an in-memory @raft_io implementation. This
 * is meant to be used in unit tests.
 */

#define RAFT_FIXTURE_MAX_SERVERS 8

/**
 * Fixture step event types.
 */
enum {
	RAFT_FIXTURE_TICK = 1, /* The tick callback has been invoked */
	RAFT_FIXTURE_NETWORK,  /* A network request has been sent or received */
	RAFT_FIXTURE_DISK,     /* An I/O request has been submitted */
	RAFT_FIXTURE_WORK      /* A large, CPU and/or memory intensive task */
};

/**
 * State of a single server in a cluster fixture.
 */
struct raft_fixture_server;

/**
 * Information about a test cluster event triggered by the fixture.
 */
struct raft_fixture_event;

/**
 * Returns the type of the event.
 */
int raft_fixture_event_type(struct raft_fixture_event *event);

/**
 * Returns the server index of the event.
 */
unsigned raft_fixture_event_server_index(struct raft_fixture_event *event);

/**
 * Event callback. See raft_fixture_hook().
 */
struct raft_fixture;
typedef void (*raft_fixture_event_cb)(struct raft_fixture *f,
				      struct raft_fixture_event *event);

/**
 * Test implementation of a cluster of @n servers, each having a user-provided
 * FSM.
 *
 * The cluster can simulate network latency and time elapsed on individual
 * servers.
 *
 * Servers can be alive or dead. Network messages sent to dead servers are
 * dropped. Dead servers do not have their @raft_io_tick_cb callback invoked.
 *
 * Any two servers can be connected or disconnected. Network messages sent
 * between disconnected servers are dropped.
 */
struct raft_fixture
{
	raft_time time;          /* Global time, common to all servers. */
	unsigned n;              /* Number of servers. */
	raft_id leader_id;       /* ID of current leader, or 0 if none. */
	struct raft_log *log;    /* Copy of current leader's log. */
	raft_index commit_index; /* Current commit index on leader. */
	struct raft_fixture_event *event; /* Last event occurred. */
	raft_fixture_event_cb hook;       /* Event callback. */
	struct raft_fixture_server *servers[RAFT_FIXTURE_MAX_SERVERS];
	uint64_t reserved[16]; /* For future expansion of struct. */
};

/**
 * Initialize a raft cluster fixture. Servers can be added by using
 * `raft_fixture_grow`.
 */
RAFT_API int raft_fixture_init(struct raft_fixture *f);

/**
 * Release all memory used by the fixture.
 */
RAFT_API void raft_fixture_close(struct raft_fixture *f);

/**
 * Convenience to generate a configuration object containing all servers in the
 * cluster. The first @n_voting servers will be voting ones.
 */
RAFT_API int raft_fixture_configuration(struct raft_fixture *f,
					unsigned n_voting,
					struct raft_configuration *conf);

/**
 * Convenience to bootstrap all servers in the cluster using the given
 * configuration.
 */
RAFT_API int raft_fixture_bootstrap(struct raft_fixture *f,
				    struct raft_configuration *conf);

/**
 * Convenience to start all servers in the fixture.
 */
RAFT_API int raft_fixture_start(struct raft_fixture *f);

/**
 * Return the number of servers in the fixture.
 */
RAFT_API unsigned raft_fixture_n(struct raft_fixture *f);

/**
 * Return the current cluster global time. All raft instances see the same time.
 */
RAFT_API raft_time raft_fixture_time(struct raft_fixture *f);

/**
 * Return the raft instance associated with the @i'th server of the fixture.
 */
RAFT_API struct raft *raft_fixture_get(struct raft_fixture *f, unsigned i);

/**
 * Return @true if the @i'th server hasn't been killed.
 */
RAFT_API bool raft_fixture_alive(struct raft_fixture *f, unsigned i);

/**
 * Return the index of the current leader, or the current number of servers if
 * there's no leader.
 */
RAFT_API unsigned raft_fixture_leader_index(struct raft_fixture *f);

/**
 * Return the ID of the server the @i'th server has voted for, or zero .
 */
RAFT_API raft_id raft_fixture_voted_for(struct raft_fixture *f, unsigned i);

/**
 * Drive the cluster so the @i'th server starts an election but doesn't
 * necessarily win it.
 *
 * This is achieved by bumping the randomized election timeout of all other
 * servers to a very high value, letting the one of the @i'th server expire.
 *
 * There must currently be no leader and no candidate and the given server must
 * be a voting one. Also, the @i'th server must be connected to a majority of
 * voting servers.
 */
RAFT_API void raft_fixture_start_elect(struct raft_fixture *f, unsigned i);

/**
 * Calls raft_fixture_start_elect, but waits and asserts that the @i'th server
 * has become the leader.
 */
RAFT_API void raft_fixture_elect(struct raft_fixture *f, unsigned i);

/**
 * Drive the cluster so the current leader gets deposed.
 *
 * This is achieved by dropping all AppendEntries result messages sent by
 * followers to the leader, until the leader decides to step down because it has
 * lost connectivity to a majority of followers.
 */
RAFT_API void raft_fixture_depose(struct raft_fixture *f);

/**
 * Step through the cluster state advancing the time to the minimum value needed
 * for it to make progress (i.e. for a message to be delivered, for an I/O
 * operation to complete or for a single time tick to occur).
 *
 * In particular, the following happens:
 *
 * 1. If there are pending #raft_io_send requests, that have been submitted
 *    using #raft_io->send() and not yet sent, the oldest one is picked and the
 *    relevant callback fired. This simulates completion of a socket write,
 *    which means that the send request has been completed. The receiver does
 *    not immediately receives the message, as the message is propagating
 *    through the network. However any memory associated with the #raft_io_send
 *    request can be released (e.g. log entries). The in-memory I/O
 *    implementation assigns a latency to each RPC message, which will get
 *    delivered to the receiver only after that amount of time elapses. If the
 *    sender and the receiver are currently disconnected, the RPC message is
 *    simply dropped. If a callback was fired, jump directly to 3. and skip 2.
 *
 * 2. All pending #raft_io_append disk writes across all servers, that have been
 *    submitted using #raft_io->append() but not yet completed, are scanned and
 *    the one with the lowest completion time is picked. All in-flight network
 *    messages waiting to be delivered are scanned and the one with the lowest
 *    delivery time is picked. All servers are scanned, and the one with the
 *    lowest tick expiration time is picked. The three times are compared and
 *    the lowest one is picked. If a #raft_io_append disk write has completed,
 *    the relevant callback will be invoked, if there's a network message to be
 *    delivered, the receiver's @raft_io_recv_cb callback gets fired, if a tick
 *    timer has expired the relevant #raft_io->tick() callback will be
 *    invoked. Only one event will be fired. If there is more than one event to
 *    fire, one of them is picked according to the following rules: events for
 *    servers with lower index are fired first, tick events take precedence over
 *    disk events, and disk events take precedence over network events.
 *
 * 3. The current cluster leader is detected (if any). When detecting the leader
 *    the Election Safety property is checked: no servers can be in leader state
 *    for the same term. The server in leader state with the highest term is
 *    considered the current cluster leader, as long as it's "stable", i.e. it
 *    has been acknowledged by all servers connected to it, and those servers
 *    form a majority (this means that no further leader change can happen,
 *    unless the network gets disrupted). If there is a stable leader and it has
 *    not changed with respect to the previous call to @raft_fixture_step(),
 *    then the Leader Append-Only property is checked, by comparing its log with
 *    a copy of it that was taken during the previous iteration.
 *
 * 4. If there is a stable leader, its current log is copied, in order to be
 *    able to check the Leader Append-Only property at the next call.
 *
 * 5. If there is a stable leader, its commit index gets copied.
 *
 * The function returns information about which particular event occurred
 * (either in step 1 or 2).
 */
RAFT_API struct raft_fixture_event *raft_fixture_step(struct raft_fixture *f);

/**
 * Call raft_fixture_step() exactly @n times, and return the last event fired.
 */
RAFT_API struct raft_fixture_event *raft_fixture_step_n(struct raft_fixture *f,
							unsigned n);

/**
 * Step the cluster until the given @stop function returns #true, or @max_msecs
 * have elapsed.
 *
 * Return #true if the @stop function has returned #true within @max_msecs.
 */
RAFT_API bool raft_fixture_step_until(struct raft_fixture *f,
				      bool (*stop)(struct raft_fixture *f,
						   void *arg),
				      void *arg,
				      unsigned max_msecs);

/**
 * Step the cluster until @msecs have elapsed.
 */
RAFT_API void raft_fixture_step_until_elapsed(struct raft_fixture *f,
					      unsigned msecs);

/**
 * Step the cluster until a leader is elected, or @max_msecs have elapsed.
 */
RAFT_API bool raft_fixture_step_until_has_leader(struct raft_fixture *f,
						 unsigned max_msecs);

/**
 * Step the cluster until the current leader gets deposed, or @max_msecs have
 * elapsed.
 */
RAFT_API bool raft_fixture_step_until_has_no_leader(struct raft_fixture *f,
						    unsigned max_msecs);

/**
 * Step the cluster until the @i'th server has applied the entry at the given
 * index, or @max_msecs have elapsed. If @i equals the number of servers, then
 * step until all servers have applied the given entry.
 */
RAFT_API bool raft_fixture_step_until_applied(struct raft_fixture *f,
					      unsigned i,
					      raft_index index,
					      unsigned max_msecs);

/**
 * Step the cluster until the state of the @i'th server matches the given one,
 * or @max_msecs have elapsed.
 */
RAFT_API bool raft_fixture_step_until_state_is(struct raft_fixture *f,
					       unsigned i,
					       int state,
					       unsigned max_msecs);

/**
 * Step the cluster until the term of the @i'th server matches the given one,
 * or @max_msecs have elapsed.
 */
RAFT_API bool raft_fixture_step_until_term_is(struct raft_fixture *f,
					      unsigned i,
					      raft_term term,
					      unsigned max_msecs);

/**
 * Step the cluster until the @i'th server has voted for the @j'th one, or
 * @max_msecs have elapsed.
 */
RAFT_API bool raft_fixture_step_until_voted_for(struct raft_fixture *f,
						unsigned i,
						unsigned j,
						unsigned max_msecs);

/**
 * Step the cluster until all pending network messages from the @i'th server to
 * the @j'th server have been delivered, or @max_msecs have elapsed.
 */
RAFT_API bool raft_fixture_step_until_delivered(struct raft_fixture *f,
						unsigned i,
						unsigned j,
						unsigned max_msecs);

/**
 * Set a function to be called after every time a fixture event occurs as
 * consequence of a step.
 */
RAFT_API void raft_fixture_hook(struct raft_fixture *f,
				raft_fixture_event_cb hook);

/**
 * Disconnect the @i'th and the @j'th servers, so attempts to send a message
 * from @i to @j will fail with #RAFT_NOCONNECTION.
 */
RAFT_API void raft_fixture_disconnect(struct raft_fixture *f,
				      unsigned i,
				      unsigned j);

/**
 * Reconnect the @i'th and the @j'th servers, so attempts to send a message
 * from @i to @j will succeed again.
 */
RAFT_API void raft_fixture_reconnect(struct raft_fixture *f,
				     unsigned i,
				     unsigned j);

/**
 * Saturate the connection between the @i'th and the @j'th servers, so messages
 * sent by @i to @j will be silently dropped.
 */
RAFT_API void raft_fixture_saturate(struct raft_fixture *f,
				    unsigned i,
				    unsigned j);

/**
 * Return true if the connection from the @i'th to the @j'th server has been set
 * as saturated.
 */
RAFT_API bool raft_fixture_saturated(struct raft_fixture *f,
				     unsigned i,
				     unsigned j);

/**
 * Desaturate the connection between the @i'th and the @j'th servers, so
 * messages sent by @i to @j will start being delivered again.
 */
RAFT_API void raft_fixture_desaturate(struct raft_fixture *f,
				      unsigned i,
				      unsigned j);

/**
 * Kill the server with the given index. The server won't receive any message
 * and its tick callback won't be invoked.
 */
RAFT_API void raft_fixture_kill(struct raft_fixture *f, unsigned i);

/**
 * Revive a killed server with the given index.
 */
RAFT_API void raft_fixture_revive(struct raft_fixture *f, unsigned i);

/**
 * Add a new empty server to the cluster and connect it to all others.
 */
RAFT_API int raft_fixture_grow(struct raft_fixture *f, struct raft_fsm *fsm);

/**
 * Set the value that will be returned to the @i'th raft instance when it asks
 * the underlying #raft_io implementation for a randomized election timeout
 * value. The default value is 1000 + @i * 100, meaning that the election timer
 * of server 0 will expire first.
 */
RAFT_API void raft_fixture_set_randomized_election_timeout(
    struct raft_fixture *f,
    unsigned i,
    unsigned msecs);

/**
 * Set the network latency in milliseconds. Each RPC message sent by the @i'th
 * server from now on will take @msecs milliseconds to be delivered. The default
 * value is 15.
 */
RAFT_API void raft_fixture_set_network_latency(struct raft_fixture *f,
					       unsigned i,
					       unsigned msecs);

/**
 * Set the disk I/O latency in milliseconds. Each append request will take this
 * amount of milliseconds to complete. The default value is 10.
 */
RAFT_API void raft_fixture_set_disk_latency(struct raft_fixture *f,
					    unsigned i,
					    unsigned msecs);

/**
 * Send the send latency in milliseconds. Each message send will take this many
 * milliseconds before the send callback is invoked.
 */
RAFT_API void raft_fixture_set_send_latency(struct raft_fixture *f,
					    unsigned i,
					    unsigned j,
					    unsigned msecs);

/**
 * Set the persisted term of the @i'th server.
 */
RAFT_API void raft_fixture_set_term(struct raft_fixture *f,
				    unsigned i,
				    raft_term term);

/**
 * Set the most recent persisted snapshot on the @i'th server.
 */
RAFT_API void raft_fixture_set_snapshot(struct raft_fixture *f,
					unsigned i,
					struct raft_snapshot *snapshot);

/**
 * Add an entry to the persisted entries of the @i'th server.
 */
RAFT_API void raft_fixture_add_entry(struct raft_fixture *f,
				     unsigned i,
				     struct raft_entry *entry);

RAFT_API void raft_fixture_append_fault(struct raft_fixture *f,
					unsigned i,
					int delay);

RAFT_API void raft_fixture_vote_fault(struct raft_fixture *f,
				      unsigned i,
				      int delay);

RAFT_API void raft_fixture_term_fault(struct raft_fixture *f,
				      unsigned i,
				      int delay);

RAFT_API void raft_fixture_send_fault(struct raft_fixture *f,
				      unsigned i,
				      int delay);

/**
 * Return the number of messages of the given type that the @i'th server has
 * successfully sent so far.
 */
RAFT_API unsigned raft_fixture_n_send(struct raft_fixture *f,
				      unsigned i,
				      int type);

/**
 * Return the number of messages of the given type that the @i'th server has
 * received so far.
 */
RAFT_API unsigned raft_fixture_n_recv(struct raft_fixture *f,
				      unsigned i,
				      int type);

/**
 * Force the @i'th server into the UNAVAILABLE state.
 */
RAFT_API void raft_fixture_make_unavailable(struct raft_fixture *f, unsigned i);

#endif /* RAFT_H */
