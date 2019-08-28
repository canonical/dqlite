Wire protocol
=============

Definitions
-----------

The following terms are used in this document:

- **server**: A dqlite node.

- **client**: Either application code (typically wanting to issue database
  queries) or a dqlite node (typically pushing replicational data).

- **connection**: A TCP or Unix socket connection established by the client
  against a server.

- **word**: A sequence of 8 bytes.

- **protocol version**: A positive number stored in a word using little endian
  representation.

- **message**: A sequence of bytes sent either by the client to the server or by
  the server to the client. It consists of a header and a body. The header
  consists of a single word with the following layout:

  - byte 0 to 3: Size of the message body, expressed in words and stored using
    little endian representation. For example a value of [2 1 0 0] means that
    the message body consists of 258 bytes.

  - byte 4: Type code identifiying the schema of the message body.

  - byte 5: Revision number of the message body schema. Within the same protocol
    version, a new schema revision can only add fields to the previous
    revision. So a client capable of understanding up to revision N of a certain
    message schema can still handle messages with revision >N by simply ignoring
    the extra bytes.

  - byte 6 to 7: Currently unused.

  The message body is a sequence of fields as described by the associated
  schema. Message types and their schemas are listed below.

Setup
-----

As soon as a connection is established, the client must send to the server a
single word containing the protocol version it wishes to use.

Conversation
------------

After the setup, communication between client and server happens by message
exchange. Typically the client will send to the server a message containing a
request and the server will send to the client a message containing a response.

Data types
----------

Each field in a message body has a specific data type, as described in the
message schema. Available data types are:

### **uint64**

A single word containing an unsigned integer in little endian representation.

### **int64**

A single word containing a two-complement signed integer in little endian
representation.

### **uint32**

Four bytes containing an unsigned integer in little endian
  representation.

### **text**

A sequence of one or more words containing a UTF-8 encoded zero-terminated
string. All bytes past the terminating zero byte are zeroed as well.

### **tuple**

A tuple is composed by a header and a body.

The format of the header changes depending on whether the tuple is a sequence of
parameters to bind to a statement, or a sequence of values of a single row
yielded by a query.

For a tuple of parameters the format of the header is:

- 8 bits: Number of values in the tuple.
- 8 bits: Type code of the 1st value of the tuple.
- 8 bits: Type code of the 2nd value of the tuple, or 0.
- 8 bits: Type code of the 3rd value of the tuple, or 0.
- ...

This repeats until reaching a full 64-bit word. If there are more than 7
parameters to bind, the header will grow additional 64-bit words as needed,
following the same pattern: a sequence of 8-bit slots with type codes of the
parameters followed by a sequence of zero bits, until word boundary is reached.

For a tuple of row values the format of the header is:

- 4 bits: Type code of the 1st value of the tuple.
- 4 bits: Type code of the 2nd value of the tuple, or 0.
- 4 bits: Type code of the 3rd value of the tuple, or 0.
- ...

This repeats until reaching a full 64-bit word. If there are more than 16
values, the header will grow additional 64-bit words as needed, following the
same pattern: a sequence of 4-bit slots with type codes of the values followed
by a sequence of zero bits, until word boundary is reached.

After the header the body follows immediately, which contains all parameters or
values in sequence, encoded using type-specific rules.

The codes of available types for tuple values and the associated encoding
formats are:

- **1**: Integer value stored using the **int64** encoding.
- **2**: An IEEE 754 floating point number stored in a single word (little endian).
- **3**: A string value using the **text** encoding.
- **4**: A binary blob: the first word of the value is the length of the blob (little endian).
- **5**: A SQL NULL value encoded as a zeroed word.
- **10**: An ISO-8601 date value using the **text** encoding.
- **11**: A boolean value using **uint64** encoding (0 for false and 1 for true)

Client messages
---------------

Server messages
---------------

The server con send to the client the message with the following type codes:

- **0**: Failure response. Schema:
  - uint64: Code identifying the failure type.
  - text: Human-readable failure message.
