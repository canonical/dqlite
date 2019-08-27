Wire protocol
=============

Definitions
-----------

The following terms are used in this document:

- **server**: A dqlite node.

- **client**: Either application code (typically wanting to issue database
  requests) or a dqlite node (typically pushing replicational data).

- **connection**: A TCP or Unix socket connection established by the client
  against a server.

- **word**: A sequence of 8 bytes.

- **protocol version**: A positive number stored in a word using little endian
  representation.

- **message**: A sequence of bytes sent either by the client to the server or by
  the server to the client. It consists of a header and a body. The header
  consists of a single word the following layout:

  - byte 0 to 3: Size of the message body, expressed in words and stored using
    little endian representation. For example a value of [2 1 0 0] means that
    the message body consists of 258 bytes.

  - byte 4: Type code identifiying the schema of the message body.

  - byte 5: Revision number of the message body schema. A new schema revision
    can only add new fields to the previous revision. So a client capable of
    understanding up to revision N of a certain message schema can still handle
    messages with revision >N by simply ignoring the extra bytes.

  - byte 6 to 7: Currently unused.

Setup
-----

As soon as a connection is established, the client must send to the server a
single word containing the protocol version it wishes to use.

Conversation
------------

After the setup, communication between client and server happens by message
exchange. Typically the client will send to the server a message containing a
request and the server will send to the client a message containing a response.

Client messages
---------------

Server messages
---------------

The server con send to the client the message with the following type codes:

- **0**: Failure response. Schema:
  - uint64: Code identifying the failure type.
  - text: Human-readable failure message.
