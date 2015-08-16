# Boiler Bay

Boiler Bay is a facade exposing a subset of the Kafka API (basically producer and high level consumer) via sockets. The implementation of Boiler Bay intentionally maps directly to the Kafka Java client - the point of Boiler Bay is to exploit the fact that the Java client of Kafka is well-maintained and documented, without being restricted to using Java on the application layer.

## ready

## consume
After connecting, a client can send `consume <group>`. This will start up a consumer on the consumergroup, tied to this socket. Under the hood, it will spawn a thread where it creates a ConsumerConnector and create a single-thread message stream on it. Once this is done, the server will send `consume-started` on the socket.

## ack
Commit and next rolled into one. After receiving `consume-started` the client can send `ack` on the socket. This tells the server that the client has processed the last message from msg (if any) and is ready for the next message on the log. This maps directly to the commitOffsets and next methods on the iterator of the only thread stream on the Java Client.

## msg
As soon as the Java client receives a message from the aforementioned .next() call, it will send it on the socket in the form of `msg <strbody>`.

# send
`send <topic> <partitionid> <body>` Automatically starts up a producer, and will send all ´send`s delivered on the same producer.

# send-ok
# send-fail <code> <message>


# Configuration
Boiler Bay offers no configuration per-consumer or per-producer - this is determined in a configuration file for all consumers.
The exceptions are "group.id" and "auto.offset.reset" which are exposed through the <group> and <start> arguments of `consume`.
