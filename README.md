# Akka Streams Valve

This mini project showcases a Java implementation of a `Valve` in
Akka Streams that is threadsafe. You can find the implementation
in the [`Valve.java` class in `akka-streams-valve` module](akka-streams-valve/src/main/java/io/example/Valve.java).

A simple example of how to use it is included in the
[`akka-streams-valve` module](akka-streams-valve/src/main/java/io/example/Main.java)
and an example of it being used with [Alpakka Kafka](https://doc.akka.io/libraries/alpakka-kafka/current/home.html)
can be found in the [`kafka-example` module](kafka-example/src/main/java/io/example/Main.java) including how to
propagate both the `Valve` and the
[
`DrainageControl` that can be used to stop a consumer on shutdown](https://doc.akka.io/libraries/alpakka-kafka/current/consumer.html#controlled-shutdown). 