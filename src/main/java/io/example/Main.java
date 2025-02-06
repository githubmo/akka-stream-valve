package io.example;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import scala.concurrent.ExecutionContext;

import java.math.BigInteger;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;


public class Main {
    public static void main(String[] args) throws InterruptedException {
        final ActorSystem system = ActorSystem.create("QuickStart");

        final Source<Integer, NotUsed> source = Source.range(1, 20);

        final var valveFlow = new Valve<BigInteger>();

        // demonstration of computation on the integers
        final Source<BigInteger, NotUsed> factorials =
                source.scan(BigInteger.ONE, (acc, next) -> acc.multiply(BigInteger.valueOf(next)));

//        var result = factorials
//                .throttle(2, Duration.ofSeconds(1))
//                .viaMat(valveFlow, Keep.right())
//                .toMat(Sink.foreach(System.out::println), Keep.both())
//                .run(system);
        var result = source
                .map(BigInteger::valueOf)
                .throttle(2, Duration.ofSeconds(1))
                .viaMat(valveFlow, Keep.right())
                .toMat(Sink.foreach(System.out::println), Keep.both())
                .run(system);

        var valve = result.first();

        system.scheduler().schedule(Duration.ZERO, Duration.ofSeconds(5), () -> {
            if (valve.isPaused()) {
                try {
                    valve.resume();
                } catch (InterruptedException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            } else {
                try {
                    valve.pause();
                } catch (InterruptedException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
        }, ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor()));

        result
                .second()
                .thenAcceptAsync(v -> system.terminate())
                .thenAccept(v -> System.exit(0));

    }
}