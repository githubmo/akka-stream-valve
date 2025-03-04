package io.example;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.CoordinatedShutdown;
import akka.japi.Pair;
import akka.kafka.*;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.ExecutionContext;

import java.math.BigInteger;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        final ActorSystem system = ActorSystem.create("QuickStart");

        final Source<Integer, NotUsed> source = Source.range(1, 20);

        final var valveFlow = new Valve<ConsumerMessage.CommittableOffset>();

        final Logger log = LoggerFactory.getLogger(Main.class);

        var topic = "example-topic";

        final Config producerConfig = system.settings().config().getConfig("akka.kafka.producer");
        final ProducerSettings<String, String> producerSettings =
                ProducerSettings.create(producerConfig, new StringSerializer(), new StringSerializer())
                        .withBootstrapServers("localhost:9092");

        final Config consumerConfig = system.settings().config().getConfig("akka.kafka.consumer");
        final ConsumerSettings<String, String> consumerSettings =
                ConsumerSettings.create(consumerConfig, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId("group1")
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        var committerSettings = CommitterSettings.create(system.settings().config().getConfig("akka.kafka.committer"));

        CompletionStage<Done> producerDone =
                Source.range(1, 100)
                        .throttle(5, Duration.ofSeconds(1))
                        .map(Object::toString)
                        .map(i -> {
                            System.out.println("***Producing: " + i);
                            return i;
                        })
                        .map(value -> new ProducerRecord<String, String>(topic, value))
                        .runWith(Producer.plainSink(producerSettings), system);

        Pair<Pair<Consumer.DrainingControl<Done>, ValveControl>, CompletionStage<Done>> consumerDoneWithValve = Consumer.<String, String>committableSource(consumerSettings, Subscriptions.topics(topic))
                .mapAsync(1, i -> {
                    System.out.println(">>>Consumed: " + i.record().value());
                    return CompletableFuture.completedFuture(i.committableOffset());
                })
                .alsoToMat(Committer.sink(committerSettings.withMaxBatch(1)), Consumer::createDrainingControl)
                .viaMat(valveFlow, Keep.both())
                .toMat(Sink.ignore(), Keep.both())
                .run(system);

        ValveControl valveControl = consumerDoneWithValve.first().second();
        var consumerDrainingControl = consumerDoneWithValve.first().first();

        system.scheduler().scheduleAtFixedRate(Duration.ZERO, Duration.ofSeconds(3), () -> {
            if (valveControl.isPaused()) {
                try {
                    valveControl.resume();
                } catch (InterruptedException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            } else {
                try {
                    valveControl.pause();
                } catch (InterruptedException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
        }, ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor()));


        CoordinatedShutdown.get(system)
                .addTask(
                        CoordinatedShutdown.PhaseBeforeServiceUnbind(),
                        "Drain and shutdown consumer",
                        () -> consumerDrainingControl.drainAndShutdown(system.dispatcher()));
    }
}