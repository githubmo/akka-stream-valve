package io.example;

import akka.NotUsed;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.stream.FlowShape;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class ValveTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    /**
     * Test class for the `Valve` class and its `shape` method.
     * The `shape` method is responsible for returning the `FlowShape` of the `Valve`,
     * which represents the structure of the flow graph, connecting an inlet to an outlet.
     */

    @Test
    public void testShapeNotNull() {
        // Arrange
        Valve<String> valve = new Valve<>();

        // Act
        FlowShape<String, String> shape = valve.shape();

        // Assert
        assertNotNull("The shape should not be null", shape);
    }

    @Test
    public void testShapeInletCorrect() {
        // Arrange
        Valve<Integer> valve = new Valve<>();

        // Act
        FlowShape<Integer, Integer> shape = valve.shape();

        // Assert
        assertNotNull("The inlet should not be null", shape.in());
        assertTrue("The inlet name should contain 'Valve.in'", shape.in().toString().contains("Valve.in"));
    }

    @Test
    public void testShapeOutletCorrect() {
        // Arrange
        Valve<Double> valve = new Valve<>();

        // Act
        FlowShape<Double, Double> shape = valve.shape();

        // Assert
        assertNotNull("The outlet should not be null", shape.out());
        assertTrue("The outlet name should contain 'Valve.out'", shape.out().toString().contains("Valve.out"));
    }

    @Test
    public void testStreamShouldEmitElementAsUsualByDefault() throws ExecutionException, InterruptedException {
        final Source<Integer, NotUsed> source = Source.range(1, 10);
        final var valveFlow = new Valve<Integer>();
        var result = source.via(valveFlow).toMat(Sink.seq(), Keep.right()).run(testKit.system());

        assertEquals(result.toCompletableFuture().get(), List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    }

    @Test
    public void testIsPausedOnStart() throws InterruptedException, TimeoutException {
        var system = testKit.system();
        final var valveFlow = new Valve<Integer>(true);
        var result = TestSource.<Integer>create(testKit.system().classicSystem())
                .viaMat(valveFlow, Keep.both())
                .toMat(TestSink.probe(system.classicSystem()), Keep.both())
                .run(testKit.system());

        var sourceProbe = result.first().first();
        var valve = result.first().second();
        var sinkProbe = result.second();

        Assert.assertTrue("Valve is paused on start", valve.isPaused());
        sourceProbe.sendNext(1);
        sinkProbe.request(1);
        sinkProbe.expectNoMessage(Duration.ofMillis(300));
        valve.resume();
        assertEquals(sinkProbe.expectNext(), Integer.valueOf(1));

        sourceProbe.sendNext(2);
        sourceProbe.sendNext(3);
        sourceProbe.sendComplete();
        sinkProbe.request(2);
        assertEquals(sinkProbe.expectNext(), Integer.valueOf(2));
        assertEquals(sinkProbe.expectNext(), Integer.valueOf(3));
        sinkProbe.expectComplete();
    }
}