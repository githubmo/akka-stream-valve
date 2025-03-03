package io.example;

import akka.Done;
import akka.japi.Pair;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;


public class Valve<T> extends AbstractGraphStageWithMaterializedValue<FlowShape<T, T>, ValveControl> {
    private static final Logger log = LoggerFactory.getLogger(Valve.class);
    public final Inlet<T> in = Inlet.create("Valve.in");
    public final Outlet<T> out = Outlet.create("Valve.out");

    private final FlowShape<T, T> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<T, T> shape() {
        return shape;
    }

    // Feedback: this should be final since it is accessed from another thread
    private final boolean isPausedOnStart;

    public Valve() {
        isPausedOnStart = false;
    }

    public Valve(boolean isPausedOnStart) {
        this.isPausedOnStart = isPausedOnStart;
    }

    @Override
    public Pair<GraphStageLogic, ValveControl> createLogicAndMaterializedValuePair(Attributes inheritedAttributes) {

        interface CommandHandler {
            AsyncCallback<ValveControl.Command> getCommandHandler();

            boolean isPaused();
        }

        abstract class CommandHandlerGraphStageLogic extends GraphStageLogic implements CommandHandler {
            public CommandHandlerGraphStageLogic(FlowShape<T, T> shape) {
                super(shape);
            }
        }

        // Akka GraphStageLogic implementation
        CommandHandlerGraphStageLogic logic = new CommandHandlerGraphStageLogic(shape) {

            private volatile boolean isPaused = isPausedOnStart;

            private boolean pullObserved = false;

            private final AsyncCallback<ValveControl.Command> commandHandler =
                    createAsyncCallback(this::onAsyncCommand);

            public AsyncCallback<ValveControl.Command> getCommandHandler() {
                return commandHandler;
            }

            // TODO: If I am strictly reading if it is paused would this work?
            public boolean isPaused() {
                return isPaused;
            }

            public void onAsyncCommand(ValveControl.Command command) {
                switch (command) {
                    case PAUSE -> {
                        if (!isPaused) {
                            isPaused = true;
                        }
                    }
                    case RESUME -> {
                        if (isPaused) {
                            // we unpause first so that we don't have a race condition of a downstream
                            // pulling data faster than we can resume
                            isPaused = false;
                            if (pullObserved) {
                                pullObserved = false; //reset the flag
                                pull(in);
                            }
                        }
                    }
                }
            }

            {


                setHandler(in, new InHandler() {
                    @Override
                    public void onPush() {
                        push(out, grab(in));
                    }
                });

                setHandler(out, new OutHandler() {
                    @Override
                    public void onPull() {
                        // Pull upstream when downstream asks for data
                        if (!isPaused) {
                            pull(in);
                        } else {
                            pullObserved = true;
                        }
                    }
                });
            }
        };

        // Control handle for external interaction
        ValveControl control = new ValveControl() {
            private final AsyncCallback<ValveControl.Command> commandHandler = logic.getCommandHandler();

            @Override
            public CompletionStage<Done> pause() throws InterruptedException, TimeoutException {
                return scala.jdk.javaapi.FutureConverters.asJava(
                        commandHandler.invokeWithFeedback(ValveControl.Command.PAUSE)
                );
            }

            @Override
            public CompletionStage<Done> resume() throws InterruptedException, TimeoutException {
                return scala.jdk.javaapi.FutureConverters.asJava(commandHandler.invokeWithFeedback(Command.RESUME));
            }

            @Override
            public boolean isPaused() {
                return logic.isPaused();
            }
        };

        return new Pair<>(logic, control);
    }
}