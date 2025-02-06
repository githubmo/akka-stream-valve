package io.example;

import akka.japi.Pair;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;

import java.time.Duration;
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

    private boolean isPausedOnStart = false;

    public Valve() {
    }

    public Valve(boolean isPausedOnStart) {
        this.isPausedOnStart = isPausedOnStart;
    }

    @Override
    public Pair<GraphStageLogic, ValveControl> createLogicAndMaterializedValuePair(Attributes inheritedAttributes) {
        // Atomic boolean to track the paused state
        AtomicBoolean isPaused = new AtomicBoolean(isPausedOnStart);

        interface CommandHandler {
            AsyncCallback<ValveControl.Command> getCommandHandler();

            void onAsyncCommand(ValveControl.Command command);
        }

        abstract class CommandHandlerGraphStageLogic extends GraphStageLogic implements CommandHandler {
            public CommandHandlerGraphStageLogic(FlowShape<T, T> shape) {
                super(shape);
            }
        }

        // Akka GraphStageLogic implementation
        CommandHandlerGraphStageLogic logic = new CommandHandlerGraphStageLogic(shape) {

            private static final AtomicBoolean hasBeenPulled = new AtomicBoolean(false);

            private final AsyncCallback<ValveControl.Command> commandHandler =
                    createAsyncCallback(this::onAsyncCommand);

            public AsyncCallback<ValveControl.Command> getCommandHandler() {
                return commandHandler;
            }

            public void onAsyncCommand(ValveControl.Command command) {
                switch (command) {
                    case PAUSE -> {
                        if (!isPaused.get()) {
                            // we pause first then empty what is in buffered so we don't
                            // have a situation with an element not being processed
                            isPaused.set(true);
                            if (isAvailable(in)) {
                                push(out, grab(in));
                            }
                        }
                    }
                    case RESUME -> {
                        if (isPaused.get()) {
                            // we unpause first so that we don't have a race condition of a downstream
                            // pulling data faster than we can resume
                            isPaused.set(false);
                            if (isAvailable(in)) {
                                push(out, grab(in));
                            } else if ((isAvailable(out) && hasBeenPulled(in)) || hasBeenPulled.get()) {
                                hasBeenPulled.set(false); //reset the flag
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
                        // Push the element downstream if not paused
                        if (!isPaused.get()) {
                            push(out, grab(in));
                        }
                    }
                });

                setHandler(out, new OutHandler() {
                    @Override
                    public void onPull() {
                        // Pull upstream when downstream asks for data
                        if (!isPaused.get()) {
                            pull(in);
                        } else {
                            hasBeenPulled.set(true);
                        }
                    }
                });
            }
        };

        // Control handle for external interaction
        ValveControl control = new ValveControl() {
            private final CompletableFuture<ValveControl.Complete> gate = new CompletableFuture<>();
            private final AsyncCallback<ValveControl.Command> commandHandler = logic.getCommandHandler();

            @Override
            public void pause() throws InterruptedException, TimeoutException {
                Await.result(commandHandler.invokeWithFeedback(ValveControl.Command.PAUSE), scala.concurrent.duration.Duration.apply(1, TimeUnit.SECONDS));
            }

            @Override
            public void resume() throws InterruptedException, TimeoutException {
                Await.result(commandHandler.invokeWithFeedback(Command.RESUME), scala.concurrent.duration.Duration.apply(1, TimeUnit.SECONDS));
            }

            @Override
            public boolean isPaused() {
                return isPaused.get();
            }

            @Override
            public CompletionStage<ValveControl.Complete> complete() {
                gate.complete(ValveControl.Completed);
                return gate;
            }
        };

        return new Pair<>(logic, control);
    }
}

