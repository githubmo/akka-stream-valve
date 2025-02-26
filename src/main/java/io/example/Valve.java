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
    private boolean isPausedOnStart = false;

    public Valve() {
    }

    public Valve(boolean isPausedOnStart) {
        this.isPausedOnStart = isPausedOnStart;
    }

    @Override
    public Pair<GraphStageLogic, ValveControl> createLogicAndMaterializedValuePair(Attributes inheritedAttributes) {

        // Feedback: the only two things that need thread-safety/synchronization/atomics
        // is signalling that the stage should be paused or unpaused
        // and possibly to inspect that it is paused/unpaused, however, since the latter is
        // idempotent: pausing an already paused stream, unpausing an unpaused stream the latter may not be needed.
        // So if you do not stricly need the isPaused method on the valve, skipping this and having the flag as a
        // regular mutable field inside the stage is cleaner

        // Atomic boolean to track the paused state
        AtomicBoolean isPaused = new AtomicBoolean(isPausedOnStart);

        interface CommandHandler {
            AsyncCallback<ValveControl.Command> getCommandHandler();

            // Feedback: this is the internal method of the stage and should not be in the interface
            void onAsyncCommand(ValveControl.Command command);
        }

        abstract class CommandHandlerGraphStageLogic extends GraphStageLogic implements CommandHandler {
            public CommandHandlerGraphStageLogic(FlowShape<T, T> shape) {
                super(shape);
            }
        }

        // Akka GraphStageLogic implementation
        CommandHandlerGraphStageLogic logic = new CommandHandlerGraphStageLogic(shape) {

            private boolean pullObserved = false;

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
                                // Feedback: this is not safe, that there is an element available for pulling
                                // does not mean the downstream is ready for pushing, when we are reacting on
                                // an async callback.
                                //
                                // There are two possible ways this could be implemented:
                                // 1. complete pause as soon as the stage sees pause command here, if there was a pull
                                //    sent upstream, the element will be stuck in the in-buffer until unpaused
                                // 2. if there was a pull sent upstream, the pause means no more demand is sent upstream
                                //    but once that element arrives it will still go through
                                //
                                // It seems like that the second one is what you are aiming for, if the pause method
                                // can be fire and forget, that is easy because then is just about pausing pulls, pushes
                                // can be let through.
                                //
                                // If you also need to observe that the pause completed so that no more elements will
                                // go downstream until unpaused, that is a bit trickier though. You will also need to
                                // have pass a CompletableFuture<Done> in the command that you can either complete right
                                // away here, if this stage has not been pushed since the last pull, or keep around
                                // and complete once that last element comes if this stage did pull and is waiting for
                                // a push.
                                //
                                // If you in addition to that also need to be able to allow multiple callers pausing and
                                // unpausing, that complicates the whole thing quite a bit further than if only on place
                                // is allowed to togle the state.
                                //
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
                                // Feedback: this is not safe, but should also not be needed if you have sorted always
                                // letting elements through if upstream was pulled when the pause was triggered - then
                                // there will _never_ be an element in here.
                                push(out, grab(in));
                            } else if ((isAvailable(out) && hasBeenPulled(in)) || pullObserved) {
                                // Feedback: you should be able to know that !hasBeenPulled(in) and not check that
                                // (not that it hurts to check, just that it should always be false)
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
                        // Feedback: as I mentioned above, for the easiest, relatively correct version that doesn't leave
                        // a single element around in a buffer if upstream was pulled, I'd let the pause logic only be
                        // about pulls, and always let pushes through here.

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
                            pullObserved = true;
                        }
                    }
                });
            }
        };

        // Control handle for external interaction
        ValveControl control = new ValveControl() {
            // Feedback: not sure what the gate is meant to do, but if it is a stream deathwatch or something like that
            // I'd recommend not ad-hhoc implementing that again in this stage but rely on using watchTermination for that
            // when composing the stream
            private final CompletableFuture<ValveControl.Complete> gate = new CompletableFuture<>();
            private final AsyncCallback<ValveControl.Command> commandHandler = logic.getCommandHandler();

            @Override
            public void pause() throws InterruptedException, TimeoutException {
                // Feedback: never do Await and hide the blocking like this, if this was called inside an actor of from that
                // a CompletionStage callback it causes starvation of the threadpool. If completion needs to be signalled
                // return CompletionStage<Done>, if the caller does not need to know, just don't Await the result but do
                // a fire and forget, in that case you can also use `invoke` instead of `invokeWithFeedback`.
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