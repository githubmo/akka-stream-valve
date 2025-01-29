package io.example;

import akka.japi.Pair;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractGraphStageWithMaterializedValue;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.OutHandler;
import akka.stream.stage.InHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;


public class Valve<T> extends AbstractGraphStageWithMaterializedValue<FlowShape<T, T>, ValveControl> {
    public final Inlet<T> in = Inlet.create("Valve.in");
    public final Outlet<T> out = Outlet.create("Valve.out");

    private final FlowShape<T, T> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<T, T> shape() {
        return shape;
    }

    @Override
    public Pair<GraphStageLogic, ValveControl> createLogicAndMaterializedValuePair(Attributes inheritedAttributes) {
        // Atomic boolean to track the paused state
        AtomicBoolean isPaused = new AtomicBoolean(false);

        // Akka GraphStageLogic implementation
        GraphStageLogic logic = new GraphStageLogic(shape) {
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
                        }
                    }
                });
            }
        };

        // Control handle for external interaction
        ValveControl control = new ValveControl() {
            private final CompletableFuture<ValveControl.Complete> gate = new CompletableFuture<>();

            @Override
            public void pause() {
                // we pause first then empty what is in buffered so we don't
                // have a situation with an element not being processed
                isPaused.set(true);
                if (logic.isAvailable(in)) {
                    logic.push(out, logic.grab(in));
                }
            }

            @Override
            public void resume() {
                // we unpause first so that we don't have a race condition of a downstream
                // pulling data faster than we can resume
                isPaused.set(false);
                if (logic.isAvailable(in)) {
                    logic.push(out, logic.grab(in));
                } else if (logic.isAvailable(out) && !logic.hasBeenPulled(in)) {
                    logic.pull(in);
                }
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

