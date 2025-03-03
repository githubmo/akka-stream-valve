package io.example;

import akka.Done;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

public interface ValveControl {
    // Pause the stream
    CompletionStage<Done> pause() throws InterruptedException, TimeoutException;

    // Resume the stream
    CompletionStage<Done> resume() throws InterruptedException, TimeoutException;

    // Check if the stream is paused
    boolean isPaused();

    enum Command {
        PAUSE, RESUME
    }
}
