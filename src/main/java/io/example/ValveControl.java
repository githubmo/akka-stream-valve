package io.example;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

public interface ValveControl {
    // Pause the stream
    void pause() throws InterruptedException, TimeoutException;

    ;

    // Resume the stream
    void resume() throws InterruptedException, TimeoutException;

    // Check if the stream is paused
    boolean isPaused();

    // Completes the valve, signaling it's finished
    CompletionStage<Complete> complete();

    public static record Complete() {
    }

    public static Complete Completed = new Complete();

    enum Command {
        PAUSE, RESUME
    }
}
