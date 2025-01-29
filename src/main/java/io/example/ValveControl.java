package io.example;

import java.util.concurrent.CompletionStage;

public interface ValveControl {
    // Pause the stream
    void pause();

    // Resume the stream
    void resume();

    // Check if the stream is paused
    boolean isPaused();

    // Completes the valve, signaling it's finished
    CompletionStage<Complete> complete();

    public static record Complete() {
    }

    public static Complete Completed = new Complete();
}
