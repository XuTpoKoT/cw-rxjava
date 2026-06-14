package org.example;

@FunctionalInterface
public interface Scheduler {
    void execute(Runnable task);
}
