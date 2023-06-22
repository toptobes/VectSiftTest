package com.datastax;

@FunctionalInterface
interface Awaitable {
    void awaitCompletion();
}
