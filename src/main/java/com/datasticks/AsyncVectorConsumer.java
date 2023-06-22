package com.datasticks;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

@FunctionalInterface
interface AsyncVectorConsumer extends BiFunction<Integer, float[], CompletableFuture<?>> {}
