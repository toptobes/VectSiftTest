package com.datastax;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class Main {
    private static final int MAX_CONCURRENT_WRITES = 100;

    private static final CqlSession session = new CqlSessionBuilder().build();

    static {
        Arrays.stream("""
        CREATE KEYSPACE IF NOT EXISTS testing WITH
            replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            };
        
        USE testing;
        
        CREATE TABLE IF NOT EXISTS sifttest (
            key int,
            val vector<float, 128>,
            PRIMARY KEY ((key))
        );
        
        CREATE CUSTOM INDEX IF NOT EXISTS ON sifttest(val) USING 'StorageAttachedIndex';
        
        TRUNCATE sifttest
        """.split(";")).forEach(session::execute);
    }

    @SuppressWarnings({"DataFlowIssue"})
    public static void main(String[] args) throws Throwable {
        readFloatVectors("/sift-data/base.fvecs", (vector, i) -> (
            session.executeAsync("INSERT INTO sifttest (key, val) VALUES (%d, %s)".formatted(i, Arrays.toString(vector))).toCompletableFuture()
        ));

        var topKFound = new AtomicInteger();
        var totalQueries = new AtomicInteger();
        final var topK = 100;

        try (var dis = new DataInputStream(new BufferedInputStream(Main.class.getResourceAsStream("/sift-data/groundtruth.ivecs")))) {
            readFloatVectors("/sift-data/query.fvecs", (vector, i) -> {
                var vecStr = Arrays.toString(vector);
                var future = session.executeAsync("SELECT key FROM sifttest ORDER BY val ANN OF %s LIMIT %d".formatted(vecStr, topK)).toCompletableFuture();

                var gt = readNextIntegerVec(dis);

                future.thenAccept((result) -> {
                    result.currentPage().forEach(row -> {
                        if (gt.contains(row.getInt("key"))) {
                            topKFound.getAndIncrement();
                        }
                    });
                    totalQueries.getAndIncrement();
                });

                return future;
            });
        }

        var recall = topKFound.doubleValue() / (totalQueries.get() * topK);

        System.out.println(recall);
        assert recall > .975;
    }

    @SuppressWarnings({"DataFlowIssue", "ResultOfMethodCallIgnored"})
    private static <T> void readFloatVectors(String filePath, BiFunction<float[], Integer, CompletableFuture<T>> fn) throws IOException, InterruptedException {
        Semaphore semaphore = new Semaphore(MAX_CONCURRENT_WRITES);
        BlockingQueue<CompletableFuture<T>> futures = new LinkedBlockingQueue<>(MAX_CONCURRENT_WRITES);
        var idx = 0;

        try (var dis = new DataInputStream(new BufferedInputStream(Main.class.getResourceAsStream(filePath)))) {
            while (dis.available() > 0) {
                var dimension = Integer.reverseBytes(dis.readInt());
                var buffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);

                dis.readFully(buffer.array());

                var vector = new float[dimension];

                for (int i = 0; i < dimension; i++) {
                    vector[i] = buffer.getFloat();
                }

                semaphore.acquire();

                CompletableFuture<T> future = fn.apply(vector, idx++);
                futures.offer(future);

                future.whenComplete((result, ex) -> {
                    futures.poll();
                    semaphore.release();
                });
            }

            futures
                .parallelStream()
                .forEach(CompletableFuture::join);
        }
    }

    private static HashSet<Integer> readNextIntegerVec(DataInputStream dis) {
        try {
            var numNeighbors = Integer.reverseBytes(dis.readInt());
            var groundTruth = new HashSet<Integer>(numNeighbors);

            for (var i = 0; i < numNeighbors; i++) {
                groundTruth.add(Integer.reverseBytes(dis.readInt()));
            }

            return groundTruth;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
