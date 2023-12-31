package com.datasticks;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final int MAX_CONCURRENT_WRITES = 100;
    private static final int TOP_K = 100;

    private static final String BASE_FVECS_FPATH  = "/sift-data/base.fvecs";
    private static final String QUERY_FVECS_FPATH = "/sift-data/query.fvecs";
    private static final String TRUTH_IVECS_FPATH = "/sift-data/groundtruth.ivecs";

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
        
        TRUNCATE sifttest;
        """.split(";"))
           .filter(s -> !s.isBlank())
           .forEach(session::execute);
    }

    public static void main(String[] args) throws Throwable {
        processFloatVectorsAsync(BASE_FVECS_FPATH, (key, vector) -> (
            executeAsync("INSERT INTO sifttest (key, val) VALUES (%d, %s)", key, Arrays.toString(vector))
        ));

        var topKFound = new AtomicInteger();
        var totalQueries = new AtomicInteger();

        try (var dis = createDISFomResource(TRUTH_IVECS_FPATH)) {
            processFloatVectorsAsync(QUERY_FVECS_FPATH, (key, vector) -> {
                var vecStr = Arrays.toString(vector);
                var future = executeAsync("SELECT key FROM sifttest ORDER BY val ANN OF %s LIMIT %d", vecStr, TOP_K);

                var gt = readNextGroundTruth(dis);

                future.thenAccept((result) -> {
                    int[] n = { 0 };

                    result.currentPage().forEach(row -> {
                        if (gt.contains(row.getInt("key"))) {
                            n[0]++;
                        }
                    });

                    topKFound.addAndGet(n[0]);
                    totalQueries.getAndIncrement();
                });

                return future;
            }).awaitCompletion();

            var recall = topKFound.doubleValue() / (totalQueries.get() * TOP_K);

            System.out.println(recall);
            assert recall > .975;
        } finally {
            session.close();
        }
    }

    private static Awaitable processFloatVectorsAsync(String filePath, AsyncVectorConsumer fn) throws IOException, InterruptedException {
        var futures = new ConcurrentLinkedQueue<CompletableFuture<?>>();
        var semaphore = new Semaphore(MAX_CONCURRENT_WRITES);

        var key = 0;

        try (var dis = createDISFomResource(filePath)) {
            while (dis.available() > 0) {
                var vector = readFloatNextVector(dis);

                semaphore.acquire();
                CompletableFuture<?> future = fn.apply(key++, vector);
                futures.offer(future);

                future.whenComplete((result, ex) -> {
                    futures.remove(future);
                    semaphore.release();
                });
            }
        }

        return () -> futures.parallelStream().forEach(CompletableFuture::join);
    }

    private static float[] readFloatNextVector(DataInputStream dis) throws IOException {
        var dimension = Integer.reverseBytes(dis.readInt());
        var buffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);

        dis.readFully(buffer.array());

        var vector = new float[dimension];
        buffer.asFloatBuffer().get(vector);

        return vector;
    }

    private static HashSet<Integer> readNextGroundTruth(DataInputStream dis) {
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

    private static CompletableFuture<AsyncResultSet> executeAsync(String query, Object ...bindings) {
        return session.executeAsync(query.formatted(bindings)).toCompletableFuture();
    }

    @SuppressWarnings("DataFlowIssue")
    private static DataInputStream createDISFomResource(String path) {
        return new DataInputStream(new BufferedInputStream(Main.class.getResourceAsStream(path)));
    }
}
