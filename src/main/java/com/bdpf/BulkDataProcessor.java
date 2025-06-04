package com.bdpf;

// import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.*;

public class BulkDataProcessor {
    public interface Callable<V> {
        V call();
    }

    protected final GraphDatabaseService db;

    public BulkDataProcessor(GraphDatabaseService db) {
        this.db = db;
    }

    public void runTransaction(TransactionProcessor processor) {
        AtomicReference<Transaction> tx = new AtomicReference<>(db.beginTx());
        AtomicBoolean isActive = new AtomicBoolean(true);
        // int bachSize = 10;
        // ExecutorService executorService = bachSize == 0 ?
        // Executors.newCachedThreadPool()
        // : Executors.newFixedThreadPool(bachSize);

        // ExecutorService executorService = Executors.newFixedThreadPool(5);
        Callable<Transaction> commit = () -> {
            Transaction tx2 = tx.get();
            Transaction newTx = db.beginTx();
            tx.set(newTx);
            // executorService.submit(() -> {
            // try {
            tx2.commit();
            tx2.close();
            // } catch (Exception e) {
            // // Log commit error if necessary
            // }
            // });
            return newTx;
        };

        Runnable endProcess = () -> {
            isActive.set(false);
            Transaction tx2 = tx.get();
            tx2.commit();
            tx2.close();
            // synchronized (tx2) {
            // tx2.commit();
            // tx2.close();
            // }
        };

        while (isActive.get()) {
            // tx.close
            // executorService.submit(() -> {
            // Transaction newTx = db.beginTx();
            processor.process(tx.get(), commit, endProcess);
            // newTx.commit();
            // newTx.close();
            // });
        }
        // executorService.wait(0);
        // executorService.awaitTermination();
        // executorService.shutdown();
        // try {
        // if (!executorService.awaitTermination(800000000, TimeUnit.SECONDS)) {
        // executorService.shutdownNow();
        // }
        // } catch (InterruptedException e) {
        // executorService.shutdownNow();
        // }
    }

    @FunctionalInterface
    public interface NodeProcessor {
        void process(Node node);
    }

    @FunctionalInterface
    public interface TransactionProcessor {
        void process(Transaction tx, Callable<Transaction> commit, Runnable endProcess);
    }
}
