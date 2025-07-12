package com.bdpf.dijkstra;

import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.logging.Log;

public class ExpiringMapStorage<K, V extends Closeable> {

    private final PriorityBlockingQueue<ExpiringEntry<V, K>> pq = new PriorityBlockingQueue<>();
    private final ConcurrentHashMap<K, ExpiringEntry<V, K>> map = new ConcurrentHashMap<>();

    public ExpiringMapStorage() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        Runnable task = () -> {
            this.runCleaner();
        };

        scheduler.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
    }

    public void runCleaner() {
        synchronized (this) {
            while (!pq.isEmpty()) {
                ExpiringEntry<V, K> entry = pq.peek();
                if (entry == null) {
                    break;
                }
                if (System.currentTimeMillis() < entry.getExpiredAt() && !this.isHeapAboveLimit()) {
                    break;
                }
                if (entry.isLocked()) {
                    this.updateExpirationTimeSeconds(entry.key, 2);
                    continue;
                }

                this.remove(entry.key);
                entry.value.close();

            }
        }
    }

    private final long maxHeap = Runtime.getRuntime().maxMemory();

    public boolean isHeapAboveLimit() {
        Runtime runtime = Runtime.getRuntime();
        long used = runtime.totalMemory() - runtime.freeMemory();
        return ((double) used / maxHeap) > 0.95;
    }

    public ExpiringEntry<V, K> put(K key, V value, long expirationTimeSeconds) {
        this.remove(key);
        ExpiringEntry<V, K> entry = new ExpiringEntry<V, K>(value, key, expirationTimeSeconds);
        if (expirationTimeSeconds == 0) {
            return entry;
        }
        synchronized (this) {
            pq.add(entry);
            map.put(key, entry);
        }
        return entry;
    }

    public void remove(K key) {
        synchronized (this) {
            ExpiringEntry<V, K> entry = this.map.get(key);
            if (entry != null) {
                pq.remove(entry);
                map.remove(key);
            }
        }
    }

    public void updateExpirationTimeSeconds(K key, long expirationTimeSeconds) {
        synchronized (this) {
            ExpiringEntry<V, K> entry = this.map.get(key);
            this.remove(key);
            if (entry != null) {
                this.put(key, entry.value, expirationTimeSeconds);
            }
        }
    }

    public ExpiringEntry<V, K> get(K key) {
        ExpiringEntry<V, K> entry = this.map.get(key);
        if (entry == null) {
            return null;
        }

        return entry;
    }

    public static class ExpiringEntry<V, K> implements Comparable<ExpiringEntry<V, K>> {
        private final V value;
        private final K key;
        private long expiredAt;

        private final ReentrantLock lock = new ReentrantLock(true);

        public ExpiringEntry(V value, K key, long expirationTimeSeconds) {
            this.value = value;
            this.key = key;
            this.updateExpirationTimeSeconds(expirationTimeSeconds);
        }

        public long getExpiredAt() {
            return this.expiredAt;
        }

        public void updateExpirationTimeSeconds(long expirationTimeSeconds) {
            this.expiredAt = System.currentTimeMillis() + 1000 * expirationTimeSeconds;
        }

        public V getValue() {
            return this.value;
        }

        public void lock() {
            this.lock.lock();
        }

        public void unlock() {
            this.lock.unlock();
        }

        public boolean isLocked() {
            return this.lock.isLocked();
        }

        public K getKey() {
            return this.key;
        }

        @Override
        public int compareTo(ExpiringEntry<V, K> other) {
            return Long.compare(this.expiredAt, other.expiredAt);
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }

}