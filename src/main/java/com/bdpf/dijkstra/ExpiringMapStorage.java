package com.bdpf.dijkstra;

import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.ConcurrentHashMap;

public class ExpiringMapStorage<K, V extends AutoCloseable> {

    private final PriorityBlockingQueue<ExpiringEntry<V, K>> pq = new PriorityBlockingQueue<>();
    private final ConcurrentHashMap<K, ExpiringEntry<V, K>> map = new ConcurrentHashMap<>();

    public ExpiringMapStorage() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        Runnable task = () -> {
            while (!pq.isEmpty()) {
                ExpiringEntry<V, K> entry = pq.poll();
                if (entry != null && System.currentTimeMillis() > entry.getExpiredAt()) {
                    try {
                        entry.value.close();
                    } catch (Exception e) {
                        System.err.println("Error while closing value: " + e.getMessage());
                    }
                    map.remove(entry.getKey());
                } else {
                    pq.add(entry);
                    break;
                }
            }
        };

        scheduler.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
    }

    public void put(K key, V value, long expirationTimeSeconds) {
        if (expirationTimeSeconds == 0) {
            return;
        }
        ExpiringEntry<V, K> entry = new ExpiringEntry<V, K>(value, key, expirationTimeSeconds);
        pq.add(entry);
        map.put(key, entry);
    }

    public void updateExpirationTimeSeconds(K key, long expirationTimeSeconds) {
        ExpiringEntry<V, K> entry = this.map.get(key);
        if (entry != null) {
            entry.updateExpirationTimeSeconds(expirationTimeSeconds);
        }
    }

    public V get(K key) {
        ExpiringEntry<V, K> entry = this.map.get(key);
        if (entry == null) {
            return null;
        }

        return entry.value;
    }

    private static class ExpiringEntry<V, K> implements Comparable<ExpiringEntry<V, K>> {
        private final V value;
        private final K key;
        private long expiredAt;

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
