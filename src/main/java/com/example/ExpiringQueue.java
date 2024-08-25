package com.example.neo4j;

import java.util.concurrent.*;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.Map.Entry;

public class ExpiringQueue<K, V> {
    private final ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();
    private final PriorityQueue<QueueEntry<K>> queue = new PriorityQueue<>(
            Comparator.comparingLong(QueueEntry::getExpirationTime));
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public ExpiringQueue() {
        startScheduler();
    }

    // Method to add an entry with an expiration time
    public void put(K key, V value, long expirationTimeMillis) {
        long expirationTime = System.currentTimeMillis() + expirationTimeMillis;
        map.put(key, value);
        synchronized (queue) {
            queue.offer(new QueueEntry<>(key, expirationTime));
            queue.notify(); // Notify the scheduler in case the new entry expires earlier
        }
    }

    // Method to retrieve an entry
    public V get(K key) {
        return map.get(key);
    }

    // Method to remove an entry manually
    public V remove(K key) {
        return map.remove(key);
    }

    // Check if a key exists
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    // Method to start the scheduler
    private void startScheduler() {
        scheduler.scheduleWithFixedDelay(this::checkExpiredEntries, 1, 1, TimeUnit.SECONDS);
    }

    // Method to check and remove expired entries
    private void checkExpiredEntries() {
        long now = System.currentTimeMillis();
        synchronized (queue) {
            while (!queue.isEmpty() && queue.peek().getExpirationTime() <= now) {
                K key = queue.poll().getKey();
                map.remove(key); // Remove the expired entry
            }
        }
    }

    // Method to shut down the scheduler gracefully
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }

    // Custom class to store entries with their expiration time
    private static class QueueEntry<K> {
        private final K key;
        private final long expirationTime;

        public QueueEntry(K key, long expirationTime) {
            this.key = key;
            this.expirationTime = expirationTime;
        }

        public K getKey() {
            return key;
        }

        public long getExpirationTime() {
            return expirationTime;
        }
    }

    // public static void main(String[] args) throws InterruptedException {
    // // Example usage
    // ExpiringQueue<String, String> queue = new ExpiringQueue<>();

    // queue.put("key1", "value1", 5000); // Expires in 5 seconds
    // queue.put("key2", "value2", 10000); // Expires in 10 seconds

    // System.out.println("Initial size: " + queue.map.size()); // Output: 2

    // Thread.sleep(7000);

    // System.out.println("Size after 7 seconds: " + queue.map.size()); // Output: 1
    // (key1 expired)

    // Thread.sleep(5000);

    // System.out.println("Size after 12 seconds: " + queue.map.size()); // Output:
    // 0 (key2 expired)

    // queue.shutdown();
    // }
}
