package com.yens;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ExpiringMap<K, V> {

    private final ConcurrentHashMap<K, ExpiringEntry<V>> map;
    private final ScheduledExecutorService executor;
    private final long expirationTime;

    public ExpiringMap(long expirationTime, TimeUnit unit) {
        this.map = new ConcurrentHashMap<>();
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.expirationTime = TimeUnit.MILLISECONDS.convert(expirationTime, unit);
        scheduleCleanup();
    }

    private void scheduleCleanup() {
        ScheduledFuture<?> cleanupTask = executor.scheduleAtFixedRate(this::cleanupExpiredEntries, 0, 1,
                TimeUnit.SECONDS);
        // Consider adding a shutdown hook to cancel the task when the map is no longer
        // needed
        // Runtime.getRuntime().addShutdownHook(new Thread(cleanupTask::cancel));
    }

    public void put(K key, V value) {
        map.put(key, new ExpiringEntry<>(value, System.currentTimeMillis() + expirationTime));
    }

    public V get(K key) {
        ExpiringEntry<V> entry = map.get(key);
        return entry != null && !entry.isExpired() ? entry.value : null;
    }

    private void cleanupExpiredEntries() {
        long currentTime = System.currentTimeMillis();
        for (K key : map.keySet()) {
            ExpiringEntry<V> entry = map.get(key);
            if (entry != null && entry.isExpired(currentTime)) {
                map.remove(key);
            }
        }
    }

    private static class ExpiringEntry<V> {
        private final V value;
        private final long expirationTime;

        public ExpiringEntry(V value, long expirationTime) {
            this.value = value;
            this.expirationTime = expirationTime;
        }

        public boolean isExpired() {
            return isExpired(System.currentTimeMillis());
        }

        public boolean isExpired(long currentTime) {
            return currentTime > expirationTime;
        }
    }

    public void shutdown() {
        executor.shutdown();
    }

}

// import java.util.concurrent.*;
// import java.util.PriorityQueue;
// import java.util.Comparator;

// public class ExpiringQueue<K, V> {
// private final ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();
// private final PriorityQueue<QueueEntry<K>> queue = new PriorityQueue<>(
// Comparator.comparingLong(QueueEntry::getExpirationTime));
// private final ScheduledExecutorService scheduler =
// Executors.newSingleThreadScheduledExecutor();

// public ExpiringQueue() {
// startScheduler();
// }

// // Method to add an entry with an expiration time
// public void put(K key, V value, long expirationTimeMillis) {
// long expirationTime = System.currentTimeMillis() + expirationTimeMillis;
// map.put(key, value);
// synchronized (queue) {
// queue.offer(new QueueEntry<>(key, expirationTime));
// queue.notify(); // Notify the scheduler in case the new entry expires earlier
// }
// }

// // Method to retrieve an entry
// public V get(K key) {
// return map.get(key);
// }

// // Method to remove an entry manually
// public V remove(K key) {
// synchronized (queue) {
// queue.removeIf(entry -> entry.getKey().equals(key));
// }
// return map.remove(key);
// }

// // Check if a key exists
// public boolean containsKey(K key) {
// return map.containsKey(key);
// }

// // Method to start the scheduler
// private void startScheduler() {
// scheduler.schedule(this::processQueue, 1, TimeUnit.SECONDS);
// }

// // Method to check and remove expired entries
// private void processQueue() {
// long now = System.currentTimeMillis();
// synchronized (queue) {
// while (!queue.isEmpty() && queue.peek().getExpirationTime() <= now) {
// K key = queue.poll().getKey();
// map.remove(key); // Remove the expired entry
// }

// if (!queue.isEmpty()) {
// long delay = queue.peek().getExpirationTime() - now;
// scheduler.schedule(this::processQueue, delay, TimeUnit.MILLISECONDS);
// } else {
// // If the queue is empty, check again after a fixed delay
// scheduler.schedule(this::processQueue, 1, TimeUnit.SECONDS);
// }
// }
// }

// // Method to shut down the scheduler gracefully
// public void shutdown() {
// scheduler.shutdown();
// try {
// if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
// scheduler.shutdownNow();
// }
// } catch (InterruptedException e) {
// scheduler.shutdownNow();
// }
// }

// // Custom class to store entries with their expiration time
// private static class QueueEntry<K> {
// private final K key;
// private final long expirationTime;

// public QueueEntry(K key, long expirationTime) {
// this.key = key;
// this.expirationTime = expirationTime;
// }

// public K getKey() {
// return key;
// }

// public long getExpirationTime() {
// return expirationTime;
// }
// }

// // public static void main(String[] args) throws InterruptedException {
// // // Example usage
// // ExpiringQueue<String, String> queue = new ExpiringQueue<>();

// // queue.put("key1", "value1", 5000); // Expires in 5 seconds
// // queue.put("key2", "value2", 10000); // Expires in 10 seconds

// // System.out.println("Initial size: " + queue.get("key1")); // Output: 2

// // Thread.sleep(7000);

// // System.out.println("Size after 7 seconds: " + queue.map.size()); //
// Output: 1
// // (key1 expired)

// // Thread.sleep(5000);

// // System.out.println("Size after 12 seconds: " + queue.map.size()); //
// Output:
// // 0 (key2 expired)

// // queue.shutdown();
// // }
// }
