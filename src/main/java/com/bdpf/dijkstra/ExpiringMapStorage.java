package com.bdpf.dijkstra;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ExpiringMapStorage<K, V> {

    private final Map<K, V> internalMap;

    private final Map<K, ExpiringEntry<K>> expiringEntries;

    private final DelayQueue<ExpiringEntry> delayQueue = new DelayQueue<ExpiringEntry>();
    private final PriorityBlockingQueue<ExpiringEntry<K>> pq = new PriorityBlockingQueue<>();

    public ExpiringMapStorage() {
        internalMap = new ConcurrentHashMap<K, V>();
        expiringEntries = new WeakHashMap<K, ExpiringEntry<K>>();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        Runnable task = () -> {
            cleanup();
        };

        scheduler.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
    }

    public V put(K key, V value, long lifeTimeMillis) {
        ExpiringEntry delayedKey = new ExpiringEntry(key, lifeTimeMillis);
        ExpiringEntry oldKey = expiringEntries.put((K) key, delayedKey);
        if (oldKey != null) {
            expireKey(oldKey);
            expiringEntries.put((K) key, delayedKey);
        }
        delayQueue.offer(delayedKey);
        pq.add(delayedKey);
        return internalMap.put(key, value);
    }

    public V get(K key) {
        return internalMap.get((K) key);
    }

    public boolean renewKey(K key) {
        ExpiringEntry<K> delayedKey = expiringEntries.get((K) key);
        if (delayedKey != null) {
            delayedKey.renew();
            return true;
        }
        return false;
    }

    public boolean updateLifeTimeMillis(K key, long maxLifeTimeMillis) {
        ExpiringEntry<K> delayedKey = expiringEntries.get((K) key);
        if (delayedKey != null) {
            delayedKey.renew();
            delayedKey.updateMaxLifeTimeMillis(maxLifeTimeMillis);
            return true;
        }
        return false;
    }

    private void expireKey(ExpiringEntry<K> delayedKey) {
        if (delayedKey != null) {
            delayedKey.expire();
            cleanup();
        }
    }

    private void cleanup() {
        ExpiringEntry<K> delayedKey = delayQueue.poll();

        while (delayedKey != null) {
            internalMap.remove(delayedKey.getKey());
            expiringEntries.remove(delayedKey.getKey());
            delayedKey = delayQueue.poll();
        }
    }

    private class ExpiringEntry<K> implements Delayed {

        private long startTime = System.currentTimeMillis();
        private long maxLifeTimeMillis;
        private final K key;

        public ExpiringEntry(K key, long maxLifeTimeMillis) {
            this.maxLifeTimeMillis = maxLifeTimeMillis;
            this.key = key;
        }

        public K getKey() {
            return key;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ExpiringEntry<K> other = (ExpiringEntry<K>) obj;
            if (this.key != other.key && (this.key == null || !this.key.equals(other.key))) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 31 * hash + (this.key != null ? this.key.hashCode() : 0);
            return hash;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(getDelayMillis(), TimeUnit.MILLISECONDS);
        }

        private long getDelayMillis() {
            return (startTime + maxLifeTimeMillis) - System.currentTimeMillis();
        }

        public void renew() {
            startTime = System.currentTimeMillis();
        }

        public void expire() {
            startTime = System.currentTimeMillis() - maxLifeTimeMillis - 1;
        }

        public void updateMaxLifeTimeMillis(long maxLifeTimeMillis) {
            this.maxLifeTimeMillis = maxLifeTimeMillis;
        }

        @Override
        public int compareTo(Delayed that) {
            return Long.compare(this.getDelayMillis(), ((ExpiringEntry) that).getDelayMillis());
        }
    }
}