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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ExpiringMapStorage<K, V> {

    private final Map<K, V> internalMap;

    private final Map<K, ExpiringKey<K>> expiringKeys;

    private final DelayQueue<ExpiringKey> delayQueue = new DelayQueue<ExpiringKey>();

    public ExpiringMapStorage() {
        internalMap = new ConcurrentHashMap<K, V>();
        expiringKeys = new WeakHashMap<K, ExpiringKey<K>>();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        Runnable task = () -> {
            cleanup();
        };

        scheduler.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
    }

    public V put(K key, V value, long lifeTimeMillis) {
        ExpiringKey delayedKey = new ExpiringKey(key, lifeTimeMillis);
        ExpiringKey oldKey = expiringKeys.put((K) key, delayedKey);
        if (oldKey != null) {
            expireKey(oldKey);
            expiringKeys.put((K) key, delayedKey);
        }
        delayQueue.offer(delayedKey);
        return internalMap.put(key, value);
    }

    public V get(K key) {
        return internalMap.get((K) key);
    }

    public boolean renewKey(K key) {
        ExpiringKey<K> delayedKey = expiringKeys.get((K) key);
        if (delayedKey != null) {
            delayedKey.renew();
            return true;
        }
        return false;
    }

    public boolean updateLifeTimeMillis(K key, long maxLifeTimeMillis) {
        ExpiringKey<K> delayedKey = expiringKeys.get((K) key);
        if (delayedKey != null) {
            delayedKey.renew();
            delayedKey.updateMaxLifeTimeMillis(maxLifeTimeMillis);
            return true;
        }
        return false;
    }

    private void expireKey(ExpiringKey<K> delayedKey) {
        if (delayedKey != null) {
            delayedKey.expire();
            cleanup();
        }
    }

    private void cleanup() {
        ExpiringKey<K> delayedKey = delayQueue.poll();
        while (delayedKey != null) {
            internalMap.remove(delayedKey.getKey());
            expiringKeys.remove(delayedKey.getKey());
            delayedKey = delayQueue.poll();
        }
    }

    private class ExpiringKey<K> implements Delayed {

        private long startTime = System.currentTimeMillis();
        private long maxLifeTimeMillis;
        private final K key;

        public ExpiringKey(K key, long maxLifeTimeMillis) {
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
            final ExpiringKey<K> other = (ExpiringKey<K>) obj;
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
            return Long.compare(this.getDelayMillis(), ((ExpiringKey) that).getDelayMillis());
        }
    }
}