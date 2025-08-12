package com.bdpf.dijkstra;

import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.neo4j.values.AnyValue;

public class Dijkstra {

    public class DataStorage implements Closeable {
        Node startNode;
        Node endNode;

        Transaction tx;

        PriorityQueue<PathFinder> pq = new PriorityQueue<>(Comparator.comparingDouble(PathFinder::getWeight));
        // PriorityQueue<PathFinder> backForwardPq = new PriorityQueue<>(
        // Comparator.comparingDouble(PathFinder::getWeight));

        Map<Long, PathFinder> forwardMap = new HashMap<>();
        Map<Long, PathFinder> backForwardMap = new HashMap<>();

        LinkedList<AnyValue> currentKPaths = new LinkedList<>();

        public DataStorage(
                GraphDatabaseService db,
                Node startNode,
                Node endNode,
                CostEvaluator<Double> costEvaluator,
                RelationshipFilter getRelationships,
                RelationshipFilter getReverseRelationships) {

            this.tx = db.beginTx();
            this.startNode = this.tx.getNodeByElementId(startNode.getElementId());
            this.endNode = this.tx.getNodeByElementId(endNode.getElementId());

            // long startId = Dijkstra.getRelationshipId(this.startNode.getId(),
            // this.endNode.getId());
            // long endId = Dijkstra.getRelationshipId(this.endNode.getId(),
            // this.startNode.getId());

            // Set<Long> forwardVisitedNodes = new HashSet<>();
            // Set<Long> backForwardVisitedNodes = new HashSet<>();

            PathFinder startEntry = new PathFinder(this.forwardMap, this.backForwardMap, this.startNode, costEvaluator,
                    getRelationships);
            PathFinder endEntry = new PathFinder(this.backForwardMap, this.forwardMap, this.endNode, costEvaluator,
                    getReverseRelationships);
            this.pq.add(startEntry);
            this.pq.add(endEntry);

            this.forwardMap.put(startEntry.getRelationshipId(this.startNode.getId(), this.endNode.getId()), startEntry);
            // this.backForwardMap.put(startEntry.getRelationshipId(this.endNode.getId(),
            // this.startNode.getId()),
            // endEntry);

            // ;
            // Map<Node, PathFinder> map,
            // Map<Node, PathFinder> reverseMap,
            // Node endNode,
            // CostEvaluator<Double> costEvaluator,
            // RelationshipFilter relationshipFilter,
            // long id
        }

        @Override
        public void close() {
            this.tx.close();
        }
    }

    private static final ExpiringMapStorage<String, DataStorage> storage = new ExpiringMapStorage<String, DataStorage>();

    public static boolean isLessThanOrEqual(double a, double b) {
        double epsilon = 1e-6;
        return a < b || Math.abs(a - b) < epsilon;
    }

    public static void logAppResources(Log logger) {

        Runtime runtime = Runtime.getRuntime();

        long maxHeap = runtime.maxMemory();
        long totalHeap = runtime.totalMemory();
        long freeHeap = runtime.freeMemory();
        long usedHeap = totalHeap - freeHeap;

        logger.info("🧠 Heap Usage:");
        logger.info("  Max Heap     : " + formatBytes(maxHeap));
        logger.info("  Total Heap   : " + formatBytes(totalHeap));
        logger.info("  Used Heap    : " + formatBytes(usedHeap));
        logger.info("  Free Heap    : " + formatBytes(freeHeap));

        int availableProcessors = runtime.availableProcessors();
        logger.info("🧮 Available CPU cores: " + availableProcessors);

        int liveThreads = ManagementFactory.getThreadMXBean().getThreadCount();
        logger.info("🧵 Live Threads: " + liveThreads);
    }

    private static String formatBytes(long bytes) {
        long MB = 1024 * 1024;
        return String.format("%.2f MB", bytes / (double) MB);
    }

    // public static long getRelationshipId(long id1, long id2) {
    // // return (((id1 + id2) * (id1 + id2 + 1)) / 2) + id2;

    // long sum = id1 + id2;
    // long min = id1 < id2 ? id1 : id2;
    // long max = sum - min;
    // return (sum * (sum + 1) / 2) + max;
    // }

    public Stream<ResponsePath> find(
            GraphDatabaseService db,
            Node startNode,
            Node endNode,
            long k,
            String storageKey,
            long storageExpirationSeconds,
            long timeoutSeconds,
            CostEvaluator<Double> costEvaluator,
            RelationshipFilter getRelationships,
            RelationshipFilter getReverseRelationships,
            Log log) {
        this.logAppResources(log);
        Dijkstra.storage.runCleaner();
        ExpiringMapStorage.ExpiringEntry<DataStorage, String> storageEntry = Dijkstra.storage.get(storageKey);
        if (storageEntry == null) {
            storageEntry = Dijkstra.storage.put(
                    storageKey,
                    new DataStorage(db, startNode, endNode, costEvaluator, getRelationships, getReverseRelationships),
                    storageExpirationSeconds);
        } else {
            Dijkstra.storage.updateExpirationTimeSeconds(storageKey, timeoutSeconds);
        }
        storageEntry.lock();
        try {
            DataStorage storage = storageEntry.getValue();

            // PriorityQueue<PathFinder> backForwardPq = storage.backForwardPq;
            PriorityQueue<PathFinder> pq = storage.pq;

            LinkedList<AnyValue> currentKPaths = storage.currentKPaths;

            long timeoutAt = System.currentTimeMillis() + 1000 * timeoutSeconds;

            if (currentKPaths.size() >= k) {
                return currentKPaths.stream()
                        .limit(k)
                        .map(path -> new ResponsePath(path));
            }
            double minWeight = Double.NEGATIVE_INFINITY;
            PathFinder currentEntry = pq.poll();

            while ((currentKPaths.size() < k && System.currentTimeMillis() < timeoutAt) &&
                    !Dijkstra.storage.isHeapAboveLimit(0.95) && currentEntry != null) {

                currentEntry.map.put(currentEntry.getId(), currentEntry);

                Iterable<Relationship> filteredRelationships = currentEntry.relationshipFilter
                        .getRelationships(currentEntry);
                for (Relationship rel : filteredRelationships) {
                    Node neighbor = rel.getOtherNode(currentEntry.getEndNode());

                    Double weight = costEvaluator.getCost(rel, currentEntry);
                    PathFinder newEntry = currentEntry.addRelationship(rel, weight, neighbor);
                    PathFinder reverseMap = currentEntry.reverseMap.get(newEntry.getReverseId());

                    // }
                    if (reverseMap != null) {
                        if (isLessThanOrEqual(minWeight, reverseMap.getWeight())
                                && !currentEntry.isBlockNode2(reverseMap)) {
                            currentKPaths.add(newEntry.relationshipFilter.toValue(newEntry, reverseMap));
                            minWeight = reverseMap.getWeight();
                        }
                        continue;
                    }
                    if (currentEntry.map.containsKey(newEntry.getReverseId())) {
                        continue;
                    }

                    pq.add(newEntry);
                }

                currentEntry = pq.poll();
            }

            if (currentKPaths.isEmpty()) {
                Dijkstra.storage.remove(storageKey);
                storage.close();
                return Stream.empty();
            }

            return currentKPaths.stream().map(path -> new ResponsePath(path));
        } catch (Exception e) {
            log.error("Error in shortestPath for storageKey: %s", storageKey, e);
            throw new RuntimeException("Failed to compute path", e);
        } finally {
            storageEntry.unlock();
            Dijkstra.storage.runCleaner();
        }
    }

    // public double walkOn(
    // Log log,
    // PathFinder currentEntry,
    // Map<Long, PathFinder> map,
    // Map<Long, PathFinder> reverseMap,
    // PriorityQueue<PathFinder> pq,
    // LinkedList<AnyValue> currentKPaths,
    // CostEvaluator<Double> costEvaluator,
    // RelationshipFilter relationshipFilter,
    // double minWeight,
    // long k) {
    // log.info("IDD: " + currentEntry.getId());
    // log.info("from: " + currentEntry.getEndNode().getProperty("phoneKey"));
    // // if (reverseMap.containsKey(currentEntry.getId())) {

    // // }
    // // if (reverseMap.containsKey(currentEntry.getId()) &&
    // // currentEntry.chain.getSize() != 0) {
    // // return minWeight;
    // // // }
    // if (map.containsKey(currentEntry.getId())) {
    // return minWeight;
    // }

    // map.put(currentEntry.getId(), currentEntry);
    // Iterable<Relationship> sortedRelationships =
    // relationshipFilter.getRelationships(currentEntry);
    // for (Relationship rel : sortedRelationships) {
    // Node neighbor = rel.getOtherNode(currentEntry.getEndNode());
    // long relId = this.getRelationshipId(currentEntry.getEndNode().getId(),
    // neighbor.getId());

    // Double weight = costEvaluator.getCost(rel, currentEntry);
    // PathFinder newEntry = currentEntry.addRelationship(rel, weight, neighbor,
    // relId);
    // // if (currentKPaths.size() >= k) {
    // // return minWeight;
    // // }
    // PathFinder reversePath = reverseMap.get(relId);
    // if (reversePath != null) {
    // currentKPaths.add(relationshipFilter.toValue(newEntry, reversePath));
    // // reverseMap.remove(relId);
    // // map.remove(currentEntry.getId());

    // }
    // if (!map.containsKey(currentEntry.getId())) {
    // pq.add(newEntry);
    // }

    // log.info("to: " + neighbor.getProperty("phoneKey") + " weight: " + weight + "
    // oldgetWeight: "
    // + currentEntry.getWeight() + " getWeight: " + newEntry.getWeight());

    // }
    // return minWeight;
    // }
}
