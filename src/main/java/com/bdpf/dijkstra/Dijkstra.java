package com.bdpf.dijkstra;

import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.*;

import org.neo4j.values.AnyValue;

public class Dijkstra {

    public class DataStorage implements AutoCloseable {
        Node startNode;
        Node endNode;

        Transaction tx;
        PriorityQueue<PathFinder> pq = new PriorityQueue<>(Comparator.comparingDouble(PathFinder::getWeight));
        Map<Node, PathFinder> forwardDistances = new HashMap<>();
        Map<Node, PathFinder> backForwardDistances = new HashMap<>();

        LinkedList<AnyValue> currentKPaths = new LinkedList<>();

        public DataStorage(
                GraphDatabaseService db,
                Node startNode,
                Node endNode,
                CostEvaluator<Double> costEvaluator,
                RelationshipFilter getRelationships,
                RelationshipFilter getReverseRelationships) {

            this.tx = db.beginTx();
            // tx.
            this.startNode = this.tx.getNodeByElementId(startNode.getElementId());
            this.endNode = this.tx.getNodeByElementId(endNode.getElementId());

            PathFinder startEntry = new PathFinder(forwardDistances, backForwardDistances, this.startNode,
                    costEvaluator,
                    getRelationships);
            PathFinder endEntry = new PathFinder(backForwardDistances, forwardDistances, this.endNode, costEvaluator,
                    getReverseRelationships);
            pq.add(startEntry);
            pq.add(endEntry);
            // forwardDistances.put(startEntry.getEndNode(), startEntry);
        }

        @Override
        public void close() throws Exception {
            this.tx.close();
        }
    }

    private static final ExpiringMapStorage<String, DataStorage> storage = new ExpiringMapStorage<String, DataStorage>();

    public DataStorage getCachedStorage(
            GraphDatabaseService db,
            Node startNode,
            Node endNode,
            String storageKey,
            long storageExpirationSeconds,
            CostEvaluator<Double> costEvaluator,
            RelationshipFilter getRelationships,
            RelationshipFilter getReverseRelationships) {
        DataStorage storage = Dijkstra.storage.get(storageKey);
        if (storage == null) {
            storage = new DataStorage(db, startNode, endNode, costEvaluator, getRelationships,
                    getReverseRelationships);
            Dijkstra.storage.put(storageKey, storage, storageExpirationSeconds * 1000);
        } else {
            Dijkstra.storage.updateLifeTimeMillis(storageKey, storageExpirationSeconds * 1000);
        }
        return storage;
    }

    public static boolean isLessThanOrEqual(double a, double b) {
        double epsilon = 1e-6;
        return a < b || Math.abs(a - b) < epsilon;
    }

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
        DataStorage storage = this.getCachedStorage(db, startNode, endNode, storageKey, storageExpirationSeconds,
                costEvaluator, getRelationships,
                getReverseRelationships);
        // storage.lock();
        // new DataStorage(startNode, endNode, costEvaluator, getRelationships,
        // getReverseRelationships);
        PriorityQueue<PathFinder> pq = storage.pq;
        LinkedList<AnyValue> currentKPaths = storage.currentKPaths;

        long timeoutAt = System.currentTimeMillis() + 1000 * timeoutSeconds;

        if (currentKPaths.size() >= k) {
            return currentKPaths.stream()
                    .limit(k)
                    .map(path -> new ResponsePath(path));
        }
        double minWeight = 0;
        while (!pq.isEmpty() && System.currentTimeMillis() < timeoutAt) {
            PathFinder currentEntry = pq.poll();
            PathFinder reversePath = currentEntry.getFromReverseMap(currentEntry.getEndNode());

            if (reversePath != null) {
                double currentWeight = currentEntry.getWeight() + reversePath.getWeight();

                if (isLessThanOrEqual(minWeight, currentWeight)) {
                    minWeight = currentWeight;
                    currentKPaths.add(currentEntry.relationshipFilter.toValue(currentEntry, reversePath));
                    if (currentKPaths.size() >= k) {
                        return currentKPaths.stream()
                                .map(path -> new ResponsePath(path));
                    }
                }
                continue;
            }
            // if (!currentEntry.map.containsKey(currentEntry.getEndNode())
            // // && !currentEntry.reverseMap.containsKey(currentEntry.getEndNode())
            // ) {
            // if (currentE</AnyValue>ntry.getEndNode() == startNode ||
            // currentEntry.getEndNode() ==
            // endNode) {
            // continue;
            // }
            currentEntry.map.put(currentEntry.getEndNode(), currentEntry);
            // } else {
            // // continue;
            // }

            Iterable<Relationship> sortedRelationships = currentEntry.getRelationships();
            // log.info(" GET REL::" + sortedRelationships. + ":::" +
            // currentEntry.getEndNode().getProperty("phoneKey"));
            // if (currentEntry.chain.getSize() == 0) {
            // log.info("START" + (getRelationships == currentEntry.relationshipFilter));
            // }
            for (Relationship rel : sortedRelationships) {
                Node neighbor = rel.getOtherNode(currentEntry.getEndNode());

                // if (currentEntry.chain.getSize() == 0) {
                // log.info("FROM:::" + currentEntry.getEndNode().getProperty("phoneKey") +
                // "__TO:::"
                // + neighbor.getProperty("phoneKey"));
                // }
                // if (currentEntry.chain.getSize() == 0) {
                // log.info("BLO, " + currentEntry.isBlockNode(endNode) + ",LL:"
                // + currentEntry.getEndNode().getProperty("phoneKey") + "EE"
                // + neighbor.getProperty("phoneKey"));
                // }
                // if (currentEntry.isBlockNode(neighbor)) {
                // continue;
                // }
                // // || (currentEntry.map.containsKey(neighbor) &&
                // // currentEntry.reverseMap.containsKey(neighbor))
                // // || reversePath != null && neighbor.equals(reversePath.chain.element.from)
                // ) {
                // // || (reversePath != null && reversePath.chain.element != null
                // // && neighbor == reversePath.chain.element.from)

                // continue;
                // }

                // || currentEntry.reverseMap.containsKey(neighbor)
                // if(isLessThanOrEqual(prev, 0)){

                // }
                Double weight = costEvaluator.getCost(rel, currentEntry);

                PathFinder newEntry = currentEntry.addRelationship(rel, weight, neighbor);
                // log.info("SIZE: "+newEntry.chain.getSize() + "__weight:::" +
                // newEntry.getWeight());
                // log.info("QQQQUE:::" + neighbor.getProperty("phoneKey")+" weight:::" +
                // newEntry.getWeight() +" SIZE:"+ pq.size() +" CHAIN_SIZE:"+
                // newEntry.chain.getSize());
                // if (!currentEntry.map.containsKey(neighbor)) {

                pq.add(newEntry);
                // }
            }

        }

        if (currentKPaths.isEmpty()) {
            return Stream.empty();
        }

        return currentKPaths.stream().map(path -> new ResponsePath(path));

    }

    public void walkOn(PathFinder currentEntry, LinkedList<AnyValue> currentKPaths) {

    }
}
