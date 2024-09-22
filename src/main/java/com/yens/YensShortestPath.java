package com.yens;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Description;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValueBuilder;

// import com.yens.YensShortestPath.CustomPath;
// import com.yens.YensShortestPath.Linker;
// import com.yens.YensShortestPath.LinkerType;
import com.yens.YensShortestPath.RelationshipFilter;
import com.yens.YensShortestPath.ResponsePath;

// import com.yens.YensShortestPath.YensProcessStorage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

// import org.neo4j.graphalgo.impl.util.Dijkstra;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Context;
import org.neo4j.graphdb.*;
import org.neo4j.cypher.internal.expressions.functions.E;
import org.neo4j.values.virtual.ListValue;

// import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.graphalgo.impl.util.PathImpl;
// import org.neo4j.graphalgo.impl.util.NodeImpl;;
import org.neo4j.graphalgo.impl.util.PathInterest;
import org.neo4j.graphalgo.impl.util.PathInterestFactory;

import org.neo4j.graphalgo.impl.util.WeightedPathImpl;
import org.neo4j.graphalgo.impl.path.Dijkstra;
import org.neo4j.graphdb.traversal.BranchState;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.management.relation.Relation;

import java.beans.Customizer;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeSet;
// import org.neo4j.graphalgo.PathImpl;

import org.neo4j.logging.Log;

import org.neo4j.kernel.impl.util.ValueUtils;
import java.util.concurrent.TimeUnit;

// MATCH (startNode:AllyNode {phoneKey: '+995598362399'}), (endNode:AllyNode
// {phoneKey: '+995591201013'})
// CALL gds.shortestPath.yens.stream('LAPTOP-0BIFBM6U-Fri Sep 13 2024 23:56:02 GMT+0400 (Georgia Standard Time)=0-BaSe-GrApH-PrOJeCtIoN', {
//     sourceNode: startNode,
//     targetNode: endNode,
//     nodeLabels: ['AllyNode'],
//     concurrency: 1,
//     relationshipTypes: ['KNOWS'],
//     relationshipWeightProperty: null,
//     k: 10
//   })
//  YIELD path 
// RETURN COLLECT {
//     UNWIND  nodes(path) as n
//     RETURN n.phoneKey
// };

// mvn clean package 

// CALL gds.graph.list() YIELD graphName

// MATCH (startNode:AllyNode {phoneKey: '+995598362399'}), (endNode:AllyNode
// {phoneKey: '+995591201013'})
// CALL custom.dijkstra("sadas",startNode, endNode, 10)
// YIELD paths
// RETURN paths
/**
 * DataStorage
 */

public class YensShortestPath {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public Transaction tx;

    public static class ResponsePath {
        public AnyValue paths;

        public ResponsePath(AnyValue paths) {
            this.paths = paths;
        }
    }

    public interface RelationshipFilter {
        int minSize = 0;

        ResourceIterable<Relationship> getRelationships(PathFinder path);

        public PathFinder concatPathsFinder(PathFinder path1, PathFinder path2, Relationship rel,
                double cost);

    }

    public interface CostEvaluator<T> {

        T getCost(Relationship relationship);
    }

    @Procedure(name = "custom.dijkstra", mode = Mode.READ)
    @Description("A custom procedure that returns a greeting message.")
    public Stream<ResponsePath> dijkstra(
            @Name("storageKey") String storageKey,
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k) {

        RelationshipFilter getRelationships = new RelationshipFilter() {
            @Override
            public ResourceIterable<Relationship> getRelationships(PathFinder path) {
                // new NodeProxy(path.getEndNode())
                return path.getEndNode().getRelationships(Direction.OUTGOING);
            };

            @Override
            public PathFinder concatPathsFinder(PathFinder path1, PathFinder path2,
                    Relationship rel,
                    double cost) {
                // double weight = path1.getWeight() + path2.getWeight() + cost;
                // log.info(path1.chain.getSize() + ":::" + path2.chain.getSize());
                return path1.concatPathsFinder(path2, rel, cost);
            };

        };
        RelationshipFilter getReverseRelationships = new RelationshipFilter() {
            @Override
            public ResourceIterable<Relationship> getRelationships(PathFinder path) {

                return path.getEndNode().getRelationships(Direction.INCOMING);
            };

            @Override
            public PathFinder concatPathsFinder(PathFinder path1, PathFinder path2,
                    Relationship rel,
                    double cost) {
                // double weight = path1.getWeight() + path2.getWeight() + cost;
                // log.info(path1.chain.getSize() + ":::" + path2.chain.getSize());
                return path1.concatReversePathsFinder(path2, rel, cost);
            };
        };
        // Priority queue for nodes to explore, ordered by shortest distance
        PriorityQueue<PathFinder> pq = new PriorityQueue<>(Comparator.comparingDouble(PathFinder::getWeight));
        Map<Node, PathFinder> forwardDistances = new HashMap<>();
        Map<Node, PathFinder> backForwardDistances = new HashMap<>();
        // Forward forward = new Forward();
        // Forward backForward = new Forward();
        //
        // CostEvaluator<Double> costEvaluator = (relationship) -> 1.0;
        CostEvaluator<Double> costEvaluator = (relationship) -> {
            return ((Number) relationship.getProperty("weight", 1.0)).doubleValue();
        };

        PathFinder startEntry = new PathFinder(forwardDistances, backForwardDistances, startNode,
                costEvaluator, getRelationships);
        PathFinder endEntry = new PathFinder(backForwardDistances, forwardDistances, endNode,
                costEvaluator, getReverseRelationships);

        pq.add(startEntry);
        pq.add(endEntry);

        forwardDistances.put(startNode, startEntry);
        // backForwardDistances.put(startNode, startEntry);
        // forwardDistances.put(endNode, endEntry);
        backForwardDistances.put(endNode, endEntry);

        LinkedList<ListValue> currentKPaths = new LinkedList<>();

        // HashSet<Node> visited = new HashSet<>();
        // // visited.add(startNode);
        // // visited.add(endNode);
        log.info("startNode: " + startNode.getProperties("phoneKey"));
        log.info("endNode: " + endNode.getProperties("phoneKey"));

        // int minSize = 0;
        while (!pq.isEmpty()) {
            PathFinder currentEntry = pq.poll();
            if (currentEntry.reverseMap.containsKey(currentEntry.getEndNode())) {
                continue;
            }
            // minSize++;
            // if (currentEntry.reverseMap.containsKey(currentEntry.getEndNode())
            // || currentEntry.map.containsKey(currentEntry.getEndNode())) {
            // continue;
            // }
            // log.info("COST: " + currentEntry.getWeight());
            // visited.add(currentEntry.getEndNode());
            // if (currentEntry.reverseMap.containsKey(currentEntry.getEndNode())) {
            // continue;
            // }
            // if (currentEntry.isEnded()) {
            // currentKPaths.add(currentEntry.toValue());
            // if (currentKPaths.size() >= k) {
            // return currentKPaths.stream()
            // .map(path -> new ResponsePath(path));
            // }
            // // }
            // }
            ResourceIterable<Relationship> sortedRelationships = currentEntry.getRelationships();
            // List<Relationship> sortedRelationships = currentEntry.getRelationships()
            // .stream()
            // .sorted(Comparator.comparingDouble(costEvaluator::getCost))
            // .collect(Collectors.toList());

            for (Relationship rel : sortedRelationships) {
                Node neighbor = rel.getOtherNode(currentEntry.getEndNode());

                // if(){}
                double weight = costEvaluator.getCost(rel);
                PathFinder reversePath = currentEntry.getFromReverseMap(neighbor);

                if (reversePath != null) {
                    if (reversePath.getWeight() > pq.peek().getWeight()) {
                        continue;
                    }
                    log.info(
                            "SIZE: " + currentKPaths.size() +
                                    ", W1:" + reversePath.getWeight() +
                                    ", W2:" + pq.peek().getWeight() + ", weight: " + weight);

                    // PathFinder newEntry = currentEntry.addRelationship(rel, weight, neighbor);
                    // if (newEntry.getWeight() < currentEntry.getCurrentMinCost(neighbor)) {
                    // currentEntry.map.put(neighbor, newEntry);
                    // }
                    // pq.add(newEntry);
                    // } else {

                    // if (reversePath.getWeight() <= pq.peek().getWeight()) {

                    // log.info("FINDED:" + rel);
                    // reversePath.BlockNode(currentEntry.getEndNode());</Relationship></Relationship>

                    // double newEntryWeight = currentEntry.getWeight() + reversePath.getWeight();
                    // double currentMinWeight = pq.peek().getWeight() + currentEntry.getWeight();
                    // if (newEntryWeight <= currentMinWeight) {
                    PathFinder pushEntry = currentEntry.relationshipFilter.concatPathsFinder(currentEntry,
                            reversePath, rel,
                            weight);
                    // pq.add(pushEntry);
                    currentKPaths.add(
                            pushEntry.toValue());
                    if (currentKPaths.size() >= k) {
                        return currentKPaths.stream()
                                .map(path -> new ResponsePath(path));
                    }
                    // }
                    // }
                    // }
                    // continue;
                }

                // double currentDistance =
                // currentEntry.getCurrentMinCost(currentEntry.getEndNode());
                // currentEntry.getWeight();
                // distances.getOrDefault(currentNode, Double.MAX_VALUE);
                // double newDistance = currentDistance + weight;
                // if (currentEntry.reverseMap.containsKey(neighbor.getElementId())) {
                // continue;
                // }
                PathFinder newEntry = currentEntry.addRelationship(rel, weight, neighbor);
                if (newEntry.getWeight() < currentEntry.getCurrentMinCost(neighbor)) {
                    currentEntry.map.put(neighbor, newEntry);
                    pq.add(newEntry);
                }
                // if (!currentEntry.reverseMap.containsKey(neighbor)) {

                // }

            }
        }

        if (currentKPaths.isEmpty())

        {
            return Stream.empty();
        }

        return currentKPaths.stream().map(path -> new ResponsePath(path));
    }

    public class ReversePathFinder {
    }

    public class Linker<E> implements Iterable<E> {
        private Linker<E> before; // Points to the previous node
        private E element; // The value of the current node

        private int size = 0;

        // Default constructor for an empty linker
        public Linker() {
        }

        // Constructor for a linker with an initial element
        public Linker(E element) {
            this.element = element;
            this.size = 1;
        }

        // Private constructor for internal usage (creating a new node with a link to
        // the previous node)
        private Linker(E element, Linker<E> before, int size) {
            this.element = element;
            this.before = before;
            this.size = size;
        }

        // Adds a new element by creating a new Linker node and linking it to the
        // current one
        public Linker<E> push(E element) {
            return new Linker<>(element, this, this.size + 1);
        }

        // Returns the current size of the linked list
        public int getSize() {
            return this.size;
        }

        @Override
        public Iterator<E> iterator() {
            return new LinkerIterator(this);
        }

        // Iterator class to traverse the Linker nodes
        private class LinkerIterator implements Iterator<E> {
            private Linker<E> current;

            // Initialize with the starting node
            public LinkerIterator(Linker<E> start) {
                this.current = start;
            }

            @Override
            public boolean hasNext() {
                return current != null && current.element != null;
            }

            @Override
            public E next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                E element = current.element;
                current = current.before; // Move to the previous node
                return element;
            }
        }
    }

    public class PathFinder {
        Map<Node, PathFinder> map;
        Map<Node, PathFinder> reverseMap;
        // String direction;
        // Boolean isBlockedDirection = false;
        // Set<Node> blockedNodes = new HashSet<>();</Node>

        Node endNode;
        // Node destinationNode;

        Linker<Relationship> chain;

        double weight = 0.000;
        CostEvaluator<Double> costEvaluator;
        RelationshipFilter relationshipFilter;

        PathFinder(
                Map<Node, PathFinder> map,
                Map<Node, PathFinder> reverseMap,
                Node endNode,
                // Node destinationNode,
                CostEvaluator<Double> costEvaluator,
                RelationshipFilter relationshipFilter) {
            this.map = map;
            this.reverseMap = reverseMap;
            this.endNode = endNode;
            // this.destinationNode = destinationNode;
            this.costEvaluator = costEvaluator;
            this.relationshipFilter = relationshipFilter;
            this.chain = new Linker<Relationship>();
            // this.direction = direction;

        }

        PathFinder(
                Map<Node, PathFinder> map,
                Map<Node, PathFinder> reverseMap,
                Node endNode,
                // Node destinationNode,</Relationship>
                double weight,
                CostEvaluator<Double> costEvaluator,
                RelationshipFilter relationshipFilter,
                Linker<Relationship> chain) {
            this.map = map;
            this.reverseMap = reverseMap;
            this.endNode = endNode;
            // this.destinationNode = destinationNode;
            this.weight = weight;
            this.costEvaluator = costEvaluator;
            this.relationshipFilter = relationshipFilter;
            this.chain = chain;
        }

        public double concat() {
            return this.weight;
        }

        public ResourceIterable<Relationship> getRelationships() {
            return this.relationshipFilter.getRelationships(this);
        }

        public double getWeight() {
            return this.weight;
        }

        // public void BlockNode(Node node) {
        // this.blockedNodes.add(node);
        // }

        // public boolean isBlockNode(Node node) {
        // return this.blockedNodes.contains(node);
        // }

        // public boolean isEnded() {
        // return this.destinationNode.equals(this.endNode);
        // }

        public PathFinder getFromReverseMap(Node node) {
            return reverseMap.get(node);
        }

        public double getCurrentMinCost(Node node) {
            PathFinder path = this.map.get(node);

            if (path == null) {
                return Double.MAX_VALUE;
            }
            return path.getWeight();
        }

        public PathFinder concatPathsFinder(PathFinder path, Relationship concatRel, double concatRelCost) {
            // path <---
            // this --->
            // log.info("---> path <---, this --->" +
            // this.destinationNode.getProperty("phoneKey"));
            // for (Relationship chainRel : this.chain) {

            // log.info("R1: " + chainRel.getStartNode().getProperty("phoneKey") + "R2: "
            // + chainRel.getEndNode().getProperty("phoneKey"));
            // }
            // log.info("---> END: path <---, this --->" +
            // this.destinationNode.getProperty("phoneKey"));

            // log.info("<--- path <---, this --->" +
            // path.destinationNode.getProperty("phoneKey"));
            // for (Relationship chainRel : path.chain) {

            // log.info("R1: " + chainRel.getStartNode().getProperty("phoneKey") + "R2: "
            // + chainRel.getEndNode().getProperty("phoneKey"));
            // }
            // log.info("<--- END: path <---, this --->" +
            // path.destinationNode.getProperty("phoneKey"));

            Linker<Relationship> newChain = path.chain.push(concatRel);
            for (Relationship chainRel : this.chain) {
                newChain = newChain.push(chainRel);
            }

            log.info("path <---, this --->");
            for (Relationship chainRel : newChain) {

                log.info("R1: " + chainRel.getStartNode().getProperty("phoneKey") + "R2: "
                        + chainRel.getEndNode().getProperty("phoneKey"));
            }
            log.info("END: path <---, this --->");

            return new PathFinder(
                    path.map,
                    path.reverseMap,
                    path.getEndNode(),
                    // path.destinationNode,
                    this.weight + path.getWeight() + concatRelCost,
                    path.costEvaluator,
                    path.relationshipFilter,
                    newChain);

        }

        public PathFinder concatReversePathsFinder(PathFinder path,
                Relationship concatRel, double concatRelCost) {
            // path --->
            // this <---

            Linker<Relationship> newChain = this.chain.push(concatRel);
            for (Relationship chainRel : path.chain) {
                newChain = newChain.push(chainRel);
            }
            log.info("path --->, this <---");
            for (Relationship chainRel : newChain) {

                log.info("R1: " + chainRel.getStartNode().getProperty("phoneKey") + "R2: "
                        + chainRel.getEndNode().getProperty("phoneKey"));
            }
            log.info("END: path --->, this <---");
            return new PathFinder(
                    this.map,
                    this.reverseMap,
                    this.getEndNode(),
                    // this.destinationNode,
                    // path.getEndNode() ,
                    this.weight + path.getWeight() + concatRelCost,
                    this.costEvaluator,
                    this.relationshipFilter,
                    newChain);
        }

        public ListValue toValue() {

            ListValueBuilder builder = ListValueBuilder
                    .newListBuilder((this.chain.getSize() * 2) + 0);
            for (Relationship chainRel : this.chain) {
                // log.info(chainRel + "1:::");
                builder.add(ValueUtils
                        .asAnyValue(this.getWeight() + ",BB," + this.getWeight() + ",BB,"
                                + chainRel.getStartNode().getProperty("phoneKey")));
                // builder.add(ValueUtils.asRelationshipValue(chainRel));
                builder.add(ValueUtils
                        .asAnyValue(this.getWeight() + ",BB," + this.getWeight() + ",BB,"
                                + chainRel.getEndNode().getProperty("phoneKey")));

            }
            // builder.add(ValueUtils.asAnyValue(weight + "BB" +
            // this.getEndNode().getProperty("phoneKey")));

            return builder.build();
        }

        public Node getEndNode() {
            return this.endNode;
        }

        public PathFinder addRelationship(Relationship rel, double relCost, Node newEndNode) {
            // this.builder.

            return new PathFinder(
                    this.map,
                    this.reverseMap,
                    newEndNode,
                    this.weight + relCost,
                    this.costEvaluator,
                    this.relationshipFilter,
                    this.chain.push(rel));
        }

        public void toWeightedPath() {
            // return new WeightedPathImpl(this.weight, this.builder.build());
        }
    }

}
