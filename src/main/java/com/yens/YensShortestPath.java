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

// mvn clean package 

// MATCH (startNode:AllyNode {phoneKey: '+995598362399'}), (endNode:AllyNode
// {phoneKey: '+995599992878'})
// CALL custom.dijkstra("sada2ss",startNode, endNode, 10)
// YIELD paths
// RETURN COLLECT {
//     UNWIND range(0, size(nodes(paths))-2) as index 
//     RETURN nodes(paths)[index+1].phoneKey+'-'+id(relationships(paths)[index])  
// };

// MATCH (startNode:AllyNode {phoneKey: '+995598362399'}), (endNode:AllyNode
// {phoneKey: '+995599992878'})
// CALL custom.dijkstra("sadas",startNode, endNode, 10)
// YIELD paths
// RETURN COLLECT {
//     UNWIND  nodes(paths) as n
//     RETURN n.phoneKey
// };
// MATCH (startNode:AllyNode {phoneKey: '+995598362399'}), (endNode:AllyNode
// {phoneKey: '+995599992878'})
// CALL gds.shortestPath.yens.stream('${graphName}', {
//     sourceNode: startNode,
//     targetNode: endNode,
//     nodeLabels: ['AllyNode'],
//     concurrency: 1,
//     relationshipTypes: ['KNOWS'],
//     relationshipWeightProperty: 'weight',
//     k: 50
//   })
//   YIELD path
//   RETURN COLLECT {
//     UNWIND  nodes(paths) as n
//     RETURN n.phoneKey
// };
/**
 * DataStorage
 */

// class Storage {

// }

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
        ResourceIterable<Relationship> getRelationships(PathFinder path);

        public ListValue concatPathsAsRelationshipList(PathFinder path1, PathFinder path2, Relationship rel,
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

        // PathConcat concat = new PathConcat() {
        // @Override
        // public CustomPath concat(CustomPath from, CustomPath to, double
        // concatRelCost, Relationship concatRel) {
        // // new NodeProxy(path.getEndNode())
        // // CustomPath path,
        // return from.concat(to, concatRelCost, concatRel);
        // };
        // };
        // PathConcat reverseConcat = new PathConcat() {
        // @Override
        // public CustomPath concat(CustomPath from, CustomPath to, double
        // concatRelCost, Relationship concatRel) {
        // // new NodeProxy(path.getEndNode())
        // // CustomPath path,
        // return to.concat(from, concatRelCost, concatRel);
        // };
        // };

        RelationshipFilter getRelationships = new RelationshipFilter() {
            @Override
            public ResourceIterable<Relationship> getRelationships(PathFinder path) {
                // new NodeProxy(path.getEndNode())
                return path.getEndNode().getRelationships(Direction.OUTGOING);
            };

            @Override
            public ListValue concatPathsAsRelationshipList(PathFinder path1, PathFinder path2,
                    Relationship rel,
                    double cost) {
                double weight = path1.getWeight() + path2.getWeight() + cost;
                log.info(path1.chain.getSize() + ":::" + path2.chain.getSize());
                // List<Relationship> relList = new ArrayList<Relationship>();

                ListValueBuilder builder = ListValueBuilder
                        .newListBuilder(((path1.chain.getSize() + path2.chain.getSize()) * 2) + 2);
                for (Relationship chainRel : path1.chain) {
                    log.info(chainRel + "1:::");
                    // builder.add();
                    // builder.
                    builder.add(ValueUtils.asAnyValue(weight + "BB" + chainRel.getStartNode().getProperty("phoneKey")));
                    // builder.add(ValueUtils.asRelationshipValue(chainRel));
                    builder.add(ValueUtils.asAnyValue(weight + "BB" + chainRel.getEndNode().getProperty("phoneKey")));

                    // builder.add(ValueUtils.asNodeValue(chainRel.getStartNode()));
                    // builder.add(ValueUtils.asRelationshipValue(chainRel));
                    // builder.add(ValueUtils.asNodeValue(chainRel.getEndNode()));
                    // relList.add(chainRel);
                }

                builder.add(ValueUtils.asAnyValue(weight + "BB" + rel.getStartNode().getProperty("phoneKey")));
                // builder.add(ValueUtils.asRelationshipValue(chainRel));
                builder.add(ValueUtils.asAnyValue(weight + "BB" + rel.getEndNode().getProperty("phoneKey")));
                // relList.add(rel);
                for (Relationship chainRel : path2.chain) {
                    // log.info(chainRel + "2:::");
                    builder.add(ValueUtils.asAnyValue(weight + "BB" + chainRel.getStartNode().getProperty("phoneKey")));
                    // builder.add(ValueUtils.asRelationshipValue(chainRel));
                    builder.add(ValueUtils.asAnyValue(weight + "BB" + chainRel.getEndNode().getProperty("phoneKey")));

                    // builder.add(ValueUtils.asNodeValue(chainRel.getStartNode()));
                    // builder.add(ValueUtils.asRelationshipValue(chainRel));
                    // builder.add(ValueUtils.asNodeValue(chainRel.getEndNode()));
                }
                return builder.build();
            };
        };
        RelationshipFilter getReverseRelationships = new RelationshipFilter() {
            @Override
            public ResourceIterable<Relationship> getRelationships(PathFinder path) {

                return path.getEndNode().getRelationships(Direction.INCOMING);
            };

            @Override
            public ListValue concatPathsAsRelationshipList(PathFinder path1, PathFinder path2,
                    Relationship rel,
                    double cost) {
                log.info(path1.chain.getSize() + ":::" + path2.chain.getSize());
                double weight = path1.getWeight() + path2.getWeight() + cost;
                // List<Relationship> relList = new ArrayList<Relationship>();

                ListValueBuilder builder = ListValueBuilder
                        .newListBuilder(((path1.chain.getSize() + path2.chain.getSize()) * 2) + 2);
                for (Relationship chainRel : path2.chain) {
                    log.info(chainRel + "1:::");
                    // builder.add();
                    // builder.
                    builder.add(ValueUtils.asAnyValue(weight + "EE" + chainRel.getStartNode().getProperty("phoneKey")));
                    // builder.add(ValueUtils.asRelationshipValue(chainRel));
                    builder.add(ValueUtils.asAnyValue(weight + "EE" + chainRel.getEndNode().getProperty("phoneKey")));

                    // builder.add(ValueUtils.asNodeValue(chainRel.getStartNode()));
                    // builder.add(ValueUtils.asRelationshipValue(chainRel));
                    // builder.add(ValueUtils.asNodeValue(chainRel.getEndNode()));
                    // relList.add(chainRel);
                }

                builder.add(ValueUtils.asAnyValue(weight + "EE" + rel.getStartNode().getProperty("phoneKey")));
                // builder.add(ValueUtils.asRelationshipValue(chainRel));
                builder.add(ValueUtils.asAnyValue(weight + "EE" + rel.getEndNode().getProperty("phoneKey")));
                // relList.add(rel);
                for (Relationship chainRel : path1.chain) {
                    // log.info(chainRel + "2:::");
                    builder.add(ValueUtils.asAnyValue(weight + "EE" + chainRel.getStartNode().getProperty("phoneKey")));
                    // builder.add(ValueUtils.asRelationshipValue(chainRel));
                    builder.add(ValueUtils.asAnyValue(weight + "EE" + chainRel.getEndNode().getProperty("phoneKey")));

                    // builder.add(ValueUtils.asNodeValue(chainRel.getStartNode()));
                    // builder.add(ValueUtils.asRelationshipValue(chainRel));
                    // builder.add(ValueUtils.asNodeValue(chainRel.getEndNode()));
                }
                return builder.build();
            };
        };
        // Priority queue for nodes to explore, ordered by shortest distance
        PriorityQueue<PathFinder> pq = new PriorityQueue<>(Comparator.comparingDouble(PathFinder::getWeight));
        Map<Node, PathFinder> forwardDistances = new HashMap<>();
        Map<Node, PathFinder> backForwardDistances = new HashMap<>();
        // Forward forward = new Forward();
        // Forward backForward = new Forward();
        //
        CostEvaluator<Double> costEvaluator = (relationship) -> 1.0;

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

        HashSet<Node> visited = new HashSet<>();
        // visited.add(startNode);
        // visited.add(endNode);
        log.info("startNode: " + startNode.getProperties("phoneKey"));
        log.info("endNode: " + endNode.getProperties("phoneKey"));

        while (!pq.isEmpty() && currentKPaths.size() < k) {
            PathFinder currentEntry = pq.poll();
            // Node currentNode = currentEntry.getEndNode();

            // Lazy loading of relationships
            // Iterable<Relationship> relationships =
            // currentNode.getRelationships(Direction.OUTGOING);

            // visited.add(currentEntry.getEndNode());
            if (currentEntry.reverseMap.containsKey(currentEntry.getEndNode())) {
                continue;
            }
            for (Relationship rel : currentEntry.getRelationships()) {
                Node neighbor = rel.getOtherNode(currentEntry.getEndNode());

                double weight = costEvaluator.getCost(rel);

                double currentDistance = currentEntry.getCurrentMinCost(currentEntry.getEndNode());
                // currentEntry.getWeight();
                // distances.getOrDefault(currentNode, Double.MAX_VALUE);
                double newDistance = currentDistance + weight;
                PathFinder reversePath = currentEntry.getFromReverseMap(neighbor);

                // log.info("neighbor: " + neighbor.getProperties("phoneKey") + "isDestination:
                // "
                // + (reversePath == null) + ", Destination: "
                // + currentEntry.getEndNode().getProperties("phoneKey") +
                // ", ჩჩჩ: " + newDistance +
                // "::" +
                // currentEntry.getCurrentMinCost(neighbor));

                if (reversePath != null) {
                    log.info("FINDED:" + rel);
                    // reversePath concatPathsAsRelationshipList
                    //
                    currentKPaths.add(
                            currentEntry.concatPathsAsRelationshipList(reversePath, rel, weight));
                    // currentEntry.map.remove(neighbor);
                    // currentEntry.reverseMap.remove(neighbor);

                    // for (Relationship r : reversePath.builder.getClass()) {

                    // }
                    // currentKPaths.add(currentEntry.addRelationship(rel, weight,
                    // neighbor).toWeightedPath());
                    // currentKPaths.add(reversePath.addRelationship(rel, weight,
                    // neighbor).toWeightedPath());
                    // currentKPaths.add(reversePath.toWeightedPath());

                    // for (Relationship n : reversePath.getRelationships()) {
                    // log.info("s: " + n.getStartNode().getProperties("phoneKey") + ", e: "
                    // + n.getEndNode().getProperties("phoneKey"));

                    // }
                    // currentKPaths.add(currentEntry.concat(reversePath, weight,
                    // rel).toWeightedPath());

                }
                if (newDistance < currentEntry.getCurrentMinCost(neighbor)) {
                    PathFinder newEntry = currentEntry.addRelationship(rel, weight, neighbor);
                    pq.add(newEntry);
                    currentEntry.map.put(neighbor, newEntry);
                }
                // if (neighbor == null || newDistance <= neighbor.getWeight()) {
                // CustomPath newEntry = currentEntry.addRelationship(rel,
                // neighbor.getEndNode(),
                // Direction.OUTGOING);

                // if (endNode.equals(newEntry.getEndNode())) {
                // currentKPaths.add(newEntry.toWeightedPath());
                // continue;
                // }

                // // distances.put(neighbor, newDistance);

                // pq.add(newEntry);
                // }
            }
        }
        // LinkedList<List<Relationship>> currentKPaths = new LinkedList<>();

        if (currentKPaths.isEmpty()) {
            return Stream.empty();
        }

        return currentKPaths.stream()
                .map(path -> new ResponsePath(path));
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

        Node endNode;
        Node destinationNode;
        Linker<Relationship> chain;

        double weight = 0.000;
        CostEvaluator<Double> costEvaluator;
        RelationshipFilter relationshipFilter;

        PathFinder(
                Map<Node, PathFinder> map,
                Map<Node, PathFinder> reverseMap,
                Node endNode,
                CostEvaluator<Double> costEvaluator,
                RelationshipFilter relationshipFilter) {
            this.map = map;
            this.reverseMap = reverseMap;
            this.endNode = endNode;
            this.costEvaluator = costEvaluator;
            this.relationshipFilter = relationshipFilter;
            this.chain = new Linker<Relationship>();

        }

        PathFinder(
                Map<Node, PathFinder> map,
                Map<Node, PathFinder> reverseMap,
                Node endNode,
                double weight,
                CostEvaluator<Double> costEvaluator,
                RelationshipFilter relationshipFilter,
                Linker<Relationship> chain) {
            this.map = map;
            this.reverseMap = reverseMap;
            this.endNode = endNode;
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

        public PathFinder getFromReverseMap(Node node) {
            return reverseMap.get(node);
        }

        public double getCurrentMinCost(Node node) {
            PathFinder path = map.get(node);

            if (path == null) {
                return Double.MAX_VALUE;
            }
            return path.getWeight();
        }

        public ListValue concatPathsAsRelationshipList(PathFinder path,
                Relationship concatRel, double concatRelCost) {
            return this.relationshipFilter.concatPathsAsRelationshipList(this, path, concatRel, concatRelCost);
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
