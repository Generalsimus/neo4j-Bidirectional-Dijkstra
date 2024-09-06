package com.yens;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Description;
import org.neo4j.values.AnyValue;

import com.yens.YensShortestPath.ResponsePath;

// import com.yens.YensShortestPath.YensProcessStorage;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Context;
import org.neo4j.graphdb.*;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.graphalgo.impl.util.PathImpl;
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

    // private static ExpiringQueue<String, YensProcessStorage> storage = new
    // ExpiringQueue<>();
    // ExpiringMap<String, String> expiringCache = new ExpiringMap<String,
    // String>();
    // @Context
    // private final static ExpiringMap<String, YensProcessStorage> storage = new
    // ExpiringMap<>(10, TimeUnit.SECONDS);
    // private final static AtomicReference<ExpiringMap<String, YensProcessStorage>>
    // storage = new AtomicReference<>(

    @Context
    public Transaction tx;

    public interface RelationshipFilter<STATE> {
        Iterator<Relationship> getRelationships(CustomPath path);

        Iterator<Relationship> getReverseRelationships(CustomPath path);
    }

    @Procedure(name = "com.dijkstra.shortestPaths", mode = Mode.READ)
    public Stream<ResponsePath> dijkstraShortestPaths(
            @Name("storageKey") String storageKey,
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k) {
        CostEvaluator<Double> costEvaluator = (relationship, direction) -> 1.0;
        PathInterest<Double> interest = PathInterestFactory.single(0.0);
        double epsilon = 0.0;

        LinkedList<WeightedPath> currentKPaths = new LinkedList<>();
        // HashSet<String> blockedPaths = new HashSet<>();
        // getPathId

        // while (currentKPaths.size() < k) {
        //
        PathExpander<Double> baseExpander = new PathExpander<Double>() {
            @Override
            public ResourceIterable<Relationship> expand(Path path, BranchState<Double> state) {
                List<Relationship> filteredRelationships = new ArrayList<>();

                for (Relationship relationship : path.endNode().getRelationships(Direction.OUTGOING)) {

                    if (relationship.getEndNode().equals(endNode)) {
                        // if ((currentKPaths.size() >= k)) {
                        // // currentKPaths.add(null)
                        // // path
                        // }
                        // // log.info("FIND NODE");
                        if (currentKPaths.size() < k) {

                            PathImpl.Builder pathBuilder = new PathImpl.Builder(path.startNode());
                            for (Relationship pathRelationship : path.relationships()) {
                                pathBuilder = pathBuilder.push(pathRelationship);
                            }
                            pathBuilder = pathBuilder.push(relationship);

                            currentKPaths.add(new WeightedPathImpl(costEvaluator, pathBuilder.build()));
                        }
                        if (currentKPaths.size() == (k - 1)) {
                            List<Relationship> endRelationships = new ArrayList<>();
                            endRelationships.add(relationship);

                            return Iterables.asResourceIterable(endRelationships);
                        }
                        // return new WeightedPathImpl(costEvaluator, builder.build());
                        // currentKPaths
                    } else {
                        filteredRelationships.add(relationship);
                    }

                }
                return Iterables.asResourceIterable(filteredRelationships);
            }

            @Override
            public PathExpander<Double> reverse() {
                return new PathExpander<Double>() {
                    @Override
                    public ResourceIterable<Relationship> expand(Path path, BranchState<Double> state) {
                        return Iterables
                                .asResourceIterable(path.endNode().getRelationships(Direction.OUTGOING));
                    }

                    @Override
                    public PathExpander<Double> reverse() {
                        return this; // Returning the current expander as reverse
                    }
                };
            }
        };
        PathFinder<WeightedPath> dijkstra = new Dijkstra(baseExpander, costEvaluator, epsilon, interest);
        // WeightedPath shortestPath =
        dijkstra.findSinglePath(startNode, endNode);

        // if (shortestPath != null) {
        // currentKPaths.add(shortestPath);
        // blockedPaths.add(getPathId(shortestPath));
        // }
        if (currentKPaths.size() == 0) {
            return Stream.empty();
        }
        // }

        return currentKPaths.stream()
                // .sorted(Comparator.comparingDouble(WeightedPath::weight))
                .map(path -> new ResponsePath(ValueUtils.of(path)));
    }

    public String getPathId(Path path) {
        String id = "";
        for (Relationship rel : path.relationships()) {
            id += rel.getElementId();
        }
        return id;
    }

    public static class ResponsePath {
        public AnyValue paths;

        public ResponsePath(AnyValue paths) {
            this.paths = paths;
        }
    }

    // public static final ExpiringMap<String, CacheStorage> storage = new
    // ExpiringMap<>(100, TimeUnit.SECONDS);

    @Procedure(name = "custom.dijkstra", mode = Mode.READ)
    @Description("A custom procedure that returns a greeting message.")
    public Stream<ResponsePath> dijkstra(
            @Name("storageKey") String storageKey,
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k) {
        // Priority queue for nodes to explore, ordered by shortest distance
        PriorityQueue<CustomPath> pq = new PriorityQueue<>(Comparator.comparingDouble(CustomPath::getWeight));
        Map<Node, Double> distances = new HashMap<>();
        CostEvaluator<Double> costEvaluator = (relationship, direction) -> 1.0;

        pq.add(new CustomPath(startNode, costEvaluator));
        // distances.put(startNode, 0.0);

        LinkedList<WeightedPath> currentKPaths = new LinkedList<>();

        while (!pq.isEmpty() && currentKPaths.size() < k) {
            CustomPath currentEntry = pq.poll();
            Node currentNode = currentEntry.getEndNode();

            // Lazy loading of relationships
            // Iterable<Relationship> relationships =
            // currentNode.getRelationships(Direction.OUTGOING);
            for (Relationship rel : currentNode.getRelationships(Direction.OUTGOING)) {
                Node neighbor = rel.getEndNode();

                double weight = costEvaluator.getCost(rel, Direction.OUTGOING);

                double currentDistance = distances.getOrDefault(currentNode, Double.MAX_VALUE);
                double newDistance = currentDistance + weight;

                if (newDistance <= distances.getOrDefault(neighbor, Double.MAX_VALUE)) {
                    CustomPath newEntry = currentEntry.addRelationship(rel, neighbor, Direction.OUTGOING);

                    if (endNode.equals(newEntry.getEndNode())) {
                        currentKPaths.add(newEntry.toWeightedPath());
                        continue;
                    }

                    distances.put(neighbor, newDistance);

                    pq.add(newEntry);
                }
            }
        }

        if (currentKPaths.isEmpty()) {
            return Stream.empty();
        }

        return currentKPaths.stream()
                .map(path -> new ResponsePath(ValueUtils.of(path)));
    }

    public class CustomPath {
        PathImpl.Builder builder;
        Node endNode;

        double weight = 0.000;
        CostEvaluator<Double> costEvaluator;

        CustomPath(Node startNode, CostEvaluator<Double> costEvaluator) {
            this.endNode = startNode;
            this.costEvaluator = costEvaluator;
            this.builder = new PathImpl.Builder(startNode);
        }

        CustomPath(PathImpl.Builder builder, Node endNode, double weight, CostEvaluator<Double> costEvaluator) {
            this.builder = builder;
            this.endNode = endNode;
            this.weight = weight;
            this.costEvaluator = costEvaluator;
        }

        public double getWeight() {
            return this.weight;
        }

        public Node getEndNode() {
            return this.endNode;
        }

        public CustomPath addRelationship(Relationship rel, Node newEndNode, Direction direction) {

            return new CustomPath(this.builder.push(rel), newEndNode,
                    this.weight + this.costEvaluator.getCost(rel, direction), this.costEvaluator);
        }

        public WeightedPathImpl toWeightedPath() {
            return new WeightedPathImpl(this.weight, this.builder.build());
        }
    }

}
