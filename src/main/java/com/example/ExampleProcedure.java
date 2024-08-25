package com.example.neo4j;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualPathValue;

// import com.example.ExampleProcedure.ExamplePath;
// import com.example.ExampleProcedure.ExampleResult;

// import com.example.ExampleProcedure.ExampleResult;
// import com.example.ExampleProcedure.PathLink;

// import com.example.ExampleProcedure.ChainPath;
// import com.example.ExampleProcedure.ExampleResult;
// import com.example.ExampleProcedure.PathResponse;
import java.util.concurrent.atomic.AtomicLong;

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
import org.neo4j.graphalgo.impl.util.WeightedPathIterator;
import org.neo4j.graphdb.traversal.BranchState;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.List;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

// import org.neo4j.graphdb.*;
import org.neo4j.graphalgo.*;
import org.neo4j.kernel.impl.util.ValueUtils;
// import org.neo4j.graphalgo.impl.path.Dijkstra;
// import org.neo4j.graphalgo.yenKShortestPaths;
import org.neo4j.procedure.*;

// mvn clean package 

// MATCH (startNode:AllyNode {phoneKey: '+995598362399'}), (endNode:AllyNode
// {phoneKey: '+995599992878'})
// CALL com.example.yenKShortestPaths(startNode, endNode, 10)
// YIELD paths
// RETURN COLLECT {
//     UNWIND  nodes(paths) as n
//     RETURN n.phoneKey
// };
public class ExampleProcedure {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    private static final ExpiringQueue<String, YensProcessStorage> storage = new ExpiringQueue<>();

    @Procedure(name = "com.example.yenKShortestPaths", mode = Mode.READ)
    public Stream<ExamplePath> yenKShortestPaths(
            @Name("storageKey") String storageKey,
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k) {

        AtomicLong LOG1 = new AtomicLong(0);
        AtomicLong LOG2 = new AtomicLong(0);
        AtomicLong LOG3 = new AtomicLong(0);
        AtomicLong LOG4 = new AtomicLong(0);
        AtomicLong LOG5 = new AtomicLong(0);

        YensProcessStorage data = getYensStorage(storageKey, startNode, endNode);

        if (data.KPaths.size() == 0) {
            return Stream.empty();
        }

        while (data.KPaths.size() < k && data.NextPath() != null)
            ;

        log.info(String.format("LOG1. Execution time: %.15f seconds%n", LOG1.get() / 1_000_000_000.0));
        log.info(String.format("LOG2. Execution time: %.15f seconds%n", LOG2.get() / 1_000_000_000.0));
        log.info(String.format("LOG3. Execution time: %.15f seconds%n", LOG3.get() / 1_000_000_000.0));
        log.info(String.format("LOG4. Execution time: %.15f seconds%n", LOG4.get() / 1_000_000_000.0));
        log.info(String.format("LOG5. Execution time: %.15f seconds%n", LOG5.get() / 1_000_000_000.0));

        List<WeightedPath> sortedList = new ArrayList<>();
        while (!data.KPaths.isEmpty()) {
            sortedList.add(data.KPaths.poll());
        }

        return sortedList.stream().map(path -> new ExamplePath(ValueUtils.of(path)));
    }

    public YensProcessStorage getYensStorage(String key, Node startNode, Node endNode) {
        YensProcessStorage data = storage.get(key);

        if (data != null) {
            return data;
        }

        data = new YensProcessStorage(startNode, endNode, (relationship, direction) -> 1.0);

        storage.put(key, data, 2000 * 100);

        return data;
    }

    public static class ExampleResult {
        public String message;

        public ExampleResult(String message) {
            this.message = message;
        }
    }

    public static class ExamplePath {
        public AnyValue paths;

        public ExamplePath(AnyValue paths) {
            this.paths = paths;
        }
    }

    public static WeightedPathImpl concatPaths(
            WeightedPath path1,
            int path1from,
            int path1to,
            WeightedPath path2,
            int path2from,
            int path2to,
            CostEvaluator<Double> costEvaluator,
            Log log) {
        PathImpl.Builder builder = new PathImpl.Builder(path1.startNode());

        // log.info(String.format("path1from: %d %n", path1from));
        // log.info(String.format("path1to: %d %n", path1to));

        // log.info(String.format("path2from: %d %n", path2from));
        // log.info(String.format("path2to: %d %n", path2to));

        // log.info(String.format("concatPaths START"));
        // for (Node node : path1.nodes()) {
        // log.info(String.format("phoneKey: %s", node.getProperties("phoneKey")));
        // }
        // log.info(String.format("concatPaths END\n"));

        // // ss = path2.()
        int index1 = 0;
        for (Relationship relationship : path1.relationships()) {
            if (index1 >= path1from && index1 < path1to) {
                // log.info(String.format("getStartNode2: %s",
                // relationship.getStartNode().getProperties("phoneKey")));
                // log.info(String.format("getEndNode2: %s",
                // relationship.getEndNode().getProperties("phoneKey")));
                builder = builder.push(relationship);
                // relationship.getStartNode();
            }
            index1++;
        }
        // path1.builder

        int index2 = 0;
        for (Relationship relationship : path2.relationships()) {
            if (index2 >= path2from && index2 < path2to) {
                builder = builder.push(relationship);
            }
            index2++;
        }

        return new WeightedPathImpl(costEvaluator, builder.build());
    }

    /**
     * DataStorage
     */
    public class YensProcessStorage {
        Node startNode;
        Node endNode;
        CostEvaluator<Double> costEvaluator;
        PriorityQueue<WeightedPath> potentialPaths;

        PriorityQueue<WeightedPath> KPaths;
        // int KIndex = 0;
        int shortPathIndex = 0;

        double epsilon = 0.0;
        PathInterest<Double> interest = PathInterestFactory.single(0.0);
        HashSet<Relationship> ignoreRelationships = new HashSet<>();

        WeightedPath shortestPath;
        List<Node> shortestPathNodes;
        List<Relationship> shortestPathRelationships;

        public YensProcessStorage(Node startNode, Node endNode, CostEvaluator<Double> costEvaluator) {
            this.startNode = startNode;
            this.endNode = endNode;
            this.costEvaluator = costEvaluator;
            // Store the shortest path in a list
            // List<WeightedPath> kPaths = new ArrayList<>();
            // PriorityQueue<WeightedPath> potentialPaths = new PriorityQueue<>(
            // Comparator.comparingDouble(WeightedPath::weight));
            // potentialPaths.element()

            // Initially find the shortest path

            // (path, state) -> Iterables.asResourceIterable(
            // path.endNode().getRelationships(Direction.OUTGOING));
            // long startTime4 = System.nanoTime(); // Start time measurement
            PathExpander<Double> baseExpander = new PathExpander<Double>() {
                @Override
                public ResourceIterable<Relationship> expand(Path path, BranchState<Double> state) {
                    return Iterables.asResourceIterable(path.endNode().getRelationships(Direction.OUTGOING));
                }

                @Override
                public PathExpander<Double> reverse() {
                    return new PathExpander<Double>() {
                        @Override
                        public ResourceIterable<Relationship> expand(Path path, BranchState<Double> state) {
                            return Iterables.asResourceIterable(path.endNode().getRelationships(Direction.INCOMING));
                        }

                        @Override
                        public PathExpander<Double> reverse() {
                            return this; // Returning the current expander as reverse
                        }
                    };
                }
            };

            PathFinder<WeightedPath> dijkstra = new Dijkstra(baseExpander, costEvaluator, epsilon, interest);

            // dijkstra.extends
            // WeightedPath shortestPath = dijkstra.findSinglePath(startNode, endNode);
            WeightedPath shortestPath = dijkstra.findSinglePath(startNode, endNode);

            // PriorityQueue<WeightedPath> potentialPaths = new PriorityQueue<>(
            // Comparator.comparingDouble(WeightedPath::weight));
            if (shortestPath != null) {
                this.potentialPaths = new PriorityQueue<>(
                        Comparator.comparingDouble(WeightedPath::weight));

                this.KPaths = new PriorityQueue<>(
                        Comparator.comparingDouble(WeightedPath::weight));

                this.potentialPaths.add(shortestPath);
                this.KPaths.add(shortestPath);

                this.shortestPath = this.potentialPaths.poll();
                this.shortestPathNodes = StreamSupport.stream(shortestPath.nodes().spliterator(), false)
                        .collect(Collectors.toList());
                this.shortestPathRelationships = StreamSupport
                        .stream(shortestPath.relationships().spliterator(), false)
                        .collect(Collectors.toList());

            }

        }

        public WeightedPath NextPath() {
            boolean shortestPathIndexEnded = this.shortPathIndex >= (this.shortestPathNodes.size() - 1);
            if (this.potentialPaths.isEmpty() && shortestPathIndexEnded) {
                return null;
            }
            // this.KIndex++;
            if (shortestPathIndexEnded) {
                this.shortPathIndex = 0;

                this.shortestPath = this.potentialPaths.poll();
                this.shortestPathNodes = StreamSupport.stream(this.shortestPath.nodes().spliterator(), false)
                        .collect(Collectors.toList());
                this.shortestPathRelationships = StreamSupport
                        .stream(shortestPath.relationships().spliterator(), false)
                        .collect(Collectors.toList());
            }
            int shortPathIndex = this.shortPathIndex;
            this.shortPathIndex++;

            Node spurNode = this.shortestPathNodes.get(shortPathIndex);
            Node ignoreNode = shortestPathNodes.get(shortPathIndex + 1);
            Relationship ignoreRelationship = shortestPathRelationships.get(shortPathIndex);
            this.ignoreRelationships.add(ignoreRelationship);

            log.info(String.format("spurNode.phoneKey: %s", spurNode.getProperties("phoneKey")));
            HashSet<Relationship> ignoreRelationships = this.ignoreRelationships;
            // Create a new PathExpander that ignores certain relationships
            PathExpander<Double> expander = new PathExpander<Double>() {
                @Override
                public ResourceIterable<Relationship> expand(Path path, BranchState<Double> state) {
                    // long startTime3 = System.nanoTime(); // Start time measurement
                    List<Relationship> filteredRelationships = new ArrayList<>();

                    for (Relationship relationship : path.endNode().getRelationships(Direction.OUTGOING)) {
                        // .getProperty("phoneKey")) {
                        if (!ignoreRelationships.contains(relationship)) {

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
                                    .asResourceIterable(path.endNode().getRelationships(Direction.INCOMING));
                        }

                        @Override
                        public PathExpander<Double> reverse() {
                            return this; // Returning the current expander as reverse
                        }
                    };
                }
            };

            // Find the shortest path from spurNode using the modified expander
            Dijkstra dijkstra = new Dijkstra(expander, this.costEvaluator, 0.0, PathInterestFactory.single(0.0));
            // long startTime5 = System.nanoTime(); // Start time measurement

            WeightedPath spurPath = dijkstra.findSinglePath(spurNode, endNode);

            if (spurPath == null) {
                return null;
            }

            WeightedPathImpl concatPath = concatPaths(shortestPath, 0, shortPathIndex, spurPath, 0,
                    spurPath.length(),
                    this.costEvaluator,
                    log);
            log.info(String.format("START"));
            for (Node node : spurPath.nodes()) {
                log.info(String.format("phoneKey: %s", node.getProperties("phoneKey")));
            }
            log.info(String.format("END"));

            this.potentialPaths.add(concatPath);
            this.KPaths.add(concatPath);
            return concatPath;
        }
    }
}
