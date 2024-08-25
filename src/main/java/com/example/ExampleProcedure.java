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

public class ExampleProcedure {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    private static final ExpiringQueue<String, DataStorage> storage = new ExpiringQueue<>();

    @Procedure(name = "com.example.exampleProcedure", mode = Mode.READ)
    public Stream<ExampleResult> exampleProcedure(@Name("param") String param) {
        return Stream.of(new ExampleResult("Hello, " + param));
    }

    @Procedure(name = "com.example.getAllShortestPaths", mode = Mode.READ)
    public Stream<ExamplePath> getAllShortestPaths(@Name("startNode") Node startNode,
            @Name("endNode") Node endNode) {

        // Define a simple cost evaluator
        CostEvaluator<Double> costEvaluator = (relationship, direction) -> 1.0; // Cost of 1.0 for each relationship

        // Define a PathExpander
        PathExpander<Double> expander = new PathExpander<Double>() {
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

        // Define a PathInterest
        PathInterest<Double> interest = PathInterestFactory.single(0.0);

        // Create an instance of the Dijkstra algorithm
        PathFinder<WeightedPath> dijkstra = new Dijkstra(
                expander, // PathExpander
                costEvaluator, // CostEvaluator
                0.0, // Epsilon
                interest // PathInterest
        );

        // Execute the Dijkstra algorithm
        Iterable<WeightedPath> paths = dijkstra.findAllPaths(startNode, endNode);

        return Stream.of(new ExamplePath(ValueUtils.of(paths)));
    }

    @Procedure(name = "com.example.yenKShortestPaths", mode = Mode.READ)
    public Stream<ExamplePath> yenKShortestPaths(@Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k) {
        // Log log = logProvider.getLog(getClass());
        // log = logProvider.getLog(ExampleProcedure.class);
        // long = 0;
        // long = 0;
        // long LOG3 = 0;
        AtomicLong LOG1 = new AtomicLong(0);
        AtomicLong LOG2 = new AtomicLong(0);
        AtomicLong LOG3 = new AtomicLong(0);
        AtomicLong LOG4 = new AtomicLong(0);
        AtomicLong LOG5 = new AtomicLong(0);
        // long startTime1 = System.nanoTime(); // Start time measurement

        // Your code here...

        // Define a simple cost evaluator
        // CostEvaluator<Double> costEvaluator = (relationship, direction) -> 1.0; //
        // Cost of 1.0 for each relationship
        DataStorage data = new DataStorage(startNode, endNode, (relationship, direction) -> 1.0);

        storage.put("ssa", data, 2000);

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

        PathFinder<WeightedPath> dijkstra = new Dijkstra(baseExpander, data.costEvaluator, data.epsilon, data.interest);

        // WeightedPath shortestPath = dijkstra.findSinglePath(startNode, endNode);
        WeightedPath shortestPaths = dijkstra.findSinglePath(data.startNode, data.endNode);

        // PriorityQueue<WeightedPath> potentialPaths = new PriorityQueue<>(
        // Comparator.comparingDouble(WeightedPath::weight));
        if (shortestPaths == null) {
            return Stream.empty();
        }

        PriorityQueue<WeightedPath> potentialPaths = new PriorityQueue<>(
                Comparator.comparingDouble(WeightedPath::weight));

        PriorityQueue<WeightedPath> KPaths = new PriorityQueue<>(
                Comparator.comparingDouble(WeightedPath::weight));

        potentialPaths.add(shortestPaths);
        KPaths.add(shortestPaths);

        // log.info(String.format("potentialPaths: %s", potentialPaths.size()));
        // log.info(String.format("KPaths: %s", KPaths.size()));
        // List<WeightedPath> kPaths = StreamSupport
        // .stream(dijkstra.findAllPaths(startNode, endNode).spliterator(), false)
        // .collect(Collectors.toList());

        // List<WeightedPath> kPaths = new ArrayList<>(shortestPaths);

        // List<WeightedPath> kPaths =
        // StreamSupport.stream(dijkstra.findAllPaths(startNode, endNode).spliterator(),
        // false)
        // .collect(Collectors.toList());
        // List<WeightedPath> kPaths =
        // StreamSupport.stream(dijkstra.findAllPaths(startNode, endNode).spliterator(),
        // false)
        // .collect(Collectors.toList());
        // Iterable<WeightedPath> paths = dijkstra.findAllPaths(startNode, endNode);

        // long endTime4 = System.nanoTime(); // End time measurement

        // double durationInSeconds2 = (endTime2 - startTime2) / 1_000_000_000.0;

        // LOG4.addAndGet(endTime4 - startTime4);

        // kPaths.add(shortestPath);

        // Yen's algorithm iterations
        // for (WeightedPath shortestPath2: shortestPaths) {
        HashSet<Relationship> ignoreRelationships = new HashSet<>(
        // StreamSupport.stream(shortestPathRelationships.spliterator(), false)
        // .collect(Collectors.toSet())
        );

        for (int i = 0; KPaths.size() < k; i++) {
            if (potentialPaths.isEmpty()) {
                break;
            }
            WeightedPath shortestPath = potentialPaths.poll();
            List<Node> shortestPathNodes = StreamSupport.stream(shortestPath.nodes().spliterator(), false)
                    .collect(Collectors.toList());
            List<Relationship> shortestPathRelationships = StreamSupport
                    .stream(shortestPath.relationships().spliterator(), false)
                    .collect(Collectors.toList());
            for (int j = 0; j < shortestPathNodes.size() - 1; j++) {
                // long startTime2 = System.nanoTime(); // Start time measurement

                // Your code here...
                Node spurNode = shortestPathNodes.get(j);
                Node ignoreNode = shortestPathNodes.get(j + 1);
                Relationship ignoreRelationship = shortestPathRelationships.get(j);
                ignoreRelationships.add(ignoreRelationship);

                log.info(String.format("spurNode.phoneKey: %s", spurNode.getProperties("phoneKey")));
                // Set<Relationship> ignoreRelationships = new HashSet<>();
                // HashSet<Relationship> ignoreRelationships = new HashSet<>(
                // StreamSupport.stream(shortestPathRelationships.spliterator(), false)
                // .collect(Collectors.toSet()));
                // WeightedPath shortestPaths = shortestPath.;
                // for (WeightedPath path : kPaths) {

                // if (path.length() > j && StreamSupport.stream(path.nodes().spliterator(),
                // false)
                // .collect(Collectors.toList()).subList(0, j)
                // .equals(StreamSupport.stream(shortestPath.nodes().spliterator(), false)
                // .collect(Collectors.toList()).subList(0, j))) {

                // ignoreRelationships.add(StreamSupport.stream(path.relationships().spliterator(),
                // false)
                // .collect(Collectors.toList()).get(j));
                // }
                // }

                // long endTime2 = System.nanoTime(); // End time measurement

                // double durationInSeconds2 = (endTime2 - startTime2) / 1_000_000_000.0;

                // LOG2.addAndGet(endTime2 - startTime2);
                // log.info(String.format("2. Execution time: %.15f seconds%n",
                // durationInSeconds2));

                // Create a new PathExpander that ignores certain relationships
                PathExpander<Double> expander = new PathExpander<Double>() {
                    @Override
                    public ResourceIterable<Relationship> expand(Path path, BranchState<Double> state) {

                        // long startTime3 = System.nanoTime(); // Start time measurement
                        List<Relationship> filteredRelationships = new ArrayList<>();

                        for (Relationship relationship : path.endNode().getRelationships(Direction.OUTGOING)) {
                            // Apply your filtering logic here
                            // if (relationship.getEndNode().getProperty("phoneKey") != ignoreNode
                            // .getProperty("phoneKey")) {
                            if (!ignoreRelationships.contains(relationship)) {

                                filteredRelationships.add(relationship);
                            }
                        }
                        // mvn clean package
                        // long endTime3 = System.nanoTime(); // End time measurement

                        // double durationInSeconds3 = () / 1_000_000_000.0;

                        // += (endTime3 - startTime3);
                        // LOG3.addAndGet(endTime3 - startTime3);
                        // log.info(String.format("3. Execution time: %.15f seconds%n",
                        // durationInSeconds3));
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
                dijkstra = new Dijkstra(expander, data.costEvaluator, 0.0, PathInterestFactory.single(0.0));
                // long startTime5 = System.nanoTime(); // Start time measurement

                WeightedPath spurPath = dijkstra.findSinglePath(spurNode, endNode);

                if (spurPath == null) {
                    break;
                }
                // Iterable<WeightedPath> spurPath = ;
                // long endTime5 = System.nanoTime(); // End time measurement

                // double durationInSeconds3 = () / 1_000_000_000.0;

                // += (endTime3 - startTime3);
                // LOG5.addAndGet(endTime5 - startTime5);

                WeightedPathImpl concatPath = concatPaths(shortestPath, 0, j, spurPath, 0, spurPath.length(),
                        log);
                log.info(String.format("START"));
                for (Node node : spurPath.nodes()) {
                    log.info(String.format("phoneKey: %s", node.getProperties("phoneKey")));
                }
                log.info(String.format("END"));

                potentialPaths.add(concatPath);
                KPaths.add(concatPath);
                // }
            }

            // MATCH (startNode:AllyNode {phoneKey: '+995598362399'}), (endNode:AllyNode
            // {phoneKey: '+995599992878'})
            // CALL com.example.yenKShortestPaths(startNode, endNode, 50)
            // YIELD paths
            // RETURN paths;
        }

        // if (potentialPaths.isEmpty()) {
        // break;
        // }
        //
        // Add the next shortest path
        // WeightedPath newPathSlice = potentialPaths.poll();
        // kPaths.add(potentialPaths.poll());
        // }

        // long endTime1 = System.nanoTime(); // End time measurement

        // LOG1 += (endTime1 - startTime1);
        // LOG1.addAndGet(endTime1 - startTime1);
        log.info(String.format("LOG1. Execution time: %.15f seconds%n", LOG1.get() / 1_000_000_000.0));
        log.info(String.format("LOG2. Execution time: %.15f seconds%n", LOG2.get() / 1_000_000_000.0));
        log.info(String.format("LOG3. Execution time: %.15f seconds%n", LOG3.get() / 1_000_000_000.0));
        log.info(String.format("LOG4. Execution time: %.15f seconds%n", LOG4.get() / 1_000_000_000.0));
        log.info(String.format("LOG5. Execution time: %.15f seconds%n", LOG5.get() / 1_000_000_000.0));

        List<WeightedPath> sortedList = new ArrayList<>();
        while (!KPaths.isEmpty()) {
            sortedList.add(KPaths.poll());
        }

        return sortedList.stream().map(path -> new ExamplePath(ValueUtils.of(path)));
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
        // builder.getStartNode()
        // log.info(String.format("111concatPaths START"));
        // for (Node node : path2.nodes()) {
        // log.info(String.format("phoneKey: %s", node.getProperties("phoneKey")));
        // }
        // log.info(String.format("111concatPaths END\n"));

        int index2 = 0;
        for (Relationship relationship : path2.relationships()) {
            if (index2 >= path2from && index2 < path2to) {
                // log.info(String.format("getStartNode1: %s",
                // relationship.getStartNode().getProperties("phoneKey")));
                // log.info(String.format("getEndNode1: %s",
                // relationship.getEndNode().getProperties("phoneKey")));
                builder = builder.push(relationship);
            }
            index2++;
        }

        CostEvaluator<Double> costEvaluator = (relationship, direction) -> 1.0;

        return new WeightedPathImpl(costEvaluator, builder.build());
    }

    /**
     * DataStorage
     */
    public class DataStorage {
        Node startNode;
        Node endNode;
        CostEvaluator<Double> costEvaluator;

        double epsilon = 0.0;
        PathInterest<Double> interest = PathInterestFactory.single(0.0);

        public DataStorage(Node startNode, Node endNode, CostEvaluator<Double> costEvaluator) {
            this.startNode = startNode;
            this.endNode = endNode;
            this.costEvaluator = costEvaluator;
        }
    }
}
