package com.yens;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.TreeSet;
// import org.neo4j.graphalgo.PathImpl;

import org.neo4j.logging.Log;

import org.neo4j.kernel.impl.util.ValueUtils;
import java.util.concurrent.TimeUnit;

// mvn clean package 

// MATCH (startNode:AllyNode {phoneKey: '+995598362399'}), (endNode:AllyNode
// {phoneKey: '+995599992878'})
// CALL com.yens.shortestPaths("sada2ss",startNode, endNode, 10)
// YIELD paths
// RETURN COLLECT {
//     UNWIND range(0, size(nodes(paths))-2) as index 
//     RETURN nodes(paths)[index+1].phoneKey+'-'+id(relationships(paths)[index])  
// };

// MATCH (startNode:AllyNode {phoneKey: '+995598362399'}), (endNode:AllyNode
// {phoneKey: '+995599992878'})
// CALL com.yens.shortestPaths("sadas",startNode, endNode, 10)
// YIELD paths
// RETURN COLLECT {
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

    class CustomPath {
        PathImpl.Builder builder;
        double weightCost = 0.000;
        CostEvaluator<Double> costEvaluator;
        Node startNode;
        Node endNode;
        HashSet<String> blockedBetweenNode;

        int size = 0;

        public CustomPath(Node startNode, CostEvaluator<Double> costEvaluator) {
            this.builder = new PathImpl.Builder(startNode);
            this.startNode = startNode;
            this.endNode = startNode;
            this.blockedBetweenNode = new HashSet<>();
            this.costEvaluator = costEvaluator;
        }

        public CustomPath(PathImpl.Builder builder, double weightCost, CostEvaluator<Double> costEvaluator,
                Node startNode, Node endNode, HashSet<String> blockedBetweenNode, int size) {
            this.builder = builder;
            this.weightCost = weightCost;
            this.costEvaluator = costEvaluator;
            this.startNode = startNode;
            this.endNode = endNode;
            this.blockedBetweenNode = blockedBetweenNode;
            this.size = size;
        }

        public double weight() {
            return this.weightCost;
        }

        public Node endNode() {
            return this.endNode;
        }

        public CustomPath addRelationship(Relationship rel, Direction direction) {
            HashSet<String> newBlockedBetweenNode = new HashSet<>(this.blockedBetweenNode);

            Node relStartNode = rel.getStartNode();
            Node relEndNode = rel.getEndNode();
            newBlockedBetweenNode.add(this.getFromToNodeId(relStartNode, relEndNode));
            newBlockedBetweenNode.add(this.getFromToNodeId(relEndNode, relStartNode));

            // this.size++;
            return new CustomPath(
                    builder.push(rel),
                    this.weightCost + costEvaluator.getCost(rel, direction),
                    this.costEvaluator,
                    this.startNode,
                    relEndNode,
                    newBlockedBetweenNode,
                    this.size + 1);
        }

        public String getFromToNodeId(Node from, Node to) {
            return from.getElementId() + to.getElementId();

        }

        public boolean isBlocked(Relationship rel) {
            return this.blockedBetweenNode.contains(this.getFromToNodeId(rel.getStartNode(), rel.getEndNode()));
        }

        public WeightedPath toWeightedPath() {
            return new WeightedPathImpl(costEvaluator, this.getPath());
        }

        public Path getPath() {
            return this.builder.build();
        }

    }

    @Procedure(name = "com.eppstein.shortestPaths", mode = Mode.READ)
    public Stream<ResponsePath> eppsteinShortestPaths(
            @Name("storageKey") String storageKey,
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k) {
        log.info("STARTED QUERY");
        PriorityQueue<CustomPath> kShortestPaths = new PriorityQueue<>(
                Comparator.comparingDouble(CustomPath::weight));
        PriorityQueue<CustomPath> pathsQueue = new PriorityQueue<>(
                Comparator.comparingDouble(CustomPath::weight));

        CostEvaluator<Double> costEvaluator = (relationship, direction) -> 1.0;
        Direction direction = Direction.OUTGOING;

        // path.endNode().getRelationships(Direction.INCOMING)
        pathsQueue.add(new CustomPath(startNode, costEvaluator));
        // int ii = 0;

        while (kShortestPaths.size() < k && pathsQueue.size() != 0) {
            CustomPath curranPathPosition = pathsQueue.peek();

            if (curranPathPosition.size > 30) {
                break;
            }
            for (Relationship relationship : curranPathPosition.endNode().getRelationships(direction)) {

                if (!curranPathPosition.isBlocked(relationship)) {
                    CustomPath newPathPosition = curranPathPosition.addRelationship(relationship, direction);
                    // console.log("🚀 --> @Name --> newPathPosition:", newPathPosition);
                    log.info("EQ: " + (newPathPosition.endNode().equals(endNode)) + ", isBlocked: "
                            + curranPathPosition.isBlocked(relationship) + ", SIZE:"
                            + curranPathPosition.weight()
                            + ", phoneKye: "
                            + relationship.getEndNode().getProperties("phoneKey") + "::::"
                            + endNode.getProperties("phoneKey"));

                    if (newPathPosition.endNode().equals(endNode)) {
                        log.info("SIZE: " + newPathPosition.weight());
                        kShortestPaths.add(newPathPosition);
                    } else {
                        pathsQueue.add(newPathPosition);
                    }
                }

            }
            pathsQueue.poll();

            // return kShortestPaths.stream()
            // // .sorted(Comparator.comparingDouble(WeightedPath::weight))
            // .map(path -> new ResponsePath(ValueUtils.of(path.toWeightedPath())));
            // ii++;
        }

        log.info("ENDED QUERY");
        return kShortestPaths.stream()
                // .sorted(Comparator.comparingDouble(WeightedPath::weight))
                .map(path -> new ResponsePath(ValueUtils.of(path.toWeightedPath())));
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
                        if ((currentKPaths.size() >= k)) {
                            filteredRelationships.add(relationship);
                            // currentKPaths.add(null)
                            // path
                        }
                        // log.info("FIND NODE");
if(){
    
    PathImpl.Builder pathBuilder = new PathImpl.Builder(path.startNode());
    for (Relationship pathRelationship : path.relationships()) {
        pathBuilder = pathBuilder.push(pathRelationship);
    }
    pathBuilder = pathBuilder.push(relationship);

    currentKPaths.add(new WeightedPathImpl(costEvaluator, pathBuilder.build()));
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

    @Procedure(name = "com.yens.shortestPaths", mode = Mode.READ)
    public Stream<ResponsePath> yenKShortestPaths(
            @Name("storageKey") String storageKey,
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k) {
        // try (Transaction transaction = tx) {

        AtomicLong LOG1 = new AtomicLong(0);
        AtomicLong LOG2 = new AtomicLong(0);
        AtomicLong LOG3 = new AtomicLong(0);
        AtomicLong LOG4 = new AtomicLong(0);
        AtomicLong LOG5 = new AtomicLong(0);

        log.info(String.format("\nAAAAAAAAAAAAAAAAAAA"));
        // boolean isCached = storage.get(storageKey) != null;
        YensPathFinderData data = new YensPathFinderData(storageKey, startNode, endNode);
        CacheStorage cachedData = data.cachedData;
        // HashSet<String> ignoreRelationshipsIds =
        // data.cachedData.ignoreRelationshipsIds;
        // getYensStorage(storageKey, startNode, endNode);
        // try (Transaction tx = db.beginTx()) {
        // YensProcessStorage data = new YensProcessStorage(startNode, endNode,
        // (relationship, direction) -> 1.0);

        log.info(String.format("\nCCCCCCCCCCCCCCCCCCCCCCCCCCCC"));
        log.info("\nDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
        if (data.potentialPaths.size() == 0) {
            return Stream.empty();
        }
        // HashSet<Relationship> ignoreRelationships = new HashSet<Relationship>();
        // PriorityQueue<WeightedPath> currentKPaths = new PriorityQueue<>();
        // data.currentKPaths.size();
        // List<WeightedPath> currentKPaths = new ArrayList<>();

        while (data.currentKPaths.size() < k && data.potentialPaths.size() > 0) {

            WeightedPath shortestPath = data.potentialPaths.peek();
            // Direction.INCOMING : Direction.OUTGOING

            // List<Node> shortestPathNodes =
            // StreamSupport.stream(shortestPath.nodes().spliterator(), false)
            // .collect(Collectors.toList());
            // List<Relationship> shortestPathRelationships = StreamSupport
            // .stream(shortestPath.relationships().spliterator(), false)
            // .collect(Collectors.toList());
            for (Relationship ignoreRelationship : shortestPath.relationships()) {

                if (data.currentKPaths.size() == k) {
                    break;
                }
                while (data.currentKPaths.size() != k) {

                    Node spurNode = ignoreRelationship.getStartNode();
                    data.BlockRelationship(ignoreRelationship, cachedData.shortPathIndex);
                    //
                    // Relationship ignoreRelationship =
                    // shortestPathRelationships.get(cachedData.shortPathIndex);
                    // if
                    // (ignoreRelationship.getStartNode().getProperties("phoneKey").equals("+995598362399")
                    // &&
                    // ignoreRelationship.getStartNode().getProperties("phoneKey").equals("+995558660067"))
                    // {

                    // log.info("ignoreRelationship: "
                    // + (cachedData.ignoreRelationshipsIds
                    // .contains("" + ignoreRelationship.getId()
                    // + cachedData.shortPathIndex))
                    // + ":::"
                    // + cachedData.shortPathIndex + ":::"
                    // + ignoreRelationship.getStartNode().getProperties("phoneKey") + ":::"
                    // + ignoreRelationship.getEndNode().getProperties("phoneKey") + ":::"
                    // + ("" + ignoreRelationship.getId()
                    // + cachedData.shortPathIndex)
                    // + "\n");

                    // log.info("INDEX: " + relationship.getElementId() + "-" +
                    // relationship.getElementId()
                    // + "-" + path.length() + "\n");
                    // }
                    // log.info("ignoreRelationship: " + ignoreRelationship.getElementId() + "--" +
                    // cachedData.shortPathIndex
                    // + "\n");

                    // cachedData.ignoreRelationshipsIds.add(ignoreRelationship.getElementId());
                    // ignoreRelationships.add(ignoreRelationship);

                    // log.info(String.format("spurNode.phoneKey: %s",
                    // spurNode.getProperties("phoneKey")));
                    // HashSet<Relationship> ignoreRelationships = this.ignoreRelationships;

                    PathExpander<Double> expander = new PathExpander<Double>() {
                        @Override
                        public ResourceIterable<Relationship> expand(Path path, BranchState<Double> state) {
                            List<Relationship> filteredRelationships = new ArrayList<>();

                            for (Relationship relationship : path.endNode().getRelationships(Direction.OUTGOING)) {

                                if (!data.isBlockedRelationship(relationship,
                                        path.length() + cachedData.shortPathIndex)) {
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

                    // Find the shortest path from spurNode using the modified expander
                    Dijkstra dijkstra = new Dijkstra(expander, data.costEvaluator, data.epsilon, data.interest);
                    // long startTime5 = System.nanoTime(); // Start time measurement

                    WeightedPath spurPath = dijkstra.findSinglePath(spurNode, endNode);

                    if (spurPath == null) {
                        // cachedData.shortPathIndex++;
                        break;
                    }

                    WeightedPathImpl concatPath = concatPaths(shortestPath, 0, cachedData.shortPathIndex, spurPath, 0,
                            spurPath.length(),
                            data.costEvaluator);

                    if (concatPath.weight() <= shortestPath.weight()) {
                        data.BlockRelationship(spurPath.relationships().iterator().next(), 0);

                        data.potentialPaths.add(concatPath);
                        data.currentKPaths.add(concatPath);

                        log.info("concatPath.weight(): " + concatPath.weight(),
                                ", shortestPath.weight(): " + shortestPath.weight());

                        break;
                    }
                }
                // currentKPaths.add(concatPath);
                // this.KPaths.add(concatPath);
                cachedData.shortPathIndex++;
            }
            data.potentialPaths.poll();
            cachedData.shortPathIndex = 0;
        }
        // while (data.KPaths.size() < k && data.NextPath() != null)
        // ;

        log.info(String.format("LOG1. Execution timee: %.15f seconds%n", LOG1.get() / 1_000_000_000.0));

        // data.db.beginTx();

        // Re-fetch nodes based on their IDs in the new transaction
        // List<WeightedPath> nodes = data.KPaths.stream()
        // // .sorted(Comparator.comparingDouble(WeightedPath::weight))
        // .map(path -> new ResponsePath(ValueUtils.of(path)))
        // .collect(Collectors.toList());

        // List<WeightedPath> sortedList = new ArrayList<>();

        log.info(String.format("LOG2. Execution timee: %.15f seconds%n", LOG2.get() / 1_000_000_000.0));
        log.info(String.format("LOG3. Execution time: %.15f seconds%n", LOG3.get() / 1_000_000_000.0));
        log.info(String.format("LOG4. Execution time: %.15f seconds%n", LOG4.get() / 1_000_000_000.0));
        log.info(String.format("LOG5. Execution time222: %.15f seconds%n", LOG5.get() / 1_000_000_000.0));

        // Use these nodes to reconstruct paths or perform new operations
        // ...

        // tx.commit();
        // }
        //
        // tx.close();
        // if (isCached) {
        // data.tx.rollback();
        // // try (Transaction transaction = data.db.beginTx()) {

        // for (WeightedPath path : currentKPaths) {
        // // path.getGraphDatabase()
        // // WeightedPath path2 = clonePathUsingNodes(path);
        // sortedList.add(path);
        // // path.
        // // path.
        // // for (Node node : path.nodes()) {
        // // log.info(String.format("sortedList.add(path): %s",
        // // node.getProperties("phoneKey")));

        // // }
        // }
        // transaction.commit(); // Commit the transaction
        return data.currentKPaths.stream()
                // .sorted(Comparator.comparingDouble(WeightedPath::weight))
                .map(path -> new ResponsePath(ValueUtils.of(path)));
        // }

        // }

        // for(

        // WeightedPath path:data.KPaths)
        // {
        // // WeightedPath path2 = clonePathUsingNodes(path);
        // sortedList.add(path);
        // // path.
        // // path.
        // // for (Node node : path.nodes()) {
        // // log.info(String.format("sortedList.add(path): %s",
        // // node.getProperties("phoneKey")));

        // // }
        // }return
        // sortedList.stream().sorted(Comparator.comparingDouble(WeightedPath::weight)).map(path->new
        // ResponsePath(ValueUtils.of(path)));
        // }

    }

    // public YensPathFinderData getYensStorage(String key, Node startNode, Node
    // endNode) {
    // log.info(String.format("BBBBBBBBBBBBBBBBBBBB"));
    // // synchronized (storage) { // Synchronize access to the map
    // // ... (rest of the logic)

    // YensPathFinderData cacheData = storage.get(key);

    // // synchronized (storage) {
    // // YensProcessStorage data1 = new YensProcessStorage(startNode, endNode,
    // // (relationship, direction) -> 1.0);
    // // log.info(String.format("AAA. Execution time: %d %n",
    // data1.KPaths.size()));
    // // storage.put("111", data1, 2000 * 100);
    // // log.info(String.format("AAA. Execution time: %d ",
    // // storage.get(key).KPaths.size()));
    // if (cacheData != null) {
    // // log.info(String.format("AAA. KPaths size: %d",
    // // storage.get(key).KPaths.size()));
    // }

    // return
    // }

    public static class ResponsePath {
        public AnyValue paths;

        public ResponsePath(AnyValue paths) {
            this.paths = paths;
        }
    }

    // public class CustomPath {
    // public class CustomRelationShips {
    // }

    // }

    public static final ExpiringMap<String, CacheStorage> storage = new ExpiringMap<>(100, TimeUnit.SECONDS);

    private class CacheStorage {
        int shortPathIndex = 0;
        HashSet<String> ignoreRelationshipsIds = new HashSet<>();
        List<List<String>> potentialPathsRelationships = new ArrayList<List<String>>();
    }

    private class YensPathFinderData {
        Node startNode;
        Node endNode;
        String keyName;
        CostEvaluator<Double> costEvaluator = (relationship, direction) -> 1.0;
        PathInterest<Double> interest = PathInterestFactory.single(0.0);
        double epsilon = 0.0;
        CacheStorage cachedData;
        PriorityQueue<WeightedPath> potentialPaths = new PriorityQueue<>(
                Comparator.comparingDouble(WeightedPath::weight));
        PriorityQueue<WeightedPath> currentKPaths = new PriorityQueue<>(
                Comparator.comparingDouble(WeightedPath::weight));

        public YensPathFinderData(String keyName, Node startNode, Node endNode) {
            this.startNode = startNode;
            this.endNode = endNode;
            this.keyName = keyName;
            CacheStorage cachedData = storage.get(keyName);
            if (cachedData == null) {
                this.cachedData = new CacheStorage();
                this.initDate();
            } else {
                this.cachedData = cachedData;
                this.initCachedDate();
            }
        }

        public void initDate() {
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
            PathFinder<WeightedPath> dijkstra = new Dijkstra(baseExpander, this.costEvaluator, this.epsilon,
                    this.interest);
            WeightedPath shortestPath = dijkstra.findSinglePath(startNode, endNode);

            Iterable<WeightedPath> shortestPathh = dijkstra.findAllPaths(startNode, endNode);
            for (WeightedPath pp : shortestPathh) {
                log.info(" pp.length(): " + pp.length());
            }
            // console.log("🚀 --> YensPathFinderData --> voidinitDate --> shortestPath:",
            // shortestPath);

            this.currentKPaths.add(shortestPath);
            this.potentialPaths.add(shortestPath);
        }

        public void initCachedDate() {
            for (List<String> potentialPathsRelationship : this.cachedData.potentialPathsRelationships) {
                PathImpl.Builder builder = new PathImpl.Builder(this.startNode);
                for (String potentialPathsRelationshipIds : potentialPathsRelationship) {

                    builder = builder.push(tx.getRelationshipByElementId(potentialPathsRelationshipIds));

                    this.potentialPaths.add(new WeightedPathImpl(this.costEvaluator, builder.build()));
                }

            }
        }

        public void saveCahe() {
            List<List<String>> potentialPathsRelationships = new ArrayList<>();
            for (WeightedPath potentialPath : this.potentialPaths) {
                List<String> pathRelationshipsIds = new ArrayList<>();
                for (Relationship relationship : potentialPath.relationships()) {
                    pathRelationshipsIds.add(relationship.getElementId());

                }
                potentialPathsRelationships.add(pathRelationshipsIds);
            }
            this.cachedData.potentialPathsRelationships = potentialPathsRelationships;
            storage.put(this.keyName, this.cachedData);
        }

        public void BlockRelationship(Relationship rel, int index) {
            this.cachedData.ignoreRelationshipsIds.add(rel.getElementId() + index);
        }

        public boolean isBlockedRelationship(Relationship rel, int index) {
            return this.cachedData.ignoreRelationshipsIds.contains(rel.getElementId() + index);
        }
    }

    // private class YensProcessStorage {
    // // GraphDatabaseService db;

    // Node startNode;
    // Node endNode;
    // // CostEvaluator<Double> costEvaluator;
    // PriorityQueue<WeightedPath> potentialPaths;

    // PriorityQueue<WeightedPath> KPaths;
    // int shortPathIndex = 0;

    // double epsilon = 0.0;
    // PathInterest<Double> interest = PathInterestFactory.single(0.0);
    // HashSet<Relationship> ignoreRelationships = new HashSet<>();

    // WeightedPath shortestPath;
    // List<Node> shortestPathNodes;
    // List<Relationship> shortestPathRelationships;
    // Transaction tx;

    // private YensProcessStorage(Node startNode, Node endNode,
    // CostEvaluator<Double> costEvaluator,
    // Transaction tx) {
    // this.startNode = startNode;
    // this.endNode = endNode;
    // this.costEvaluator = costEvaluator;
    // this.tx = tx;

    // // shortestPath.t
    // // this.db = db;

    // PathExpander<Double> baseExpander = new PathExpander<Double>() {
    // @Override
    // public ResourceIterable<Relationship> expand(Path path, BranchState<Double>
    // state) {
    // return
    // Iterables.asResourceIterable(path.endNode().getRelationships(Direction.OUTGOING));
    // }

    // @Override
    // public PathExpander<Double> reverse() {
    // return new PathExpander<Double>() {
    // @Override
    // public ResourceIterable<Relationship> expand(Path path, BranchState<Double>
    // state) {
    // return Iterables
    // .asResourceIterable(path.endNode().getRelationships(Direction.INCOMING));
    // }

    // @Override
    // public PathExpander<Double> reverse() {
    // return this; // Returning the current expander as reverse
    // }
    // };
    // }
    // };

    // PathFinder<WeightedPath> dijkstra = new Dijkstra(baseExpander, costEvaluator,
    // epsilon, interest);
    // // dijkstra.
    // // dijkstra.
    // WeightedPath shortestPath = dijkstra.findSinglePath(this.startNode, endNode);

    // if (shortestPath != null) {
    // this.potentialPaths = new
    // PriorityQueue<>(Comparator.comparingDouble(WeightedPath::weight));

    // this.KPaths = new
    // PriorityQueue<>(Comparator.comparingDouble(WeightedPath::weight));

    // this.potentialPaths.add(shortestPath);
    // this.KPaths.add(shortestPath);

    // this.shortestPath = this.potentialPaths.poll();
    // this.shortestPathNodes =
    // StreamSupport.stream(shortestPath.nodes().spliterator(), false)
    // .collect(Collectors.toList());
    // this.shortestPathRelationships = StreamSupport
    // .stream(shortestPath.relationships().spliterator(), false)
    // .collect(Collectors.toList());

    // }

    // }

    // // public void NextPath() {
    // // boolean shortestPathIndexEnded = this.shortPathIndex >=
    // // (this.shortestPathNodes.size() - 1);
    // // if (this.potentialPaths.isEmpty() && shortestPathIndexEnded) {
    // // // return null;
    // // }
    // // if (shortestPathIndexEnded) {
    // // this.shortPathIndex = 0;

    // // this.shortestPath = this.potentialPaths.poll();
    // // this.shortestPathNodes =
    // // StreamSupport.stream(this.shortestPath.nodes().spliterator(), false)
    // // .collect(Collectors.toList());
    // // this.shortestPathRelationships = StreamSupport
    // // .stream(shortestPath.relationships().spliterator(), false)
    // // .collect(Collectors.toList());
    // // }
    // // // int shortPathIndex = this.shortPathIndex;
    // // this.shortPathIndex++;

    // // // return concatPath;
    // // }

    // }

    private WeightedPathImpl concatPaths(WeightedPath path1, int path1from, int path1to, WeightedPath path2,
            int path2from, int path2to, CostEvaluator<Double> costEvaluator) {
        PathImpl.Builder builder = new PathImpl.Builder(path1.startNode());

        int index1 = 0;
        for (Relationship relationship : path1.relationships()) {
            if (index1 >= path1from && index1 < path1to) {
                builder = builder.push(relationship);
            }
            index1++;
        }

        int index2 = 0;
        for (Relationship relationship : path2.relationships()) {
            if (index2 >= path2from && index2 < path2to) {
                builder = builder.push(relationship);
            }
            index2++;
        }

        return new WeightedPathImpl(costEvaluator, builder.build());
    }
}
