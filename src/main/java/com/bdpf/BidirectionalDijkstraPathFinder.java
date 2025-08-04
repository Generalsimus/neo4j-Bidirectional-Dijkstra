
package com.bdpf;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.AnyValue;

import com.bdpf.dijkstra.CostEvaluator;
import com.bdpf.dijkstra.Dijkstra;
import com.bdpf.dijkstra.PathFinder;
import com.bdpf.dijkstra.RelationshipFilter;
import com.bdpf.dijkstra.ResponsePath;

// CALL com.bdpf.shortestPath("key", 2, startNode, endNode, 1, "REGISTERED", 10) YIELD paths
// RETURN [path IN paths | path.start.phoneKey + "_" + path.end.phoneKey] AS phoneChain;
// ""
// CALL com.bdpf.shortestPath("key", 2, startNode, endNode, 1, "REGISTERED", 10) YIELD paths
// RETURN paths;

// mvn clean package
public class BidirectionalDijkstraPathFinder {
    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tr;

    @Context
    public Log log;

    public static RelationshipFilter getRegisteredRelationships = new RelationshipFilter() {
        @Override
        public Iterable<Relationship> getRelationships(PathFinder path) {
            // Logger log = Logger.getLogger(MyClass.class.getName());
            Set<Node> uniqueNodes = new HashSet<>();
            if (path.chain.getSize() == 0) {
                return () -> path.getEndNode().getRelationships(Direction.BOTH)
                        .stream()
                        .filter(rel -> uniqueNodes.add(rel.getOtherNode(path.getEndNode())))
                        .iterator();
            }
            return () -> path.getEndNode().getRelationships(Direction.BOTH)
                    .stream()
                    .filter(rel -> !uniqueNodes.add(rel.getOtherNode(path.getEndNode())))
                    .iterator();
        };

        @Override
        public AnyValue toValue(PathFinder path1, PathFinder path2) {
            // return path1.concatPathsFinder(path2);
            return path1.contactToValue(path2);
        };

    };
    // REGISTERED REVERSE
    public static RelationshipFilter getRegisteredReverseRelationships = new RelationshipFilter() {
        @Override
        public Iterable<Relationship> getRelationships(PathFinder path) {
            // System.out.println("DIRECTION REVERSE INCOMING");
            return path.getEndNode().getRelationships(Direction.INCOMING);
        };

        @Override
        public AnyValue toValue(PathFinder path1, PathFinder path2) {
            // return path1.concatReversePathsFinder(path2);
            // return path2.contactToValue(path1);
            return path2.contactToValue(path1);
        };
    };
    // UNREGISTERED
    public static RelationshipFilter getUnregisteredRelationships = new RelationshipFilter() {
        @Override
        public Iterable<Relationship> getRelationships(PathFinder path) {
            Set<Node> uniqueNodes = new HashSet<>();
            return () -> path.getEndNode().getRelationships(Direction.BOTH)
                    .stream()
                    .filter(rel -> uniqueNodes.add(rel.getOtherNode(path.getEndNode())))
                    .iterator();
        };

        @Override
        public AnyValue toValue(PathFinder path1, PathFinder path2) {
            return path1.contactToValue(path2);
        };

    };
    // UNREGISTERED REVERSE
    public static RelationshipFilter getUnregisteredReverseRelationships = new RelationshipFilter() {
        @Override
        public Iterable<Relationship> getRelationships(PathFinder path) {
            Set<Node> uniqueNodes = new HashSet<>();
            return () -> path.getEndNode().getRelationships(Direction.BOTH)
                    .stream()
                    .filter(rel -> uniqueNodes.add(rel.getOtherNode(path.getEndNode())))
                    .iterator();
        };

        @Override
        public AnyValue toValue(PathFinder path1, PathFinder path2) {
            return path2.contactToValue(path1);
        };
    };

    @Procedure(name = "com.bdpf.shortestPath", mode = Mode.READ)
    public Stream<ResponsePath> shortestPath(
            @Name("storageKey") String storageKey,
            @Name("storageExpirationSeconds") long storageExpirationSeconds,
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k,
            @Name("chainType") String chainType,
            @Name("timeoutSeconds") long timeoutSeconds) {
        storageExpirationSeconds = Math.max(storageExpirationSeconds, timeoutSeconds);

        Dijkstra dijkstra = new Dijkstra();
        CostEvaluator<Double> costEvaluator = (relationship, path) -> {
            int size = path.chain.getSize() + 1;
            return ((double) size);
            // double optimalWeight = ((Number) relationship.getProperty("weight", 1)).doubleValue();
            // return (size * 100) + optimalWeight;
        };
        if (chainType.equals("REGISTERED")) {
            return dijkstra.find(
                    db,
                    startNode,
                    endNode,
                    k,
                    storageKey,
                    storageExpirationSeconds,
                    timeoutSeconds,
                    costEvaluator,
                    getRegisteredRelationships,
                    getRegisteredReverseRelationships,
                    log);
        }

        return dijkstra.find(
                db,
                startNode,
                endNode,
                k,
                storageKey,
                storageExpirationSeconds,
                timeoutSeconds,
                costEvaluator,
                getUnregisteredRelationships,
                getUnregisteredReverseRelationships,
                log);

    }

    @Procedure(name = "com.bdpf.trustfullyPath", mode = Mode.READ)
    public Stream<ResponsePath> trustfullyPath(
            @Name("storageKey") String storageKey,
            @Name("storageExpirationSeconds") long storageExpirationSeconds,
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k,
            @Name("chainType") String chainType,
            @Name("timeoutSeconds") long timeoutSeconds) {
        storageExpirationSeconds = Math.max(storageExpirationSeconds, timeoutSeconds);

        Dijkstra dijkstra = new Dijkstra();
        CostEvaluator<Double> costEvaluator = (relationship, path) -> {
            int size = path.chain.getSize() + 1;
            double weight = ((Number) relationship.getProperty("weight", 1)).doubleValue();

            // Weight should favor "connections" and "contacts" (lower is better)
            double weightContribution = weight * 0.7; // Reduce impact of weight slightly

            // Penalize long paths logarithmically for better control
            double sizePenalty = Math.log(size + 1) * 0.8;

            return weightContribution + sizePenalty;
        };

        if (chainType.equals("REGISTERED")) {
            return dijkstra.find(
                    db,
                    startNode,
                    endNode,
                    k,
                    storageKey,
                    storageExpirationSeconds,
                    timeoutSeconds,
                    costEvaluator,
                    getRegisteredRelationships,
                    getRegisteredReverseRelationships,
                    log);
        }
        return dijkstra.find(
                db,
                startNode,
                endNode,
                k,
                storageKey,
                storageExpirationSeconds,
                timeoutSeconds,
                costEvaluator,
                getUnregisteredRelationships,
                getUnregisteredReverseRelationships,
                log);
    }

    @Procedure(name = "com.bdpf.optimalPath", mode = Mode.READ)
    public Stream<ResponsePath> optimalPath(
            @Name("storageKey") String storageKey,
            @Name("storageExpirationSeconds") long storageExpirationSeconds,
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("k") long k,
            @Name("chainType") String chainType,
            @Name("timeoutSeconds") long timeoutSeconds) {
        storageExpirationSeconds = Math.max(storageExpirationSeconds, timeoutSeconds);

        Dijkstra dijkstra = new Dijkstra();
        CostEvaluator<Double> costEvaluator = (relationship, path) -> {
            int size = path.chain.getSize() + 1;
            double optimalWeight = ((Number) relationship.getProperty("weight", 1)).doubleValue();

            // Convert optimal weight into trustworthy weight dynamically
            double trustworthyWeight = Math.pow(optimalWeight, 1.5) / 2.0; // Adjust k and scale as needed

            // Add path penalty to discourage long, weak paths
            double trustPenalty = Math.pow(size, 1.2) * 0.3;

            return trustworthyWeight + trustPenalty;
        };

        if (chainType.equals("REGISTERED")) {
            return dijkstra.find(
                    db,
                    startNode,
                    endNode,
                    k,
                    storageKey,
                    storageExpirationSeconds,
                    timeoutSeconds,
                    costEvaluator,
                    getRegisteredRelationships,
                    getRegisteredReverseRelationships,
                    log);

        }
        return dijkstra.find(
                db,
                startNode,
                endNode,
                k,
                storageKey,
                storageExpirationSeconds,
                timeoutSeconds,
                costEvaluator,
                getUnregisteredRelationships,
                getUnregisteredReverseRelationships,
                log);
    }

    @Procedure(name = "com.bdpf.regenerateDb", mode = Mode.SCHEMA)
    public Stream<Result> regenerateDb(
            @Name("jdbcUrl") String jdbcUrl,
            @Name("username") String username,
            @Name("password") String password,
            @Name("runQuery") String runQuery) {

        new RegenerationDb(db).run(jdbcUrl, username, password, runQuery, log);

        return Stream.empty();
    }

    public static class Result {
        public String value;

        public Result(String value) {
            this.value = value;
        }
    }

}
