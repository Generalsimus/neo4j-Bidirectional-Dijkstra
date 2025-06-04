package com.bdpf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;

public class RegenerationDb extends BulkDataProcessor {
    public RegenerationDb(GraphDatabaseService db) {
        // db.beginTx()
        super(db);
    }

    public void run(
            String jdbcUrl,
            String username,
            String password,
            String runQuery, Log log) {
        // int count = 0;
        log.info("START @");
        // AtomicInteger count = new AtomicInteger(0);
        // AtomicReference<ResourceIterator<Node>> nodesIterator = new AtomicInteger(0);
        // ResourceIterator<Node> nodesIterator = null;
        // db.runQuery("");
        db.executeTransactionally("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 1000 ROWS;");
        log.info("START @ii");

        this.runTransaction((tx, commit, close) -> {
            try {
                commit.call().execute("DROP INDEX weight_index IF EXISTS");
            } catch (Exception e) {
                log.info("DROP INDEX error:", e);
            }
            try {
                commit.call().execute("DROP CONSTRAINT node_phoneKey_index IF EXISTS");
            } catch (Exception e) {
                log.info("DROP CONSTRAINT error:", e);
            }
            close.run();
        });

        // Second transaction: create
        this.runTransaction((tx, commit, close) -> {
            commit.call()
                    .execute("CREATE CONSTRAINT node_phoneKey_index FOR (n:AllyNode) REQUIRE n.phoneKey IS UNIQUE");
            commit.call().execute("CREATE INDEX weight_index FOR ()-[r:CONTACT]->() ON (r.weight)");
            close.run();
        });
        // this.runTransaction((tx, commit, close) -> {
        // try {
        // commit.call().execute("DROP INDEX weight_index");
        // } catch (Exception e) {
        // log.info("ERROR:", e);
        // }
        // try {
        // commit.call().execute("DROP CONSTRAINT node_phoneKey_index");
        // } catch (Exception e) {
        // log.info("ERROR:", e);
        // }
        // commit.call().execute(
        // "CREATE CONSTRAINT node_phoneKey_index FOR (n:AllyNode) REQUIRE n.phoneKey IS
        // UNIQUE");
        // commit.call().execute("CREATE INDEX weight_index FOR ()-[r:CONTACT]->() ON
        // (r.weight);");

        // close.run();
        // });

        log.info("START @ccccii");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(runQuery)) {
            RelationshipType REL_TYPE = RelationshipType.withName("CONTACT");
            Label Label_TYPE = Label.label("AllyNode");

            AtomicInteger count = new AtomicInteger(0);
            this.runTransaction((tx, commit, close) -> {
                try {
                    if (!resultSet.next()) {
                        close.run();
                        log.info("process end.");
                        return;
                    }
                    String fromColumn = resultSet.getString("fromPhoneKey");
                    String toColumn = resultSet.getString("toPhoneKey");
                    double weightColumn = resultSet.getDouble("weight");
                    String relationshipStatus = resultSet.getString("relationshipStatus");

                    Node fromNode = tx.findNode(Label_TYPE, "phoneKey", fromColumn);
                    if (fromNode == null) {
                        fromNode = tx.createNode(Label_TYPE);
                        fromNode.setProperty("phoneKey", fromColumn);
                    }
                    Node toNode = tx.findNode(Label_TYPE, "phoneKey", toColumn);
                    if (toNode == null) {
                        toNode = tx.createNode(Label_TYPE);
                        toNode.setProperty("phoneKey", toColumn);
                    }
                    // fromNode.getSingleRelationship(REL_TYPE, null)

                    // fromNode.getRelationships(Direction.OUTGOING).stream()
                    // .filter(rel -> rel.getOtherNode(fromNode).equals(toNode))
                    // .findFirst() // Get only one relationship
                    // .map(Stream::of)
                    // .orElse(Stream.empty());

                    Relationship currRelationship = null;
                    for (Relationship relationship : fromNode.getRelationships(Direction.OUTGOING, REL_TYPE)) {
                        if (relationship.getOtherNode(fromNode).equals(toNode)) {
                            currRelationship = relationship;
                            break;
                        }
                    }
                    // for (Relationship relationship:fromNode.getRelationships(Direction.OUTGOING,
                    // REL_TYPE)) {
                    // if (relationship.getOtherNode(fromNode).equals(toNode)) {
                    // currRelationship = relationship;
                    // break;
                    // }
                    // };
                    if (currRelationship == null) {
                        currRelationship = fromNode.createRelationshipTo(toNode, REL_TYPE);
                    }
                    // Relationship relationship = fromNode.createRelationshipTo(toNode, REL_TYPE);
                    currRelationship.setProperty("weight", weightColumn);
                    currRelationship.setProperty("status", relationshipStatus);

                } catch (SQLException e) {
                    log.info("atch (SQLException e)", e);

                    close.run();

                }

                if (count.incrementAndGet() % 1000 == 0) {
                    commit.call();
                }
            });

        } catch (Exception e) {
            log.info("ERRtion e)", e);
        }
    }
}
