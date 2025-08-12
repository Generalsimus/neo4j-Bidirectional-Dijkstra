
package com.bdpf.dijkstra;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.kernel.impl.util.ValueUtils;
import java.util.*;

public class PathFinder {
    Map<Long, PathFinder> map;
    Map<Long, PathFinder> reverseMap;
    PathFinder endPath;

    // long id = 0;
    // long reverseId = 0;
    //
    Node endNode;

    public Linker<Connection> chain;

    Double weight = 0.0;
    CostEvaluator<Double> costEvaluator;
    RelationshipFilter relationshipFilter;

    PathFinder(
            Map<Long, PathFinder> map,
            Map<Long, PathFinder> reverseMap,
            Node endNode,
            CostEvaluator<Double> costEvaluator,
            RelationshipFilter relationshipFilter) {
        this.map = map;
        this.reverseMap = reverseMap;
        // this.id = id;
        // this.reverseMap = reverseMap;
        this.endNode = endNode;
        this.costEvaluator = costEvaluator;
        this.relationshipFilter = relationshipFilter;
        this.chain = new Linker<Connection>();
    }

    PathFinder(Map<Long, PathFinder> map,
            Map<Long, PathFinder> reverseMap, Node endNode, Double weight, Linker<Connection> chain,
            CostEvaluator<Double> costEvaluator,
            RelationshipFilter relationshipFilter) {
        this.map = map;
        this.reverseMap = reverseMap;
        this.endNode = endNode;
        this.weight = weight;
        this.costEvaluator = costEvaluator;
        this.relationshipFilter = relationshipFilter;
        this.chain = chain;
        // this.id = id;
    }

    public Double getWeight() {
        return this.weight;
    }

    public PathFinder getEndEntry() {
        return this.endPath;
    }

    public boolean isBlockNode(Node node) {
        Iterator<Connection> iterator = this.chain.iterator();

        while (iterator.hasNext()) {
            Connection current = iterator.next();
            if (current.start.equals(node)) {
                return true;
            }
        }
        // if (this.chain.getSize() == 0) {
        return this.endNode.equals(node);
        // }
        // return false;
    }

    public boolean isBlockNode2(Node node) {
        Iterator<Connection> iterator = this.chain.iterator();

        while (iterator.hasNext()) {
            Connection current = iterator.next();
            if (current.end.equals(node)) {
                return true;
            }
        }
        // if (this.isBlockNode(current.end)) {
        // return true;
        // }
        // Connection current = iterator.next();
        // if (current.start.equals(node)) {
        // return true;
        // }
        // return false
        // }

        // if (this.chain.getSize() == 0) {
        // return this.endNode.equals(node);
        // }
        // if (this.chain.getSize() == 0) {
        // return this.endNode.equals(node);
        // }
        return false;
        // return this.isBlockNode(point.getEndNode());
    }

    public ListValue contactToValue(PathFinder reverse) {
        Linker<AnyValue> newChain = new Linker<>();
        ListValueBuilder builder = ListValueBuilder.newListBuilder(this.chain.getSize() + reverse.chain.getSize());

        for (Connection chainRel : this.chain) {
            newChain = newChain.push(chainRel.toValue());
        }

        for (AnyValue el : newChain) {
            builder.add(el);
        }

        for (Connection chainRel : reverse.chain) {

            builder.add(chainRel.toReversedValue());
        }

        return builder.build();
    }

    public Node getEndNode() {
        return this.endNode;
    }

    public static long getRelationshipId(long id1, long id2) {
        return (((id1 + id2) * (id1 + id2 + 1)) / 2) + id2;

        // long sum = id1 + id2;
        // long min = id1 < id2 ? id1 : id2;
        // long max = sum - min;
        // return (sum * (sum + 1) / 2) + max;
    }

    public long getId() {
        if (this.chain.getSize() == 0) {
            return -1;
        }
        return this.getRelationshipId(this.chain.element.start.getId(), this.chain.element.end.getId());
    }

    public long getReverseId() {
        if (this.chain.getSize() == 0) {
            return -2;
        }
        return this.getRelationshipId(this.chain.element.end.getId(), this.chain.element.start.getId());
        // return this.id;
    }

    public Node getPreviousNode() {
        if (this.chain.getSize() > 0) {
            return this.chain.element.start;
        }
        return null;
    }

    public PathFinder addRelationship(Relationship rel, Double relCost, Node newEndNode) {
        return new PathFinder(
                this.map,
                this.reverseMap,
                newEndNode,
                this.weight + relCost,
                this.chain.push(new Connection(this.getEndNode(), rel, newEndNode, relCost)),
                this.costEvaluator,
                this.relationshipFilter);
    }

}