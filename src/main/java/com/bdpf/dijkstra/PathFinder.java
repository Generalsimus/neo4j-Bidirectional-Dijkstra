
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
    Set<Long> visitedNodes;
    Set<Long> reverseVisitedNodes;
    PathFinder endPath;

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
            RelationshipFilter relationshipFilter,
            Set<Long> visitedNodes,
            Set<Long> reverseVisitedNodes) {
        this.map = map;
        this.reverseMap = reverseMap;
        // this.id = id;
        // this.reverseMap = reverseMap;
        this.endNode = endNode;
        this.costEvaluator = costEvaluator;
        this.relationshipFilter = relationshipFilter;
        this.visitedNodes = visitedNodes;
        this.reverseVisitedNodes = reverseVisitedNodes;
        this.chain = new Linker<Connection>();

        // this.visitedNodes.add(endNode.getId());
    }

    PathFinder(Map<Long, PathFinder> map,
            Map<Long, PathFinder> reverseMap, Node endNode, Double weight, Linker<Connection> chain,
            CostEvaluator<Double> costEvaluator,
            RelationshipFilter relationshipFilter,
            Set<Long> visitedNodes,
            Set<Long> reverseVisitedNodes) {
        this.map = map;
        this.reverseMap = reverseMap;
        this.endNode = endNode;
        this.weight = weight;
        this.costEvaluator = costEvaluator;
        this.relationshipFilter = relationshipFilter;
        this.visitedNodes = visitedNodes;
        this.reverseVisitedNodes = reverseVisitedNodes;
        this.chain = chain;
        // this.visitedNodes.add(endNode.getId());
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

    public boolean isBlockNode2(PathFinder path) {
        Iterator<Connection> iterator = path.chain.iterator();

        while (iterator.hasNext()) {
            Connection current = iterator.next();
            if (this.isBlockNode(current.start)) {
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
        int chainFullSize = this.chain.getSize() + reverse.chain.getSize();
        boolean neeRemoveConnectionElement = this.chain.getSize() != 0 && reverse.chain.getSize() != 0;
        if (neeRemoveConnectionElement) {
            chainFullSize = chainFullSize - 1;
        }
        ListValueBuilder builder = ListValueBuilder.newListBuilder(chainFullSize);

        for (Connection chainRel : this.chain) {
            if (!neeRemoveConnectionElement) {
                newChain = newChain.push(chainRel.toValue());
            }
            neeRemoveConnectionElement = false;
        }
        // if (neeRemoveConnectionElement) {
        // newChain = newChain.before;
        // }

        for (AnyValue el : newChain) {
            builder.add(el);
        }
        // boolean ignoreConnectionLink = this.chain.getSize() != 0 &&
        // reverse.chain.getSize() != 0;

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
                this.relationshipFilter,
                this.visitedNodes,
                this.reverseVisitedNodes);
    }

}