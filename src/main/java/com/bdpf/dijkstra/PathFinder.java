
package com.bdpf.dijkstra;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import java.util.*;

public class PathFinder {
    Map<Long, PathFinder> map;
    Map<Long, PathFinder> reverseMap;
    PathFinder endPath;

    long id = 0;
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
            long id) {
        this.map = map;
        this.reverseMap = reverseMap;
        this.id = id;
        // this.reverseMap = reverseMap;
        this.endNode = endNode;
        this.costEvaluator = costEvaluator;
        this.relationshipFilter = relationshipFilter;
        this.chain = new Linker<Connection>();
    }

    PathFinder(Map<Long, PathFinder> map,
            Map<Long, PathFinder> reverseMap, Node endNode, Double weight, Linker<Connection> chain,
            CostEvaluator<Double> costEvaluator,
            RelationshipFilter relationshipFilter, long id) {
        this.map = map;
        this.reverseMap = reverseMap;
        this.endNode = endNode;
        this.weight = weight;
        this.costEvaluator = costEvaluator;
        this.relationshipFilter = relationshipFilter;
        this.chain = chain;
        this.id = id;
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
        return this.endNode.equals(node);
    }

    public ListValue contactToValue(PathFinder reverse) {
        Linker<AnyValue> newChain = new Linker<>();
        ListValueBuilder builder = ListValueBuilder.newListBuilder(this.chain.getSize() +
                reverse.chain.getSize());

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

    public Long getId() {
        return this.id;
    }

    public Node getPreviousNode() {
        if (this.chain.getSize() > 0) {
            return this.chain.element.start;
        }
        return null;
    }

    public PathFinder addRelationship(Relationship rel, Double relCost, Node newEndNode, long newId) {
        return new PathFinder(
                this.map,
                this.reverseMap,
                newEndNode,
                this.weight + relCost,
                this.chain.push(new Connection(this.getEndNode(), rel, newEndNode, relCost)),
                this.costEvaluator,
                this.relationshipFilter,
                newId);
    }

}