
package com.bdpf.dijkstra;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import java.util.*;

public class PathFinder {
    Map<Node, PathFinder> map;
    Map<Node, PathFinder> reverseMap;
    PathFinder endPath;

    // Set<Node> blockedNeighbors;

    Node endNode;

    public Linker<Connection> chain;

    Double weight = 0.0;
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
        this.chain = new Linker<Connection>();
        // this.blockedNeighbors = new HashSet<>();
        // this.blockedNeighbors.add(endNode);
    }

    PathFinder(
            Map<Node, PathFinder> map,
            Map<Node, PathFinder> reverseMap,
            // Set<Node> blockedNeighbors,
            Node endNode,
            Double weight,
            CostEvaluator<Double> costEvaluator,
            RelationshipFilter relationshipFilter,
            Linker<Connection> chain) {
        this.map = map;
        this.reverseMap = reverseMap;
        // this.blockedNeighbors = blockedNeighbors;
        this.endNode = endNode;
        this.weight = weight;
        this.costEvaluator = costEvaluator;
        this.relationshipFilter = relationshipFilter;
        this.chain = chain;
    }

    public double concat() {
        return this.weight;
    }

    public Iterable<Relationship> getRelationships() {
        return this.relationshipFilter.getRelationships(this);
    }

    public Double getWeight() {
        return this.weight;
        // * this.chain.getSize();
        // (0.97 * this.chain.getSize());
    }

    public PathFinder getEndEntry() {
        return this.endPath;
    }

    // public void setEndEntry(PathFinder endPath) {
    // this.endPath = endPath;
    // }

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

    // public boolean isEnded() {
    // return this.destinationNode.equals(this.endNode);
    // }

    public PathFinder getFromReverseMap(Node node) {
        return reverseMap.get(node);
    }

    public double getCurrentMinCost(Node node) {
        PathFinder path = this.map.get(node);

        if (path == null) {
            return Double.MAX_VALUE;
        }
        return path.getWeight();
    }

    public PathFinder concatPathsFinder(PathFinder path) {
        Linker<Connection> newChain = path.chain;

        for (Connection chainRel : this.chain) {
            newChain = newChain.push(chainRel);
        }

        return new PathFinder(
                path.map,
                path.reverseMap,
                path.getEndNode(),
                this.weight + path.weight,
                path.costEvaluator,
                path.relationshipFilter,
                newChain);

    }

    public PathFinder concatReversePathsFinder(PathFinder path) {
        Linker<Connection> newChain = this.chain;

        for (Connection chainRel : path.chain) {
            newChain = newChain.push(chainRel);
        }

        return new PathFinder(
                this.map,
                this.reverseMap,
                this.getEndNode(),
                this.weight + path.getWeight(),
                this.costEvaluator,
                this.relationshipFilter,
                newChain);
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
        // Linker<AnyValue> newChain = new Linker<>();
        // double relWeight = 0;
        // for (Connection chainRel : this.chain) {
        // relWeight = relWeight + chainRel.weight;
        // }
        // for (Connection chainRel : reverse.chain) {
        // relWeight = relWeight + chainRel.weight;
        // }
        // ListValueBuilder builder =
        // ListValueBuilder.newListBuilder(this.chain.getSize() +
        // reverse.chain.getSize() + 1);

        // for (Connection chainRel : this.chain) {
        // newChain = newChain.push(
        // ValueUtils.asAnyValue("CC:" + this.getWeight() + ":N:" +
        // chainRel.start.getProperty("phoneKey")));
        // }

        // for (AnyValue el : newChain) {
        // builder.add(el);
        // }
        // builder.add(
        // ValueUtils.asAnyValue("CCM:" + relWeight + ":N:" +
        // this.getEndNode().getProperty("phoneKey")));
        // for (Connection chainRel : reverse.chain) {

        // builder.add(
        // ValueUtils
        // .asAnyValue("CCR:" + reverse.getWeight() + ":N:" +
        // chainRel.start.getProperty("phoneKey")));
        // }

        // return builder.build();
    }

    // public ListValue contactToValueReverse(PathFinder reverse) {
    // return reverse.contactToValue(this);
    // return this.contactToValue(reverse).reverse();
    // ListValueBuilder builder =
    // ListValueBuilder.newListBuilder(reverse.chain.getSize());
    // // Linker<AnyValue> newChain = new Linker<>();
    // double relWeight = 0;
    // for (Connection chainRel : this.chain) {
    // relWeight = relWeight + chainRel.weight;
    // }
    // for (Connection chainRel : reverse.chain) {
    // relWeight = relWeight + chainRel.weight;
    // }
    // Linker<AnyValue> newChain = new Linker<>();
    // for (Connection chainRel : reverse.chain) {
    // newChain = newChain.push(ValueUtils
    // .asAnyValue("RRR:" + reverse.getWeight() + ":N:" +
    // chainRel.from.getProperty("phoneKey")));
    // }
    // for (AnyValue eee : newChain) {
    // builder.add(eee);
    // }
    // // builder.add(
    // // ValueUtils.asAnyValue("RRM:" + relWeight + ":N:" +
    // // reverse.getEndNode().getProperty("phoneKey")));

    // // for (Connection chainRel : this.chain) {
    // // builder.add(ValueUtils.asAnyValue("RR:" + this.getWeight() + ":N:" +
    // // chainRel.to.getProperty("phoneKey")));
    // // }

    // return builder.build();
    // }

    public Node getEndNode() {
        return this.endNode;
    }

    public Node getPreviousNode() {
        if (this.chain.getSize() > 0) {
            return this.chain.element.start;
        }
        return null;
    }

    public PathFinder addRelationship(Relationship rel, Double relCost, Node newEndNode) {
        // this.builder.
        // Set<Node> newBlocNeighbors = new HashSet<>(this.blockedNeighbors);
        // newBlocNeighbors.add(newEndNode);
        return new PathFinder(
                this.map,
                this.reverseMap,
                newEndNode,
                this.weight + relCost,
                this.costEvaluator,
                this.relationshipFilter,
                this.chain.push(new Connection(this.getEndNode(), rel, newEndNode, relCost)));
    }

    public Iterator<Connection> getNextConnections(Direction direction) {
        ResourceIterator<Relationship> relationships = this.getEndNode().getRelationships(direction).iterator();
        PathFinder point = this;
        return new Iterator<Connection>() {
            @Override
            public boolean hasNext() {
                return relationships.hasNext();
            }

            @Override
            public Connection next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Relationship relationship = relationships.next();
                Node otherNode = relationship.getOtherNode(point.getEndNode());

                return new Connection(
                        point.getEndNode(),
                        relationship,
                        otherNode,
                        point.costEvaluator.getCost(relationship, point));
            }
        };
    }

    public Iterator<Connection> getNextUniqueConnections(Direction direction) {
        HashSet<Node> noDuplicates = new HashSet<>();
        ResourceIterator<Relationship> relationships = this.getEndNode().getRelationships(direction).iterator();
        PathFinder point = this;
        return new Iterator<Connection>() {
            private Connection curr = null;

            @Override
            public boolean hasNext() {
                while (relationships.hasNext()) {
                    Relationship relationship = relationships.next();
                    Node otherNode = relationship.getOtherNode(point.getEndNode());

                    if (noDuplicates.add(otherNode)) {

                        curr = new Connection(
                                point.getEndNode(),
                                relationship,
                                otherNode,
                                point.costEvaluator.getCost(relationship, point));
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Connection next() {
                if (curr == null && !hasNext()) {
                    throw new NoSuchElementException();
                }
                Connection currEl = curr;
                curr = null;
                return currEl;
            }
        };

    }
    // public void toWeightedPath() {
    // // return new WeightedPathImpl(this.weight, this.builder.build());
    // }
}