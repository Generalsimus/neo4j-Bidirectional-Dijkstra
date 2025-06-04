package com.bdpf.dijkstra;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.VirtualValues;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;

public class Connection {
    public final Node start;
    public final Relationship relationship;
    public final Node end;
    public final double weight;

    public Connection(Node start, Relationship relationship, Node end, double weight) {
        this.start = start;
        this.relationship = relationship;
        this.end = end;
        this.weight = weight;
    }

    public AnyValue toValue() {
        return VirtualValues.map(
                new String[] { "start", "relationship", "end", "weight" },
                new org.neo4j.values.AnyValue[] {
                        ValueUtils.asNodeValue(start),
                        ValueUtils.asRelationshipValue(relationship),
                        ValueUtils.asNodeValue(end),
                        ValueUtils.asDoubleValue(weight)
                });
    }
    public AnyValue toReversedValue() {
        return VirtualValues.map(
                new String[] { "end", "relationship", "start", "weight" },
                new org.neo4j.values.AnyValue[] {
                        ValueUtils.asNodeValue(start),
                        ValueUtils.asRelationshipValue(relationship),
                        ValueUtils.asNodeValue(end),
                        ValueUtils.asDoubleValue(weight)
                });
    }

}
// start: Neo4jConnectionPathNode;
//   relationship: {
//     properties: {
//       weight: ContactWeightValue;
//       status: RelationshipStatus;
//       weightCoeff: ContactWeightValue;
//     };
//     startNodeElementId: string;
//     endNodeElementId: string;
//   };
//   end: Neo4jConnectionPathNode;