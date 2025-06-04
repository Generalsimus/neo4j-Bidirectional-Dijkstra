package com.bdpf.dijkstra;

import org.neo4j.graphdb.Relationship;
import org.neo4j.values.AnyValue;

public interface RelationshipFilter {

    Iterable<Relationship> getRelationships(PathFinder path);

    public AnyValue toValue(PathFinder path1, PathFinder path2);

}