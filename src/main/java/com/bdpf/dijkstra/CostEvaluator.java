package com.bdpf.dijkstra;

import org.neo4j.graphdb.Relationship;

public interface CostEvaluator<T> {

    T getCost(Relationship relationship, PathFinder path);
}