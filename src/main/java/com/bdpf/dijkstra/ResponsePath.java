package com.bdpf.dijkstra;

import org.neo4j.values.AnyValue;

public class ResponsePath {
    public AnyValue paths;

    public ResponsePath(AnyValue paths) {
        this.paths = paths;
    }
}