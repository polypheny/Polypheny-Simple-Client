package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.graph.GraphBench;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.GraphInsert.SimpleGraphInsert;

public class CreateGraphDatabase extends QueryBuilder {


    @Override
    public Query getNewQuery() {
        return new SimpleGraphInsert( String.format( "CREATE DATABASE %s IF NOT EXISTS", GraphBench.GRAPH_DATABASE ) );
    }

}
