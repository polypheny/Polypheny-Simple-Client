package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import java.util.Random;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.graph.GraphBenchConfig;
import org.polypheny.simpleclient.scenario.graph.GraphQuery.SimpleGraphQuery;

public class DeleteNodeBuilder extends QueryBuilder {

    private final GraphBenchConfig config;
    private final Random random;


    public DeleteNodeBuilder( GraphBenchConfig config ) {
        this.config = config;
        this.random = new Random( config.seed );
    }


    @Override
    public Query getNewQuery() {
        return new SimpleGraphQuery( String.format( "MATCH (n:Label%s) DELETE n", random.nextInt( config.highestLabel ) ) );
    }

}
