package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import java.util.Random;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.graph.GraphBenchConfig;
import org.polypheny.simpleclient.scenario.graph.GraphQuery.SimpleGraphQuery;

public class SetPropertyBuilder extends QueryBuilder {

    private final Random random;
    private final GraphBenchConfig config;


    public SetPropertyBuilder( GraphBenchConfig config ) {
        this.random = new Random( config.seed );
        this.config = config;
    }


    @Override
    public Query getNewQuery() {
        return new SimpleGraphQuery( String.format( "MATCH (n:Label%s) SET n.newProp = 'value'", random.nextInt( config.highestLabel ) ) );
    }

}
