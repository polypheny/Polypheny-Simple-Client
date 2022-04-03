package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import java.util.Random;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.graph.GraphBenchConfig;
import org.polypheny.simpleclient.scenario.graph.GraphQuery.SimpleGraphQuery;

public class UnwindBuilder extends QueryBuilder {

    private final Random random;
    private final int highestLabel;
    private final int highestProperty;


    public UnwindBuilder( GraphBenchConfig config ) {
        this.random = new Random( config.seed );

        this.highestLabel = config.maxPathLength - 1;
        this.highestProperty = config.properties - 1;
    }


    @Override
    public Query getNewQuery() {
        int labelId = random.nextInt( highestLabel );
        int propertyId = random.nextInt( highestProperty );
        return new SimpleGraphQuery( String.format( "MATCH (n:Label%s {key_0_%s: 'value_0_%s'}) UNWIND n.list AS x RETURN x", labelId*2, propertyId, propertyId ) );
    }

}
