package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import java.util.Random;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.graph.GraphBenchConfig;
import org.polypheny.simpleclient.scenario.graph.GraphQuery.SimpleGraphQuery;

public class FindNeighborsBuilder extends QueryBuilder {

    private final Random random;
    private final int highestLabel;
    private final int highestProperty;


    public FindNeighborsBuilder( GraphBenchConfig config ) {
        this.random = new Random( config.seed );

        this.highestLabel = config.maxClusterSize;
        this.highestProperty = config.properties;

    }


    @Override
    public Query getNewQuery() {
        int labelId = random.nextInt( highestLabel );
        int propertyId = random.nextInt( highestProperty );
        return new SimpleGraphQuery( String.format( "MATCH (n:Label%s { key_%s_%s: 'value_%s_%s'})-[]-(t)\nRETURN t", labelId*2, propertyId, propertyId, propertyId, propertyId ) );
    }


}
