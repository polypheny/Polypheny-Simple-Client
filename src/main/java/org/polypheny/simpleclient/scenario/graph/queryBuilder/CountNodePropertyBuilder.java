package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import java.util.Random;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.graph.GraphBenchConfig;
import org.polypheny.simpleclient.scenario.graph.GraphQuery.SimpleGraphQuery;

public class CountNodePropertyBuilder extends QueryBuilder {


    private final Random random;
    private final int highestLabel;
    private final int highestProperty;


    public CountNodePropertyBuilder( GraphBenchConfig config ) {
        int randomSeed = config.seed;
        this.random = new Random( randomSeed );

        this.highestLabel = config.maxClusterSize - 1;
        this.highestProperty = config.properties;
    }


    @Override
    public Query getNewQuery() {
        return new SimpleGraphQuery( String.format(
                "MATCH (n%s) RETURN count(n.value_%s_1)",
                ":Label" + (random.nextInt( highestLabel ) * 2),
                random.nextInt( highestProperty ) ) );
    }

}
