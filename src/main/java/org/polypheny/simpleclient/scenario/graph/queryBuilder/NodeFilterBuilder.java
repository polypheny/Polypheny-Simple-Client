package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import java.util.Random;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.graph.GraphBenchConfig;
import org.polypheny.simpleclient.scenario.graph.GraphQuery.SimpleGraphQuery;

public class NodeFilterBuilder extends QueryBuilder {

    private final Random random;
    private final GraphBenchConfig config;


    public NodeFilterBuilder( GraphBenchConfig config ) {
        this.random = new Random( config.seed );
        this.config = config;
    }


    @Override
    public Query getNewQuery() {
        int label = random.nextInt( config.highestLabel );
        int key1 = random.nextInt( config.highestProperty );
        int key3 = random.nextInt( config.highestProperty );
        return new SimpleGraphQuery( String.format(
                "MATCH (n:Label%s { key_%s_%s: 'value_%s_%s', key_%s_0: 'value_%s_0' }) WHERE n.key_1_%s <> 'value%s' RETURN n", label*2, key1, key1, key1, key1, key1 + 1, key1 + 1, key3, 1 ) );
    }

}
