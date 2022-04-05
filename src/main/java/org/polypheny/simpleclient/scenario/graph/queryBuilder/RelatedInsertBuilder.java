package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import java.util.Random;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.graph.GraphBenchConfig;
import org.polypheny.simpleclient.scenario.graph.GraphQuery.SimpleGraphQuery;

public class RelatedInsertBuilder extends QueryBuilder {

    private final Random random;
    private final GraphBenchConfig config;


    public RelatedInsertBuilder( GraphBenchConfig config ) {
        this.random = new Random( config.seed );
        this.config = config;
    }


    @Override
    public Query getNewQuery() {
        return new SimpleGraphQuery( String.format(
                "MATCH (n:Label%s), (m:Label%s) CREATE (n)-[:Label%s {list:['el','el','el']}]-(m)",
                random.nextInt( config.highestLabel ),
                random.nextInt( config.highestLabel ),
                random.nextInt( config.highestLabel ) ) );
    }

}
