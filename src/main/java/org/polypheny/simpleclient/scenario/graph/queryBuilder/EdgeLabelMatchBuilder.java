package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import java.util.Random;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.graph.GraphBenchConfig;
import org.polypheny.simpleclient.scenario.graph.GraphQuery;
import org.polypheny.simpleclient.scenario.graph.GraphQuery.SimpleGraphQuery;
import org.polypheny.simpleclient.scenario.graph.queryBuilder.GraphInsert.SimpleGraphInsert;

public class EdgeLabelMatchBuilder extends QueryBuilder {


    private final String nodeLeftLabel;
    private final String properties;
    private final String nodeRightLabel;
    private final Random random;
    private final int highestLabel;


    public EdgeLabelMatchBuilder( GraphBenchConfig config ) {
        int randomSeed = config.seed;
        this.random = new Random( randomSeed );

        this.highestLabel = config.maxClusterSize - 1;
        this.nodeLeftLabel = null;
        this.properties = null;
        this.nodeRightLabel = null;
    }


    @Override
    public Query getNewQuery() {
        return new SimpleGraphQuery(
                String.format(
                        "MATCH (n%s)-[rel%s %s]-(m%s) RETURN rel",
                        nodeLeftLabel == null ? "" : ":" + nodeLeftLabel,
                        ":Label" + random.nextInt( this.highestLabel ),
                        properties == null ? "" : properties,
                        nodeRightLabel == null ? "" : ":" + nodeRightLabel ) );
    }

}
