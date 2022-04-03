package org.polypheny.simpleclient.scenario.graph;

import java.util.Map;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;

public abstract class GraphQuery extends Query {


    public GraphQuery() {
        super( GraphBench.EXPECTED_RESULT );
    }


    @Override
    public String getSql() {
        return null;
    }


    @Override
    public String getParameterizedSqlQuery() {
        return null;
    }


    @Override
    public HttpRequest<?> getRest() {
        return null;
    }


    @Override
    public String getMongoQl() {
        return null;
    }

    @Override
    public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
        return null;
    }

    public static class SimpleGraphQuery extends GraphQuery{


        private final String cypher;


        public SimpleGraphQuery( String cypher ) {
            this.cypher = cypher;
        }


        @Override
        public String getCypher() {
            return this.cypher;
        }

    }

}
