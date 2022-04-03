package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import com.google.gson.JsonObject;
import java.util.Map;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.scenario.graph.GraphBench;

public abstract class GraphInsert extends BatchableInsert {

    public GraphInsert() {
        super( GraphBench.EXPECTED_RESULT );
    }


    @Override
    public String getSqlRowExpression() {
        return null;
    }


    @Override
    public JsonObject getRestRowExpression() {
        return null;
    }


    @Override
    public String getTable() {
        return null;
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
    public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
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


    public static class SimpleGraphInsert extends GraphInsert {

        private final String cypher;


        public SimpleGraphInsert( String cypher ) {
            this.cypher = cypher;
        }


        @Override
        public String getCypherRowExpression() {
            return cypher;
        }


        @Override
        public String getCypher() {
            return cypher;
        }

    }

}
