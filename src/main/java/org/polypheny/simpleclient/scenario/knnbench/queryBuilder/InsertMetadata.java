package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;


import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertMetadata extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private static final AtomicInteger nextId = new AtomicInteger( 1 );


    @Override
    public BatchableInsert getNewQuery() {
        return new InsertMetadataQuery( nextId.getAndIncrement() );
    }


    private static class InsertMetadataQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO knn_metadata (id, textdata) VALUES ";

        private final int id;
        private final String textdata;


        private InsertMetadataQuery( int id ) {
            super( EXPECT_RESULT );
            this.id = id;
            this.textdata = "textdata_" + id + "_blubber";
        }


        @Override
        public String getSqlRowExpression() {
            return "(" + id + ", '" + textdata + "')";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, id ) );
            map.put( 2, new ImmutablePair<>( DataTypes.VARCHAR, textdata ) );
            return map;
        }


        @Override
        public JsonObject getRestRowExpression() {
            return null;
        }


        @Override
        public String getTable() {
            return "public.knn_metadata";
        }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }

        // TODO JS: Implement getCottontail()

    }

}
