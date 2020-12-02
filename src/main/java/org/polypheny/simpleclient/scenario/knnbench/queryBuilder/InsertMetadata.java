package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;


import com.google.gson.JsonObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.CottontailQuery;
import org.polypheny.simpleclient.query.CottontailQuery.QueryType;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Data;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Entity;
import org.vitrivr.cottontail.grpc.CottontailGrpc.FloatVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Schema;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Tuple;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Vector;


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


        @Override
        public CottontailQuery getCottontail() {
            Map<String, Data> dataMap = new HashMap<>();
            dataMap.put( "id", Data.newBuilder().setIntData( (int) id ).build() );
            dataMap.put( "textdata",  Data.newBuilder().setStringData( textdata ).build() );
            InsertMessage insertMessage = InsertMessage.newBuilder()
                    .setFrom( From.newBuilder().setEntity( Entity.newBuilder().setSchema( Schema.newBuilder().setName( "public" ).build() ).setName( "knn_metadata" ).build() ).build() )
                    .setTuple( Tuple.newBuilder().putAllData( dataMap ).build() )
                    .build();
            return new CottontailQuery( QueryType.INSERT, insertMessage );
        }
    }

}
