package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;


import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SimpleMetadata extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final long randomSeed;
    private final int numOfEntries;

    private final Random random;


    public SimpleMetadata( long randomSeed, int numOfEntries ) {
        this.randomSeed = randomSeed;
        this.numOfEntries = numOfEntries;

        this.random = new Random( randomSeed );
    }


    private int getRandomId() {
        return this.random.nextInt( this.numOfEntries );
    }


    @Override
    public Query getNewQuery() {
        return new SimpleMetadataQuery( this.getRandomId() );
    }


    private static class SimpleMetadataQuery extends Query {

        private static final String SQL = "SELECT id, textdata FROM knn_metadata WHERE id = ";

        private final int id;


        private SimpleMetadataQuery( int id ) {
            super( EXPECT_RESULT );
            this.id = id;
        }


        @Override
        public String getSql() {
            return SQL + id;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "?";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, id ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }

    }

}
