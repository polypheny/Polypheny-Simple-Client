package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;


import com.google.gson.JsonObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertRealFeature extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private static final AtomicInteger nextId = new AtomicInteger( 1 );
    private final long randomSeed;
    private final int dimension;

    private final Random random;


    public InsertRealFeature( long randomSeed, int dimension ) {
        this.randomSeed = randomSeed;
        this.dimension = dimension;

        this.random = new Random( randomSeed );
    }


    private Float[] getRandomVector() {
        Float[] floats = new Float[this.dimension];
        for ( int i = 0; i < this.dimension; i++ ) {
            floats[i] = random.nextInt( 100 ) / 100.0f;
        }

        return floats;
    }


    @Override
    public synchronized BatchableInsert getNewQuery() {
        return new InsertRealFeatureQuery(
                nextId.getAndIncrement(),
                getRandomVector()
        );
    }


    private static class InsertRealFeatureQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO knn_realfeature (id, feature) VALUES ";
        private final int id;
        private final Float[] feature;


        private InsertRealFeatureQuery( int id, Float[] feature ) {
            super( EXPECT_RESULT );
            this.id = id;
            this.feature = feature;
        }


        @Override
        public String getSqlRowExpression() {
            return "(" + id + ", ARRAY" + Arrays.toString( feature ) + ")";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, id ) );
            map.put( 2, new ImmutablePair<>( DataTypes.ARRAY_REAL, feature ) );
            return map;
        }


        @Override
        public JsonObject getRestRowExpression() {
            return null;
        }


        @Override
        public String getTable() {
            return "public.knn_realfeature";
        }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }

    }

}
