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
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertIntFeature extends QueryBuilder {
    private static final boolean EXPECT_RESULT = false;

    private static final AtomicInteger nextId = new AtomicInteger( 1 );
    private final long randomSeed;
    private final int dimension;

    private final Random random;


    public InsertIntFeature( long randomSeed, int dimension ) {
        this.randomSeed = randomSeed;
        this.dimension = dimension;

        this.random = new Random( randomSeed );
    }

    private Integer[] getRandomVector() {
        Integer[] integers = new Integer[this.dimension];
        for ( int i = 0; i < this.dimension; i++ ) {
            integers[i] = random.nextInt( 500 );
        }

        return integers;
    }


    @Override
    public synchronized BatchableInsert getNewQuery() {
        return new InsertIntFeatureQuery(
                nextId.getAndIncrement(),
                getRandomVector()
        );
    }


    private static class InsertIntFeatureQuery extends BatchableInsert {
        private static final String SQL = "INSERT INTO knn_intfeature (id, feature) VALUES ";
        private final int id;
        private final Integer[] feature;


        private InsertIntFeatureQuery( int id, Integer[] feature ) {
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
            map.put( 2, new ImmutablePair<>( DataTypes.ARRAY_INT, feature ) );
            return map;
        }


        @Override
        public JsonObject getRestRowExpression() {
            return null;
        }


        @Override
        public String getTable() {
            return "public.knn_intfeature";
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
