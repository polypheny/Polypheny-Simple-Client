package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;


import java.util.Arrays;
import java.util.Random;
import kong.unirest.HttpRequest;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SimpleKnnRealFeature extends QueryBuilder {
    private static final boolean EXPECT_RESULT = true;

    private final long randomSeed;
    private final int dimension;
    private final int limit;
    private final String norm;

    private final Random random;


    public SimpleKnnRealFeature( long randomSeed, int dimension, int limit, String norm ) {
        this.randomSeed = randomSeed;
        this.dimension = dimension;

        this.random = new Random( randomSeed );
        this.limit = limit;
        this.norm = norm;
    }


    private Float[] getRandomVector() {
        Float[] integers = new Float[this.dimension];
        for ( int i = 0; i < this.dimension; i++ ) {
            integers[i] = random.nextInt( 100 ) / 100.0f;
        }

        return integers;
    }


    @Override
    public synchronized Query getNewQuery() {
        return new SimpleKnnRealFeatureQuery(
                getRandomVector(),
                limit,
                norm
        );
    }


    private static class SimpleKnnRealFeatureQuery extends Query {
        private static final String SQL_1 = "SELECT id, distance(feature, ARRAY";
        private static final String SQL_2 = ", '";
        private static final String SQL_3 = "') as dist FROM knn_realfeature ORDER BY dist ASC LIMIT ";

        private final Float[] target;
        private final int limit;
        private final String norm;
        
        public SimpleKnnRealFeatureQuery( Float[] target, int limit, String norm ) {
            super( EXPECT_RESULT );
            this.target = target;
            this.limit = limit;
            this.norm = norm;
        }


        @Override
        public String getSql() {
            return SQL_1 + Arrays.toString( target ) + SQL_2 + norm + SQL_3 + limit;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }
    }
}
