package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;


import java.util.Arrays;
import java.util.Random;
import kong.unirest.HttpRequest;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class MetadataKnnRealFeature extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final long randomSeed;
    private final int dimension;
    private final int limit;
    private final String norm;

    private final Random random;


    public MetadataKnnRealFeature( long randomSeed, int dimension, int limit, String norm ) {
        this.randomSeed = randomSeed;
        this.dimension = dimension;
        this.limit = limit;
        this.norm = norm;

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
    public synchronized Query getNewQuery() {
        return new MetadataKnnRealFeatureQuery(
                getRandomVector(),
                limit,
                norm
        );
    }


    private static class MetadataKnnRealFeatureQuery extends Query {

        private static final String SQL_1 = "SELECT knn_metadata.id, knn_metadata.textdata FROM knn_metadata WHERE knn_metadata.id in ( SELECT id FROM knn_realfeature ORDER BY distance(feature, ARRAY";
        private static final String SQL_2 = ", '";
        private static final String SQL_3 = "') ASC LIMIT ";
        private static final String SQL_4 = ")";

        private final Float[] target;
        private final int limit;
        private final String norm;


        private MetadataKnnRealFeatureQuery( Float[] target, int limit, String norm ) {
            super( EXPECT_RESULT );
            this.target = target;
            this.limit = limit;
            this.norm = norm;
        }


        @Override
        public String getSql() {
            return SQL_1 + Arrays.toString( target ) + SQL_2 + norm + SQL_3 + limit + SQL_4;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }

    }

}
