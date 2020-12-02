package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;


import java.util.Arrays;
import java.util.Random;
import kong.unirest.HttpRequest;
import org.polypheny.simpleclient.query.CottontailQuery;
import org.polypheny.simpleclient.query.CottontailQuery.QueryType;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Entity;
import org.vitrivr.cottontail.grpc.CottontailGrpc.FloatVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.IntVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Knn;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Knn.Distance;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Schema;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Vector;


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


        @Override
        public CottontailQuery getCottontail() {
            CottontailGrpc.Query query = CottontailGrpc.Query.newBuilder()
                    .setFrom( From.newBuilder().setEntity( Entity.newBuilder().setSchema( Schema.newBuilder().setName( "public" ).build() ).setName( "knn_realfeature" ).build() ) )
                    .setLimit( limit )
                    .setKnn( Knn.newBuilder()
                            .setAttribute( "feature" )
                            .setK( limit )
                            .addQuery( Vector.newBuilder().setFloatVector( FloatVector.newBuilder().addAllVector( Arrays.asList( target ) ).build() ).build() )
                            .setDistance( getDistance( norm ) )
                            .build() )
                    .build();
            return new CottontailQuery(
                    QueryType.QUERY,
                    query
            );
        }


        private static CottontailGrpc.Knn.Distance getDistance( String norm ) {
            if ( "L2".equalsIgnoreCase( norm ) ) {
                return Distance.L2;
            }
            if ( "L1".equalsIgnoreCase( norm ) ) {
                return Distance.L1;
            }
            if ( "L2SQUARED".equalsIgnoreCase( norm ) ) {
                return Distance.L2SQUARED;
            }
            if ( "CHISQUARED".equalsIgnoreCase( norm ) ) {
                return Distance.CHISQUARED;
            }
            if ( "COSINE".equalsIgnoreCase( norm ) ) {
                return Distance.COSINE;
            }

            throw new RuntimeException( "Unsupported norm: " + norm );
        }
    }

}
