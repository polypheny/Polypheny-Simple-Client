/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 The Polypheny Project
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.CottontailQuery;
import org.polypheny.simpleclient.query.CottontailQuery.QueryType;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Entity;
import org.vitrivr.cottontail.grpc.CottontailGrpc.FloatVector;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Knn;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Knn.Distance;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Projection;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Schema;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Vector;


public class SimpleKnnIdRealFeature extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final long randomSeed;
    private final int dimension;
    private final int limit;
    private final String norm;

    private final Random random;


    public SimpleKnnIdRealFeature( long randomSeed, int dimension, int limit, String norm ) {
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
        return new SimpleKnnIdRealFeatureQuery(
                getRandomVector(),
                limit,
                norm
        );
    }


    private static class SimpleKnnIdRealFeatureQuery extends Query {

        private static final String SQL_1 = "SELECT closest.dist FROM ( SELECT id, distance(feature, ";
        private static final String SQL_2 = ", ";
        private static final String SQL_3 = ") AS dist FROM knn_intfeature ORDER BY dist ASC LIMIT ";
        private static final String SQL_4 = ") AS closest";

        private final Float[] target;
        private final int limit;
        private final String norm;


        public SimpleKnnIdRealFeatureQuery( Float[] target, int limit, String norm ) {
            super( EXPECT_RESULT );
            this.target = target;
            this.limit = limit;
            this.norm = norm;
        }


        @Override
        public String getSql() {
            return SQL_1 + "ARRAY" + Arrays.toString( target ) + SQL_2 + "'" + norm + "'" + SQL_3 + limit + SQL_4;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return null;
            //return SQL_1 + "?" + SQL_2 + "'" + norm + "'" + SQL_3 + limit + SQL_4;
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.ARRAY_REAL, target ) );
            return map;
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
        public CottontailQuery getCottontail() {
            Map<String, String> projection = new HashMap<>();
            projection.put( "id", "id" );
            CottontailGrpc.Query query = CottontailGrpc.Query.newBuilder()
                    .setFrom( From.newBuilder().setEntity( Entity.newBuilder().setSchema( Schema.newBuilder().setName( "public" ).build() ).setName( "knn_intfeature" ).build() ) )
                    .setLimit( limit )
                    .setKnn( Knn.newBuilder()
                            .setAttribute( "feature" )
                            .setK( limit )
                            .addQuery( Vector.newBuilder().setFloatVector( FloatVector.newBuilder().addAllVector( Arrays.asList( target ) ).build() ).build() )
                            .setDistance( getDistance( norm ) )
                            .build() )
                    .setProjection( Projection.newBuilder().putAllAttributes( projection ).build() )
                    .build();
            return new CottontailQuery(
                    QueryType.QUERY,
                    query
            );
        }


        private static Distance getDistance( String norm ) {
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
