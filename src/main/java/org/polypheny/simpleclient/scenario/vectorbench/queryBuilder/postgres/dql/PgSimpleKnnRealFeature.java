/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2026 The Polypheny Project
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
 */

package org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.postgres.dql;

import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import java.util.Map;
import java.util.Random;


public class PgSimpleKnnRealFeature extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final int dimension;
    private final int limit;
    private final String operator;
    private final Random random;


    public PgSimpleKnnRealFeature( long randomSeed, int dimension, int limit, String distanceMetric ) {
        this.dimension = dimension;
        this.limit = limit;
        this.random = new Random( randomSeed );
        this.operator = toOperator( distanceMetric );
    }


    private static String toOperator( String metric ) {
        return switch ( metric.toLowerCase() ) {
            case "cosine" -> "<=>";
            case "l2" -> "<->";
            case "l1" -> "<+>";
            default -> throw new IllegalArgumentException( "Provided metric is invalid: " + metric );
        };
    }


    private Float[] getRandomVector() {
        Float[] floats = new Float[dimension];
        for ( int i = 0; i < dimension; i++ ) {
            floats[i] = random.nextInt( 100 ) / 100.0f;
        }
        return floats;
    }


    @Override
    public synchronized Query getNewQuery() {
        return new PgSimpleKnnRealFeatureQuery( getRandomVector(), limit, operator );
    }


    private static class PgSimpleKnnRealFeatureQuery extends Query {

        private final Float[] target;
        private final int limit;
        private final String operator;


        PgSimpleKnnRealFeatureQuery( Float[] target, int limit, String operator ) {
            super( EXPECT_RESULT );
            this.target = target;
            this.limit = limit;
            this.operator = operator;
        }


        @Override
        public String getSql() {
            StringBuilder sb = new StringBuilder( "SELECT id, feature " );
            sb.append( operator ).append( " '[" );
            for ( int i = 0; i < target.length; i++ ) {
                if ( i > 0 ) sb.append( "," );
                sb.append( target[i] );
            }
            sb.append( "]' AS dist FROM knn_realfeature ORDER BY dist ASC LIMIT " ).append( limit );
            return sb.toString();
        }


        @Override
        public String getParameterizedSqlQuery() { return null; }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() { return null; }


        @Override
        public HttpRequest<?> getRest() { return null; }


        @Override
        public String getMongoQl() { return null; }
    }
}

