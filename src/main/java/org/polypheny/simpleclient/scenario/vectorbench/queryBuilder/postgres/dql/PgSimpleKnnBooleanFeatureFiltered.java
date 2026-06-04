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

public class PgSimpleKnnBooleanFeatureFiltered extends QueryBuilder {
    private static final boolean EXPECT_RESULT = true;

    private final int dimension;
    private final int limit;
    private final String operator;
    private final String filterCategory;
    private final Random random;


    public PgSimpleKnnBooleanFeatureFiltered( long randomSeed, int dimension, int limit, String booleanDistanceMetric, String filterCategory ) {
        this.dimension = dimension;
        this.limit = limit;
        this.filterCategory = filterCategory;
        this.random = new Random( randomSeed );
        this.operator = toOperator( booleanDistanceMetric );
    }


    private static String toOperator( String metric ) {
        return switch ( metric.toLowerCase() ) {
            case "hamming" -> "<~>";
            case "jaccard" -> "<%>";
            default -> throw new IllegalArgumentException( "Provided boolean metric is invalid: " + metric );
        };
    }


    private String getRandomBits() {
        StringBuilder sb = new StringBuilder( dimension );
        for ( int i = 0; i < dimension; i++ ) {
            sb.append( random.nextBoolean() ? '1' : '0' );
        }
        return sb.toString();
    }


    @Override
    public synchronized Query getNewQuery() {
        return new PgSimpleKnnBooleanFeatureFilteredQuery( getRandomBits(), dimension, limit, operator, filterCategory );
    }


    private static class PgSimpleKnnBooleanFeatureFilteredQuery extends Query {

        private final String bits;
        private final int dimension;
        private final int limit;
        private final String operator;
        private final String category;


        PgSimpleKnnBooleanFeatureFilteredQuery( String bits, int dimension, int limit, String operator, String category ) {
            super( EXPECT_RESULT );
            this.bits = bits;
            this.dimension = dimension;
            this.limit = limit;
            this.operator = operator;
            this.category = category;
        }


        @Override
        public String getSql() {
            return "SELECT id, feature " + operator + " '" + bits + "'::bit(" + dimension + ") AS dist "
                    + "FROM knn_booleanfeature WHERE category = '" + category + "' ORDER BY dist ASC LIMIT " + limit;
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
