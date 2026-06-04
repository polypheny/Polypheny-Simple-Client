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


public class PgSimpleKnnRealCrossJoin extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final int limit;
    private final String operator;


    // randomSeed and dimension are accepted for signature parity with the Polypheny-path
    // SimpleKnnRealCrossJoin; the cross-join compares against the feature of row id = 1,
    // so no random query vector is needed.
    public PgSimpleKnnRealCrossJoin( long randomSeed, int dimension, int limit, String distanceMetric ) {
        this.limit = limit;
        this.operator = toOperator( distanceMetric );
    }


    private static String toOperator( String metric ) {
        return switch ( metric.toLowerCase() ) {
            case "cosine" -> "<=>";
            case "l2" -> "<->";
            case "l1" -> "<+>";
            case "inner_product" -> "<#>";
            default -> throw new IllegalArgumentException( "Provided metric is invalid: " + metric );
        };
    }


    @Override
    public synchronized Query getNewQuery() {
        return new PgSimpleKnnRealCrossJoinQuery( limit, operator );
    }


    private static class PgSimpleKnnRealCrossJoinQuery extends Query {

        private final int limit;
        private final String operator;


        PgSimpleKnnRealCrossJoinQuery( int limit, String operator ) {
            super( EXPECT_RESULT );
            this.limit = limit;
            this.operator = operator;
        }


        @Override
        public String getSql() {
            return "SELECT t1.id, t1.feature " + operator + " t2.feature AS dist "
                    + "FROM knn_realfeature t1, knn_realfeature t2 WHERE t2.id = 1 "
                    + "ORDER BY dist ASC LIMIT " + limit;
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
