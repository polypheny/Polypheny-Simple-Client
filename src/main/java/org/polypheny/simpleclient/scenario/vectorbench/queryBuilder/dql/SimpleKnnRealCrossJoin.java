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

package org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql;

import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import java.util.Map;
import java.util.Random;

public class SimpleKnnRealCrossJoin extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final int dimension;
    private final int limit;
    private final String norm;

    private final Random random;


    public SimpleKnnRealCrossJoin( long randomSeed, int dimension, int limit, String norm ) {
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
        return new SimpleKnnRealCrossJoin.SimpleKnnRealCrossJoinQuery(
                getRandomVector(),
                limit,
                norm );
    }


    private static class SimpleKnnRealCrossJoinQuery extends Query {

        private static final String SQL_1 = "SELECT t1.id, distance(t1.feature, t2.feature,";
        private static final String SQL_2 = ") as dist FROM knn_realfeature t1, knn_realfeature t2 WHERE t2.id = 1 ORDER BY dist ASC LIMIT ";

        private final Float[] target;
        private final int limit;
        private final String norm;


        public SimpleKnnRealCrossJoinQuery( Float[] target, int limit, String norm ) {
            super( EXPECT_RESULT );
            this.target = target;
            this.limit = limit;
            this.norm = norm;
        }


        @Override
        public String getSql() {
            return SQL_1 + " '" + norm + "' " + SQL_2 + limit;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            return Map.of();
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }


        @Override
        public String getMongoQl() {
            return null;
        }

    }
}
