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
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

public class SimpleKnnBooleanFeature extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;
    private final int dimension;
    private final int limit;
    private final String norm; // Should be 'JACCARD' or 'HAMMING'
    private final Random random;

    public SimpleKnnBooleanFeature( long randomSeed, int dimension, int limit, String norm ) {
        this.dimension = dimension;
        this.limit = limit;
        this.norm = norm;
        this.random = new Random( randomSeed );
    }

    private Boolean[] getRandomVector() {
        Boolean[] booleans = new Boolean[this.dimension];
        for ( int i = 0; i < this.dimension; i++ ) {
            booleans[i] = random.nextBoolean();
        }
        return booleans;
    }

    @Override
    public synchronized Query getNewQuery() {
        return new SimpleKnnBooleanFeatureQuery( getRandomVector(), limit, norm );
    }

    private static class SimpleKnnBooleanFeatureQuery extends Query {

        private final Boolean[] target;
        private final int limit;
        private final String norm;

        public SimpleKnnBooleanFeatureQuery( Boolean[] target, int limit, String norm ) {
            super( EXPECT_RESULT );
            this.target = target;
            this.limit = limit;
            this.norm = norm;
        }

        @Override
        public String getSql() {
            return "SELECT id, " + norm.toLowerCase() + "_distance(feature, ARRAY" + Arrays.toString( target ) + ") "
                    + "as dist " +
                    "FROM knn_booleanfeature " + // Ensure this tableexists in DataGenerator
                    "ORDER BY dist ASC LIMIT " + limit;
        }


        @Override
        public String getParameterizedSqlQuery() { return null; }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>>
        getParameterValues() { return null; }


        @Override
        public HttpRequest<?> getRest() { return null; }


        @Override
        public String getMongoQl() {
            return "db.knn_booleanfeature.aggregate([{" +
                    "    \"$vectorSearch\": {" +
                    "        \"path\": \"feature\"," +
                    "        \"queryVector\": " + Arrays.toString( target )
                    + "," +
                    "        \"metric\": \"" + norm + "\"," +
                    "        \"limit\": " + limit +
                    "    }" +
                    "}])";
        }
    }
}
