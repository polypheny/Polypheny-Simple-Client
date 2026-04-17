/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-4/17/26, 2:54 PM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.vectorbench.queryBuilder;

import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class MetadataKnnRealCrossJoin extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final int dimension;
    private final int limit;
    private final String norm;

    private final Random random;


    public MetadataKnnRealCrossJoin( long randomSeed, int dimension, int limit, String norm ) {
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
        return new MetadataKnnRealCrossJoin.MetadataKnnRealCrossJoinQuery(
                getRandomVector(),
                limit,
                norm
        );
    }


    private static class MetadataKnnRealCrossJoinQuery extends Query {

        private static final String SQL_1 = "SELECT knn_metadata.id, knn_metadata.textdata, closest.dist FROM knn_metadata, ( SELECT t1.id, distance(t1.feature, t2.feature, '";
        private static final String SQL_2 = "') AS dist FROM knn_realfeature t1, knn_realfeature t2 WHERE t2.id = 1 ORDER BY dist ASC LIMIT ";
        private static final String SQL_3 = ") AS closest WHERE knn_metadata.id = closest.id ORDER BY closest.dist ASC";
        private final Float[] target;
        private final int limit;
        private final String norm;


        private MetadataKnnRealCrossJoinQuery( Float[] target, int limit, String norm ) {
            super( EXPECT_RESULT );
            this.target = target;
            this.limit = limit;
            this.norm = norm;
        }


        @Override
        public String getSql() {
            return SQL_1 + norm + SQL_2 + limit + SQL_3;
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

    }

}

