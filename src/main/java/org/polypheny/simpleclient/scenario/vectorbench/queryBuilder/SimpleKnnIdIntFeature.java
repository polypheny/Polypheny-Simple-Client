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

package org.polypheny.simpleclient.scenario.vectorbench.queryBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SimpleKnnIdIntFeature extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final long randomSeed;
    private final int dimension;
    private final int limit;
    private final String norm;

    private final Random random;


    public SimpleKnnIdIntFeature( long randomSeed, int dimension, int limit, String norm ) {
        this.randomSeed = randomSeed;
        this.dimension = dimension;

        this.random = new Random( randomSeed );
        this.limit = limit;
        this.norm = norm;
    }


    private Integer[] getRandomVector() {
        Integer[] integers = new Integer[this.dimension];
        for ( int i = 0; i < this.dimension; i++ ) {
            integers[i] = random.nextInt( 500 );
        }

        return integers;
    }


    @Override
    public synchronized Query getNewQuery() {
        return new SimpleKnnIdIntFeatureQuery(
                getRandomVector(),
                limit,
                norm
        );
    }


    private static class SimpleKnnIdIntFeatureQuery extends Query {

        private static final String SQL_1 = "SELECT closest.dist FROM ( SELECT id, ";
        private static final String SQL_2 = "feature, ";
        private static final String SQL_3 =  ") AS dist FROM knn_intfeature ORDER BY dist ASC LIMIT ";
        private static final String SQL_4 = ") AS closest";

        private static final String SQL_L1 = "l1_distance(";
        private static final String SQL_L2 = "l2_distance(";
        private static final String SQL_COS = "cos_distance(";

        private final Integer[] target;
        private final int limit;
        private final String norm;


        public SimpleKnnIdIntFeatureQuery( Integer[] target, int limit, String norm ) {
            super( EXPECT_RESULT );
            this.target = target;
            this.limit = limit;
            this.norm = norm;
        }


        @Override
        public String getSql() {
            String distance_sql = "";
            switch ( norm ) {
                case "L1" -> distance_sql = SQL_L1;
                case "L2" -> distance_sql = SQL_L2;
                case "COS" -> distance_sql = SQL_COS;
            }
            if ( distance_sql.isEmpty() ){
                return SQL_1 + "ARRAY" + Arrays.toString( target ) + SQL_2 + " '" + norm + "' " + SQL_3 + limit + SQL_4;

            } else {
                return SQL_1 + distance_sql + SQL_2 + "ARRAY" + Arrays.toString( target )  + SQL_3 + limit + SQL_4;

            }
        }


        @Override
        public String getParameterizedSqlQuery() {
            return null;
            //return SQL_1 + "?" + SQL_2 + "'" + norm + "'" + SQL_3 + limit + SQL_4;
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.ARRAY_INT, target ) );
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
