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
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class MetadataKnnIntFeature extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final long randomSeed;
    private final int dimension;
    private final int limit;
    private final String norm;

    private final Random random;


    public MetadataKnnIntFeature( long randomSeed, int dimension, int limit, String norm ) {
        this.randomSeed = randomSeed;
        this.dimension = dimension;
        this.limit = limit;
        this.norm = norm;

        this.random = new Random( randomSeed );
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
        return new MetadataKnnIntFeatureQuery(
                getRandomVector(),
                limit,
                norm
        );
    }


    private static class MetadataKnnIntFeatureQuery extends Query {

        private static final String SQL_1 = "SELECT knn_metadata.id, knn_metadata.textdata, closest.dist FROM knn_metadata, ( SELECT id, distance(feature, ";
        private static final String SQL_2 = ", ";
        private static final String SQL_3 = ") AS dist FROM knn_intfeature ORDER BY dist ASC LIMIT ";
        private static final String SQL_4 = ") AS closest WHERE knn_metadata.id = closest.id ORDER BY closest.dist ASC";

        private final Integer[] target;
        private final int limit;
        private final String norm;


        private MetadataKnnIntFeatureQuery( Integer[] target, int limit, String norm ) {
            super( EXPECT_RESULT );
            this.target = target;
            this.limit = limit;
            this.norm = norm;
        }


        @Override
        public String getSql() {
            return SQL_1 + "ARRAY" + Arrays.toString( target ) + SQL_2 + " '" + norm + "' " + SQL_3 + limit + SQL_4;
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


        @Override
        public CottontailQuery getCottontail() {
            throw new RuntimeException( "This query is unsupported by cottontail." );
        }

    }

}
