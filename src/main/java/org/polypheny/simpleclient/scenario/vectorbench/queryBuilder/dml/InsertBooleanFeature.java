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

package org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dml;

import com.google.gson.JsonObject;
import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class InsertBooleanFeature extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;
    private static final AtomicInteger nextId = new AtomicInteger( 1 );
    private final long randomSeed;
    private final int dimension;
    private final Random random;
    private static final String[] CATEGORIES = {"cat_A", "cat_B", "cat_C", "cat_D"};

    public InsertBooleanFeature( long randomSeed, int dimension ) {
        this.randomSeed = randomSeed;
        this.dimension = dimension;
        this.random = new Random( randomSeed );
    }


    private Boolean[] getRandomVector() {
        Boolean[] booleans = new Boolean[this.dimension];
        for ( int i = 0; i < this.dimension; i++ ) {
            // Generate random bitvectors (true/false)
            booleans[i] = random.nextBoolean();
        }
        return booleans;
    }


    @Override
    public synchronized BatchableInsert getNewQuery() {
        return new InsertBooleanFeatureQuery(
                nextId.getAndIncrement(),
                getRandomVector(),
                CATEGORIES[random.nextInt(CATEGORIES.length)]
        );
    }


    private static class InsertBooleanFeatureQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO knn_booleanfeature (id, category, feature) VALUES ";
        private final int id;
        private final Boolean[] feature;
        private final String randomCategory;

        private InsertBooleanFeatureQuery( int id, Boolean[] feature, String randomCategory ) {
            super( EXPECT_RESULT );
            this.id = id;
            this.feature = feature;
            this.randomCategory = randomCategory;
        }


        @Override
        public String getSqlRowExpression() {
            // Arrays.toString on Boolean[] will result in "[true, false, true...]"
            return "(" + id + ", '" + randomCategory + "', ARRAY" + Arrays.toString( feature ) + ")";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, id ) );
            map.put( 2, new ImmutablePair<>( DataTypes.VARCHAR, randomCategory ) );
            map.put( 3, new ImmutablePair<>( DataTypes.ARRAY_BOOLEAN, feature ) );
            return map;
        }


        @Override
        public JsonObject getRestRowExpression() { return null; }


        @Override
        public String getEntity() { return "public.knn_booleanfeature"; }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public HttpRequest<?> getRest() { return null; }


        @Override
        public String getMongoQl() { return null; }


        private String getBitString( Boolean[] feature ) {
            StringBuilder sb = new StringBuilder(feature.length);
            for ( Boolean b : feature ) {
                sb.append( b ? "1" : "0" );
            }
            return sb.toString();
        }
    }
}
