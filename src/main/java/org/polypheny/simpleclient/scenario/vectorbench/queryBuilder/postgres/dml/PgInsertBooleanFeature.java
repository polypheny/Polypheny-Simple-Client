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

package org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.postgres.dml;

import com.google.gson.JsonObject;
import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class PgInsertBooleanFeature extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;
    private static final AtomicInteger nextId = new AtomicInteger( 1 );
    private static final String[] CATEGORIES = { "cat_A", "cat_B", "cat_C", "cat_D" };

    private final int dimension;
    private final Random random;


    public PgInsertBooleanFeature( long randomSeed, int dimension ) {
        this.dimension = dimension;
        this.random = new Random( randomSeed );
    }


    private String getRandomBits() {
        StringBuilder sb = new StringBuilder( dimension );
        for ( int i = 0; i < dimension; i++ ) {
            sb.append( random.nextBoolean() ? '1' : '0' );
        }
        return sb.toString();
    }


    @Override
    public synchronized BatchableInsert getNewQuery() {
        return new PgInsertBooleanFeatureQuery(
                nextId.getAndIncrement(),
                getRandomBits(),
                CATEGORIES[random.nextInt( CATEGORIES.length )]
        );
    }


    private static class PgInsertBooleanFeatureQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO knn_booleanfeature (id, category, feature) VALUES ";
        private final int id;
        private final String feature;
        private final String category;


        PgInsertBooleanFeatureQuery( int id, String feature, String category ) {
            super( EXPECT_RESULT );
            this.id = id;
            this.feature = feature;
            this.category = category;
        }


        @Override
        public String getSqlRowExpression() {
            StringBuilder sb = new StringBuilder( "(" );
            sb.append( id ).append( ", '" ).append( category ).append( "', B'" ).append( feature ).append( "')" );
            return sb.toString();
        }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public String getParameterizedSqlQuery() {
            return null;
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            return null;
        }


        @Override
        public JsonObject getRestRowExpression() {
            return null;
        }


        @Override
        public String getEntity() {
            return "public.knn_booleanfeature";
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
