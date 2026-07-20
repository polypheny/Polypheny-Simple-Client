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


public class PgInsertRealFeature extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;
    private static final AtomicInteger nextId = new AtomicInteger( 1 );
    private static final String[] CATEGORIES = { "cat_A", "cat_B", "cat_C", "cat_D" };

    private final int dimension;
    private final Random random;


    public PgInsertRealFeature( long randomSeed, int dimension ) {
        this.dimension = dimension;
        this.random = new Random( randomSeed );
    }


    private Float[] getRandomVector() {
        Float[] floats = new Float[dimension];
        for ( int i = 0; i < dimension; i++ ) {
            floats[i] = random.nextInt( 100 ) / 100.0f;
        }
        return floats;
    }


    @Override
    public synchronized BatchableInsert getNewQuery() {
        return new PgInsertRealFeatureQuery(
                nextId.getAndIncrement(),
                getRandomVector(),
                CATEGORIES[random.nextInt( CATEGORIES.length )]
        );
    }


    private static class PgInsertRealFeatureQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO knn_realfeature (id, category, feature) VALUES ";
        private final int id;
        private final Float[] feature;
        private final String category;


        PgInsertRealFeatureQuery( int id, Float[] feature, String category ) {
            super( EXPECT_RESULT );
            this.id = id;
            this.feature = feature;
            this.category = category;
        }


        @Override
        public String getSqlRowExpression() {
            StringBuilder sb = new StringBuilder( "(" );
            sb.append( id ).append( ", '" ).append( category ).append( "', '[" );
            for ( int i = 0; i < feature.length; i++ ) {
                if ( i > 0 )
                    sb.append( "," );
                sb.append( feature[i] );
            }
            sb.append( "]')" );
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
            return "public.knn_realfeature";
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
