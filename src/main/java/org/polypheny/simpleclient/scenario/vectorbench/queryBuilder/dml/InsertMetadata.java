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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertMetadata extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private static final AtomicInteger nextId = new AtomicInteger( 1 );


    @Override
    public BatchableInsert getNewQuery() {
        return new InsertMetadataQuery( nextId.getAndIncrement() );
    }


    private static class InsertMetadataQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO knn_metadata (id, textdata) VALUES ";

        private final int id;
        private final String textdata;


        private InsertMetadataQuery( int id ) {
            super( EXPECT_RESULT );
            this.id = id;
            this.textdata = "textdata_" + id + "_blubber";
        }


        @Override
        public String getSqlRowExpression() {
            return "(" + id + ", '" + textdata + "')";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, id ) );
            map.put( 2, new ImmutablePair<>( DataTypes.VARCHAR, textdata ) );
            return map;
        }


        @Override
        public JsonObject getRestRowExpression() {
            return null;
        }


        @Override
        public String getEntity() {
            return "public.knn_metadata";
        }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
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
