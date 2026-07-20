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

package org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.ddl;

import java.util.Map;
import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class CreateRealFeatureIndex extends QueryBuilder {

    private final String store;
    private final String method;
    private final String metric;
    private final int m;
    private final int efConstruction;
    private final int lists;


    public CreateRealFeatureIndex( String store, String method, String metric, int m, int efConstruction, int lists ) {
        this.store = store;
        this.method = method;
        this.metric = metric;
        this.m = m;
        this.efConstruction = efConstruction;
        this.lists = lists;
    }


    @Override
    public Query getNewQuery() {
        return new CreateRealFeatureIndexQuery( store, method, metric, m, efConstruction, lists );
    }


    private static class CreateRealFeatureIndexQuery extends Query {

        private final String store;
        private final String method;
        private final String metric;
        private final int m;
        private final int efConstruction;
        private final int lists;
        private final boolean isHnsw;


        CreateRealFeatureIndexQuery( String store, String method, String metric, int m, int efConstruction, int lists ) {
            super( false );
            this.store = store;
            this.method = method;
            this.metric = metric;
            this.m = m;
            this.efConstruction = efConstruction;
            this.lists = lists;
            this.isHnsw = method.equals( "hnsw" );
        }


        @Override
        public String getSql() {
            String sql = "ALTER TABLE knn_realfeature ADD INDEX feature_" + method
                    + " ON (feature) USING " + method;
            if ( store != null ) {
                sql += " ON STORE \"" + store + "\"";
            }
            String params = isHnsw
                    ? "m=" + m + ", ef_construction=" + efConstruction
                    : "lists=" + lists;
            sql += " WITH (metric='" + metric + "', " + params + ")";
            return sql;
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
        public HttpRequest<?> getRest() {
            return null;
        }


        @Override
        public String getMongoQl() {
            return null;
        }

    }

}
