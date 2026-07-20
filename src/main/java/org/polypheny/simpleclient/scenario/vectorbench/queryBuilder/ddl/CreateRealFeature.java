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


public class CreateRealFeature extends QueryBuilder {

    private final String store;
    private final int dimension;
    private final boolean supportsNotNullArray;


    public CreateRealFeature( String store, int dimension, boolean supportsNotNullArray ) {
        this.store = store;
        this.dimension = dimension;
        this.supportsNotNullArray = supportsNotNullArray;
    }


    @Override
    public Query getNewQuery() {
        return new CreateRealFeatureQuery( store, dimension, supportsNotNullArray );
    }


    private static class CreateRealFeatureQuery extends Query {

        private final String store;
        private final int dimension;
        private final boolean supportsNotNullArray;


        CreateRealFeatureQuery( String store, int dimension, boolean supportsNotNullArray ) {
            super( false );
            this.store = store;
            this.dimension = dimension;
            this.supportsNotNullArray = supportsNotNullArray;
        }


        @Override
        public String getSql() {
            String elementsNullable = supportsNotNullArray ? " NOT NULL " : " ";
            String sql = "CREATE TABLE knn_realfeature ("
                    + "id INTEGER NOT NULL, "
                    + "category VARCHAR(50), "
                    + "feature REAL" + elementsNullable + "ARRAY(1, " + this.dimension + "), "
                    + "PRIMARY KEY(id))";
            if ( this.store != null ) {
                sql += " ON STORE \"" + this.store + "\"";
            }
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
