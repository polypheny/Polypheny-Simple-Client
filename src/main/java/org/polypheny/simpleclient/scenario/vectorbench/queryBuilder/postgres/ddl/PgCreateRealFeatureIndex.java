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

package org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.postgres.ddl;

import java.util.Map;
import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class PgCreateRealFeatureIndex extends QueryBuilder {

    private final String method;
    private final String opClass;
    private final int m;
    private final int efConstruction;
    private final int lists;


    public PgCreateRealFeatureIndex( String method, String metric, int m, int efConstruction, int lists ) {
        this.method = method;
        this.opClass = toOpClass( metric );
        this.m = m;
        this.efConstruction = efConstruction;
        this.lists = lists;
    }


    private static String toOpClass( String metric ) {
        return switch ( metric.toLowerCase() ) {
            case "cosine" -> "vector_cosine_ops";
            case "l2" -> "vector_l2_ops";
            case "l1" -> "vector_l1_ops";
            case "ip" -> "vector_ip_ops";
            default -> throw new IllegalArgumentException( "Provided metric is invalid: " + metric );
        };
    }


    @Override
    public Query getNewQuery() {
        return new PgCreateRealFeatureIndexQuery( method, opClass, m, efConstruction, lists );
    }


    private static class PgCreateRealFeatureIndexQuery extends Query {

        private final String method;
        private final String opClass;
        private final int m;
        private final int efConstruction;
        private final int lists;
        private final boolean isHnsw;


        PgCreateRealFeatureIndexQuery( String method, String opClass, int m, int efConstruction, int lists ) {
            super( false );
            this.method = method;
            this.opClass = opClass;
            this.m = m;
            this.efConstruction = efConstruction;
            this.lists = lists;
            this.isHnsw = method.equals( "hnsw" );
        }


        @Override
        public String getSql() {
            String withClause = isHnsw
                    ? "(m=" + m + ", ef_construction=" + efConstruction + ")"
                    : "(lists=" + lists + ")";
            return "CREATE INDEX ON knn_realfeature USING " + method
                    + " (feature " + opClass + ") WITH " + withClause;
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
