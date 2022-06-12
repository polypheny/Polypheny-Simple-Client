/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2022 The Polypheny Project
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

package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import com.google.gson.JsonObject;
import java.util.Map;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.scenario.graph.GraphBench;

public abstract class GraphInsert extends BatchableInsert {

    public GraphInsert() {
        super( GraphBench.EXPECTED_RESULT );
    }


    @Override
    public String getSqlRowExpression() {
        return null;
    }


    @Override
    public JsonObject getRestRowExpression() {
        return null;
    }


    @Override
    public String getTable() {
        return null;
    }


    @Override
    public String getSql() {
        return null;
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


    public static class SimpleGraphInsert extends GraphInsert {

        private final String cypher;


        public SimpleGraphInsert( String cypher ) {
            this.cypher = cypher;
        }


        @Override
        public String getCypherRowExpression() {
            return cypher;
        }


        @Override
        public String getCypher() {
            return cypher;
        }

    }

}
