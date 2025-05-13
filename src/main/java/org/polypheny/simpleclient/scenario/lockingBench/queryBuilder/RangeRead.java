/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/21/25, 8:08 PM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.lockingBench.queryBuilder;

import java.util.HashMap;
import java.util.Map;
import kong.unirest.core.HttpRequest;
import kong.unirest.core.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.lockingBench.NumberTracker;
import org.polypheny.simpleclient.scenario.lockingBench.LockingBenchSchema;

public class RangeRead extends QueryBuilder {

    private final LockingBenchSchema schema;
    private final NumberTracker numberTracker;


    public RangeRead( LockingBenchSchema schema, NumberTracker numberTracker ) {
        this.schema = schema;
        this.numberTracker = numberTracker;
    }


    @Override
    public Query getNewQuery() {
        String tableName = schema.getRandomEntityFullName();
        int categoryId = numberTracker.getRandomCategoryId();
        return new RangeReadQuery( tableName, categoryId );
    }


    private static class RangeReadQuery extends Query {

        private final String tableName;
        private final int categoryId;


        public RangeReadQuery( String tableName, int categoryId ) {
            super( true );
            this.tableName = tableName;
            this.categoryId = categoryId;
        }


        @Override
        public String getSql() {
            return "SELECT COUNT(*) AS cnt FROM " + tableName +
                    " WHERE category_id = " + categoryId;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "SELECT COUNT(*) AS cnt FROM " + tableName +
                    " WHERE category_id = ?";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, categoryId ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public." + tableName )
                    .queryString( "_project", "public." + tableName + ".id@num(COUNT)" )
                    .queryString( "_where",
                            "public." + tableName + ".category_id.eq." + categoryId);
        }


        @Override
        public String getMongoQl() {
            return "db." + tableName + ".count({ category_id: " + categoryId + "})";
        }

    }

}
