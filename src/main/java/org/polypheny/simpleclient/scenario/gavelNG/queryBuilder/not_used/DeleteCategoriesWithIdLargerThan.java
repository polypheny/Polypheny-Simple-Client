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
 *
 */

package org.polypheny.simpleclient.scenario.gavelNG.queryBuilder.not_used;


import java.util.HashMap;
import java.util.Map;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class DeleteCategoriesWithIdLargerThan extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private final int larger;


    public DeleteCategoriesWithIdLargerThan( int larger ) {
        this.larger = larger;
    }


    @Override
    public Query getNewQuery() {
        return new DeleteCategoriesWithIdLargerThanQuery( larger );
    }


    private static class DeleteCategoriesWithIdLargerThanQuery extends Query {

        private final int larger;


        public DeleteCategoriesWithIdLargerThanQuery( int larger ) {
            super( EXPECT_RESULT );
            this.larger = larger;
        }


        @Override
        public String getSql() {
            return "delete from category where id > " + larger;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "delete from category where id > ?";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, larger ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }


        @Override
        public String getMongoQl() {
            // dml not supported for non document stores // substitute query
            return "db.category.count({\"id\":{\"$gt\":" + larger + "}})";
        }

    }

}
