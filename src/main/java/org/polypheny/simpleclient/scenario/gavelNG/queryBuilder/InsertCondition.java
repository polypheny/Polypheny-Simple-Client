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

package org.polypheny.simpleclient.scenario.gavelNG.queryBuilder;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import kong.unirest.HttpRequest;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;

public class InsertCondition extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private static final AtomicInteger nextCategoryId = new AtomicInteger( 1 );
    private final ArrayList<String> conditions;


    public InsertCondition() {
        this.conditions = new ArrayList<>();

        try ( InputStream is = ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavelNG/conditions.txt" ) ) {
            if ( is == null ) {
                throw new RuntimeException( "Conditions list file not found!" );
            }
            try ( InputStreamReader in = new InputStreamReader( is ) ) {
                try ( Stream<String> stream = new BufferedReader( in ).lines() ) {
                    stream.forEach( conditions::add );
                }
            }
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }


    }


    @Override
    public BatchableInsert getNewQuery() {
        if ( conditions.size() == 0 ) {
            throw new RuntimeException( "List of conditions is empty" );
        }
        return new InsertConditionQuery(
                nextCategoryId.getAndIncrement(),
                conditions.remove( 0 )
        );
    }


    private static class InsertConditionQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO \"condition\" (id, \"condition\") VALUES ";

        private final int categoryId;
        private final String condition;


        public InsertConditionQuery( int categoryId, String condition ) {
            super( EXPECT_RESULT );
            this.categoryId = categoryId;
            this.condition = condition;
        }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public String getSqlRowExpression() {
            return "("
                    + categoryId + ","
                    + "'" + StringEscapeUtils.escapeSql( condition ) + "',"
                    + ")";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, categoryId ) );
            map.put( 2, new ImmutablePair<>( DataTypes.VARCHAR, condition ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return buildRestInsert( "public.condition", ImmutableList.of( getRestRowExpression() ) );
        }


        @Override
        public String getMongoQl() {
            return buildMongoQlInsert( "\"condition\"", ImmutableList.of( "id", "\"condition\"" ), ImmutableList.of( categoryId, condition ) );
        }


        @Override
        public JsonObject getRestRowExpression() {
            JsonObject row = new JsonObject();
            row.add( "public.condition.id", new JsonPrimitive( categoryId ) );
            row.add( "public.condition.condition", new JsonPrimitive( condition ) );
            return row;
        }


        @Override
        public String getEntity() {
            return "public.condition";
        }

    }

}
