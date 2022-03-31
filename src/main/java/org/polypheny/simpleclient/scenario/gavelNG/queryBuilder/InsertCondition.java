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
        public String getTable() {
            return "public.condition";
        }

    }

}
