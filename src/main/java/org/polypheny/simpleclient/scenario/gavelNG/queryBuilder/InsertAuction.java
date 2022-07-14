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
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import kong.unirest.HttpRequest;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertAuction extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private final int userId;
    private final int categoryId;
    private final int conditionId;
    private final LocalDateTime startDate;
    private final LocalDateTime endDate;
    private final String title;
    private final String description;
    private final int auctionId;


    public InsertAuction( int auctionId, int userId, int categoryId, int conditionId, LocalDateTime startDate, LocalDateTime endDate, String title, String description ) {
        this.auctionId = auctionId;
        this.userId = userId;
        this.categoryId = categoryId;
        this.conditionId = conditionId;
        this.startDate = startDate;
        this.endDate = endDate;
        this.title = title;
        this.description = description;
    }


    @Override
    public BatchableInsert getNewQuery() {
        return new InsertAuctionQuery(
                auctionId,
                userId,
                categoryId,
                conditionId,
                startDate,
                endDate,
                title,
                description );
    }


    @Getter
    public static class InsertAuctionQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO auction(id, title, description, start_date, end_date, category, \"user\", \"condition\") VALUES ";

        private final int auctionId;
        private final int userId;
        private final int categoryId;
        private final int conditionId;
        private final LocalDateTime startDate;
        private final LocalDateTime endDate;
        private final String title;
        private final String description;


        public InsertAuctionQuery( int auctionId, int userId, int categoryId, int conditionId, LocalDateTime startDate, LocalDateTime endDate, String title, String description ) {
            super( EXPECT_RESULT );
            this.auctionId = auctionId;
            this.userId = userId;
            this.categoryId = categoryId;
            this.conditionId = conditionId;
            this.startDate = startDate;
            this.endDate = endDate;
            this.title = title;
            this.description = description;
        }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public String getSqlRowExpression() {
            return "("
                    + auctionId + ","
                    + "'" + title + "',"
                    + "'" + description + "',"
                    + "timestamp '" + startDate.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss" ) ) + "',"
                    + "timestamp '" + endDate.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss" ) ) + "',"
                    + categoryId + ","
                    + userId + ","
                    + conditionId
                    + ")";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?, ?, ?, ?, ?, ?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, auctionId ) );
            map.put( 2, new ImmutablePair<>( DataTypes.VARCHAR, title ) );
            map.put( 3, new ImmutablePair<>( DataTypes.VARCHAR, description ) );
            map.put( 4, new ImmutablePair<>( DataTypes.TIMESTAMP, Timestamp.valueOf( startDate ) ) );
            map.put( 5, new ImmutablePair<>( DataTypes.TIMESTAMP, Timestamp.valueOf( endDate ) ) );
            map.put( 6, new ImmutablePair<>( DataTypes.INTEGER, categoryId ) );
            map.put( 7, new ImmutablePair<>( DataTypes.INTEGER, userId ) );
            map.put( 8, new ImmutablePair<>( DataTypes.INTEGER, conditionId ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return buildRestInsert( "public.auction", ImmutableList.of( getRestRowExpression() ) );
        }


        @Override
        public String getMongoQl() {
            return "db.auction.insert({\"id\":" + maybeQuote( auctionId )
                    + ",\"title\":" + maybeQuote( title )
                    + ",\"description\":" + maybeQuote( description )
                    + ",\"start_date\":" + startDate.toLocalDate().toEpochDay()
                    + ",\"end_date\":" + endDate.toLocalDate().toEpochDay()
                    + ",\"category\":" + maybeQuote( categoryId )
                    + ",\"user\":" + maybeQuote( userId )
                    + ",\"condition\":" + maybeQuote( conditionId ) + "})";
        }


        @Override
        public JsonObject getRestRowExpression() {
            JsonObject row = new JsonObject();
            row.add( "public.auction.id", new JsonPrimitive( auctionId ) );
            row.add( "public.auction.title", new JsonPrimitive( title ) );
            row.add( "public.auction.description", new JsonPrimitive( description ) );
            row.add( "public.auction.start_date", new JsonPrimitive( startDate.format( DateTimeFormatter.ISO_LOCAL_DATE_TIME ) ) );
            row.add( "public.auction.end_date", new JsonPrimitive( endDate.format( DateTimeFormatter.ISO_LOCAL_DATE_TIME ) ) );
            row.add( "public.auction.category", new JsonPrimitive( categoryId ) );
            row.add( "public.auction.user", new JsonPrimitive( userId ) );
            row.add( "public.auction.condition", new JsonPrimitive( conditionId ) );
            return row;
        }


        @Override
        public String getEntity() {
            return "public.auction";
        }


    }

}
