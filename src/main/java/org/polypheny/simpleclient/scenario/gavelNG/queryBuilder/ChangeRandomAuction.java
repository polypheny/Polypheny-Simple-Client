/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 The Polypheny Project
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


import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.text.TextProducer;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.gavelNG.GavelNGConfig;


public class ChangeRandomAuction extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private final int numberOfAuctions;
    private final int auctionTitleMinLength;
    private final int auctionTitleMaxLength;
    private final int auctionDescriptionMinLength;
    private final int auctionDescriptionMaxLength;

    private final TextProducer text;


    public ChangeRandomAuction( int numberOfAuctions, GavelNGConfig config ) {
        this.numberOfAuctions = numberOfAuctions;
        this.auctionTitleMinLength = config.auctionTitleMinLength;
        this.auctionTitleMaxLength = config.auctionTitleMaxLength;
        this.auctionDescriptionMinLength = config.auctionDescriptionMinLength;
        this.auctionDescriptionMaxLength = config.auctionDescriptionMaxLength;
        text = Fairy.create().textProducer();
    }


    @Override
    public Query getNewQuery() {
        return new ChangeRandomAuctionQuery(
                ThreadLocalRandom.current().nextInt( 1, numberOfAuctions + 1 ),
                text.latinWord( ThreadLocalRandom.current().nextInt( auctionTitleMinLength, auctionTitleMaxLength + 1 ) ),
                text.paragraph( ThreadLocalRandom.current().nextInt( auctionDescriptionMinLength, auctionDescriptionMaxLength + 1 ) )
        );
    }


    private static class ChangeRandomAuctionQuery extends Query {

        private final int auctionId;
        private final String title;
        private final String description;


        public ChangeRandomAuctionQuery( int auctionId, String title, String description ) {
            super( EXPECT_RESULT );
            this.auctionId = auctionId;
            this.title = title;
            this.description = description;
        }


        @Override
        public String getSql() {
            return "UPDATE auction SET "
                    + "title = '" + title + "', "
                    + "description = '" + description + "' "
                    + "WHERE id = "
                    + auctionId;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "UPDATE auction SET title = ?, description = ? WHERE id = ?";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.VARCHAR, title ) );
            map.put( 2, new ImmutablePair<>( DataTypes.VARCHAR, description ) );
            map.put( 3, new ImmutablePair<>( DataTypes.INTEGER, auctionId ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            JsonObject set = new JsonObject();
            set.add( "public.auction.title", new JsonPrimitive( title ) );
            set.add( "public.auction.description", new JsonPrimitive( description ) );

            Map<String, String> where = new LinkedHashMap<>();
            where.put( "public.auction.id", "=" + auctionId );

            return buildRestUpdate( "public.auction", set, where );
        }


        @Override
        public String getMongoQl() {
            // document model on non-document stores is not yet supported
            // return "db.auction.update({\"id\":" + auctionId + "},{\"$set\":{ \"description\":" + maybeQuote( description ) + ", \"title\":" + maybeQuote( title ) + "}})";
            return "db.\"auction\".count({\"id\":" + auctionId + "})";
        }

    }

}
