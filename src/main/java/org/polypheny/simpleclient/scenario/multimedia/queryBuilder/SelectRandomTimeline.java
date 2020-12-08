/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
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

package org.polypheny.simpleclient.scenario.multimedia.queryBuilder;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SelectRandomTimeline extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final int numberOfUsers;


    public SelectRandomTimeline( int numberOfUsers ) {
        this.numberOfUsers = numberOfUsers;
    }


    @Override
    public Query getNewQuery() {
        int userId = ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 );
        return new SelectRandomTimelineQuery( userId );
    }


    public static class SelectRandomTimelineQuery extends Query {

        private final int userId;


        public SelectRandomTimelineQuery( int userId ) {
            super( EXPECT_RESULT );
            this.userId = userId;
        }


        @Override
        public String getSql() {
            //TODO LIMIT
            return "SELECT friend.firstname, friend.profile_pic, timeline.message, timeline.img, timeline.\"video\", timeline.\"sound\" "
                    + "FROM public.timeline, "
                    + "public.\"users\" usr, "
                    + "public.\"users\" friend, "
                    + "public.followers "
                    + "WHERE usr.id = followers.\"user_id\" "
                    + "AND friend.id = followers.friend_id "
                    + "AND timeline.user_id = friend.id "
                    + "AND usr.id=" + userId;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "SELECT friend.firstname, friend.profile_pic, timeline.message, timeline.img, timeline.\"video\", timeline.\"sound\" "
                    + "FROM public.timeline, "
                    + "public.\"users\" usr, "
                    + "public.\"users\" friend, "
                    + "public.followers "
                    + "WHERE usr.id = followers.\"user_id\" "
                    + "AND friend.id = followers.friend_id "
                    + "AND timeline.user_id = friend.id "
                    + "AND usr.id=?";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, userId ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }

    }

}