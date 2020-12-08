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


import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.person.Person;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertAlbum extends QueryBuilder {

    private static final AtomicInteger nextId = new AtomicInteger( 1 );
    private static final Random RANDOM = new Random();

    private static final Locale[] locales = {
            new Locale( "de" ),
            new Locale( "en" ),
            new Locale( "es" ),
            new Locale( "fr" ),
            new Locale( "it" ),
    };

    private final int userId;


    public InsertAlbum( int userId ) {
        this.userId = userId;
    }


    @Override
    public InsertAlbumQuery getNewQuery() {
        Fairy fairy = Fairy.create( locales[RANDOM.nextInt( locales.length )] );
        Person person = fairy.person();
        return new InsertAlbumQuery(
                nextId.getAndIncrement(),
                userId,
                person.getUsername()
        );
    }


    public static class InsertAlbumQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO \"album\" (\"id\", \"user_id\", \"name\") VALUES ";
        public final int album_id;
        private final int user_id;
        private final String name;


        public InsertAlbumQuery( int album_id, int user_id, String name ) {
            super( false );
            this.album_id = album_id;
            this.user_id = user_id;
            this.name = name;
        }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public String getSqlRowExpression() {
            return "("
                    + album_id + ","
                    + user_id + ","
                    + "'" + name + "'"
                    + ")";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, album_id ) );
            map.put( 2, new ImmutablePair<>( DataTypes.INTEGER, user_id ) );
            map.put( 3, new ImmutablePair<>( DataTypes.VARCHAR, name ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }


        @Override
        public JsonObject getRestRowExpression() {
            return null;
        }


        @Override
        public String getTable() {
            return "public.album";
        }

    }

}