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

package org.polypheny.simpleclient.scenario.multimedia.queryBuilder;


import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.person.Person;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.File;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.MultipartInsert;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.multimedia.MediaGenerator;


public class InsertUser extends QueryBuilder {

    private static final AtomicInteger nextId = new AtomicInteger( 1 );
    private static final Random RANDOM = new Random();

    private static final Locale[] locales = {
            new Locale( "de" ),
            new Locale( "en" ),
            new Locale( "es" ),
            new Locale( "fr" ),
            new Locale( "it" ),
    };

    private final int imgSize;


    public InsertUser( int imgSize ) {
        this.imgSize = imgSize;
    }


    @Override
    public InsertUserQuery getNewQuery() {
        Fairy fairy = Fairy.create( locales[RANDOM.nextInt( locales.length )] );
        Person person = fairy.person();
        return new InsertUserQuery(
                nextId.getAndIncrement(),
                person.getFirstName(),
                person.getLastName(),
                person.getEmail(),
                person.getPassword(),
                MediaGenerator.generateRandomImg( imgSize, imgSize )
        );
    }


    public static class InsertUserQuery extends MultipartInsert {

        private static final String SQL = "INSERT INTO \"users\" (\"id\", \"firstName\", \"lastName\", \"email\", \"password\", \"profile_pic\") VALUES ";
        public final int id;
        private final String firstName;
        private final String lastName;
        private final String email;
        private final String password;
        private final File profile_pic;


        public InsertUserQuery( int id, String firstName, String lastName, String email, String password, File profile_pic ) {
            super( false );
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.email = email;
            this.password = password;
            this.profile_pic = profile_pic;
            setFile( "profile_pic", profile_pic );
        }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public String getSqlRowExpression() {
            return "("
                    + id + ","
                    + "'" + firstName + "',"
                    + "'" + lastName + "',"
                    + "'" + email + "',"
                    + "'" + password + "',"
                    + MediaGenerator.insertByteHexString( MediaGenerator.getAndDeleteFile( profile_pic ) )
                    + ")";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?, ?, ?, ?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, id ) );
            map.put( 2, new ImmutablePair<>( DataTypes.VARCHAR, firstName ) );
            map.put( 3, new ImmutablePair<>( DataTypes.VARCHAR, lastName ) );
            map.put( 4, new ImmutablePair<>( DataTypes.VARCHAR, email ) );
            map.put( 5, new ImmutablePair<>( DataTypes.VARCHAR, password ) );
            map.put( 6, new ImmutablePair<>( DataTypes.FILE, profile_pic ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }


        @Override
        public String getMongoQl() {
            return null;
        }


        @Override
        public JsonObject getRestRowExpression() {
            JsonObject set = new JsonObject();
            String table = getEntity() + ".";
            set.add( table + "id", new JsonPrimitive( id ) );
            set.add( table + "firstname", new JsonPrimitive( firstName ) );
            set.add( table + "lastname", new JsonPrimitive( lastName ) );
            set.add( table + "email", new JsonPrimitive( email ) );
            set.add( table + "password", new JsonPrimitive( password ) );
            set.add( table + "profile_pic", new JsonPrimitive( "profile_pic" ) );
            return set;

        }


        @Override
        public String getEntity() {
            return "public.users";
        }


        @Override
        public Map<String, String> getRestParameters() {
            return null;
        }

    }

}
