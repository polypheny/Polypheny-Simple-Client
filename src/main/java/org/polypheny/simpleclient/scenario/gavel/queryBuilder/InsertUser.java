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

package org.polypheny.simpleclient.scenario.gavel.queryBuilder;


import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.person.Person;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertUser extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private static final AtomicInteger nextUserId = new AtomicInteger( 1 );

    private static final Random RANDOM = new Random();
    private static final CharsetDecoder DECODER = StandardCharsets.ISO_8859_1.newDecoder();
    private static final CharsetEncoder ENCODER = StandardCharsets.ISO_8859_1.newEncoder();

    private static final Locale[] locales = {
            new Locale( "de" ),
            new Locale( "en" ),
            new Locale( "es" ),
            new Locale( "fr" ),
            new Locale( "it" ),
            //new Locale( "ka" ),
            //new Locale( "pl" ),
            //new Locale( "sv" ),
            //new Locale( "zh" ),
    };


    static {
        ENCODER.onUnmappableCharacter( CodingErrorAction.IGNORE );
        DECODER.onUnmappableCharacter( CodingErrorAction.IGNORE );
    }


    @Override
    public BatchableInsert getNewQuery() {
        Fairy fairy = Fairy.create( locales[RANDOM.nextInt( locales.length )] );
        Person person = fairy.person();
        return new InsertUserQuery(
                nextUserId.getAndIncrement(),
                person.getEmail(),
                person.getPassword(),
                person.getLastName(),
                person.getFirstName(),
                person.getSex().name().substring( 0, 1 ).toLowerCase(),
                person.getDateOfBirth(),
                person.getAddress().getCity(),
                person.getAddress().getPostalCode(),
                person.getNationality().name()
        );
    }


    private static class InsertUserQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO \"user\"(id, email, password, last_name, first_name, gender, birthday, city, zip_code, country) VALUES ";

        private final int userId;
        private final String email;
        private final String password;
        private final String lastName;
        private final String firstName;
        private final String gender;
        private final LocalDate birthday;
        private final String city;
        private final String zipCode;
        private final String country;


        public InsertUserQuery( int userId, String email, String password, String lastName, String firstName, String gender, LocalDate birthday, String city, String zipCode, String country ) {
            super( EXPECT_RESULT );
            this.userId = userId;
            this.email = email;
            this.password = password;
            this.lastName = lastName;
            this.firstName = firstName;
            this.gender = gender;
            this.birthday = birthday;
            this.city = city;
            this.zipCode = zipCode;
            this.country = country;
        }


        @Override
        public String getSql() {
            return SQL + getSqlRowExpression();
        }


        @Override
        public String getSqlRowExpression() {
            return "("
                    + userId + ","
                    + "'" + escapeAndConvert( email ) + "',"
                    + "'" + escapeAndConvert( password ) + "',"
                    + "'" + escapeAndConvert( lastName ) + "',"
                    + "'" + escapeAndConvert( firstName ) + "',"
                    + "'" + gender + "',"
                    + "date '" + birthday.format( DateTimeFormatter.ofPattern( "yyyy-MM-dd" ) ) + "',"
                    + "'" + escapeAndConvert( city ) + "',"
                    + "'" + zipCode + "',"
                    + "'" + country + "'"
                    + ")";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, userId ) );
            map.put( 2, new ImmutablePair<>( DataTypes.VARCHAR, email ) );
            map.put( 3, new ImmutablePair<>( DataTypes.VARCHAR, password ) );
            map.put( 4, new ImmutablePair<>( DataTypes.VARCHAR, lastName ) );
            map.put( 5, new ImmutablePair<>( DataTypes.VARCHAR, firstName ) );
            map.put( 6, new ImmutablePair<>( DataTypes.VARCHAR, gender ) );
            map.put( 7, new ImmutablePair<>( DataTypes.DATE, Date.valueOf( birthday ) ) );
            map.put( 8, new ImmutablePair<>( DataTypes.VARCHAR, city ) );
            map.put( 9, new ImmutablePair<>( DataTypes.VARCHAR, zipCode ) );
            map.put( 10, new ImmutablePair<>( DataTypes.VARCHAR, country ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return buildRestInsert( "public.user", ImmutableList.of( getRestRowExpression() ) );
        }


        @Override
        public String getMongoQl() {
            return "db.user.insert({\"id\":" + maybeQuote( userId )
                    + ",\"email\":" + maybeQuote( email )
                    + ",\"password\":" + maybeQuote( password )
                    + ",\"last_name\":" + maybeQuote( lastName )
                    + ",\"first_name\":" + maybeQuote( firstName )
                    + ",\"gender\":" + maybeQuote( gender )
                    + ",\"birthday\":" + birthday.toEpochDay()
                    + ",\"city\":" + maybeQuote( city )
                    + ",\"zip_code\":" + maybeQuote( zipCode )
                    + ",\"country\":" + maybeQuote( country ) + "})";
        }


        @Override
        public JsonObject getRestRowExpression() {
            JsonObject row = new JsonObject();
            row.add( "public.user.id", new JsonPrimitive( userId ) );
            row.add( "public.user.email", new JsonPrimitive( escapeAndConvert( email ) ) );
            row.add( "public.user.password", new JsonPrimitive( escapeAndConvert( password ) ) );
            row.add( "public.user.last_name", new JsonPrimitive( escapeAndConvert( lastName ) ) );
            row.add( "public.user.first_name", new JsonPrimitive( escapeAndConvert( firstName ) ) );
            row.add( "public.user.gender", new JsonPrimitive( gender ) );
            row.add( "public.user.birthday", new JsonPrimitive( birthday.format( DateTimeFormatter.ISO_LOCAL_DATE ) ) );
            row.add( "public.user.city", new JsonPrimitive( escapeAndConvert( city ) ) );
            row.add( "public.user.zip_code", new JsonPrimitive( zipCode ) );
            row.add( "public.user.country", new JsonPrimitive( country ) );
            return row;
        }


        @Override
        public String getEntity() {
            return "public.user";
        }


        private String escapeAndConvert( String s ) {
            try {
                synchronized ( DECODER ) {
                    return DECODER.decode( ENCODER.encode( CharBuffer.wrap( StringEscapeUtils.escapeSql( s ) ) ) ).toString();
                }
            } catch ( CharacterCodingException e ) {
                throw new RuntimeException( e );
            }
        }

    }

}
