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

package org.polypheny.simpleclient.scenario.gavel.queryBuilder;


import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.person.Person;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.HttpRequest;
import org.apache.commons.lang.StringEscapeUtils;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertUser extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private static final AtomicInteger nextUserId = new AtomicInteger( 1 );

    private static final Random RANDOM = new Random();
    private static final CharsetDecoder DECODER = Charset.forName( "ISO-8859-1" ).newDecoder();
    private static final CharsetEncoder ENCODER = Charset.forName( "ISO-8859-1" ).newEncoder();

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
    public Query getNewQuery() {
        Fairy fairy = Fairy.create( locales[RANDOM.nextInt( locales.length )] );
        Person person = fairy.person();
        return new InsertUserQuery(
                nextUserId.getAndIncrement(),
                person.getEmail(),
                person.getPassword(),
                person.getLastName(),
                person.getFirstName(),
                person.getSex().name().substring( 0, 1 ).toLowerCase(),
                person.getDateOfBirth().format( DateTimeFormatter.ofPattern( "yyyy-MM-dd" ) ),
                person.getAddress().getCity(),
                person.getAddress().getPostalCode(),
                person.getNationality().name()
        );
    }


    private static class InsertUserQuery extends Query {

        private final int userId;
        private final String email;
        private final String password;
        private final String lastName;
        private final String firstName;
        private final String gender;
        private final String birthday;
        private final String city;
        private final String zipCode;
        private final String country;


        public InsertUserQuery( int userId, String email, String password, String lastName, String firstName, String gender, String birthday, String city, String zipCode, String country ) {
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
            return "INSERT INTO \"user\"(id, email, password, last_name, first_name, gender, birthday, city, zip_code, country) VALUES ("
                    + userId + ","
                    + "'" + escapeAndConvert( email ) + "',"
                    + "'" + escapeAndConvert( password ) + "',"
                    + "'" + escapeAndConvert( lastName ) + "',"
                    + "'" + escapeAndConvert( firstName ) + "',"
                    + "'" + gender + "',"
                    + "date '" + birthday + "',"
                    + "'" + escapeAndConvert( city ) + "',"
                    + "'" + zipCode + "',"
                    + "'" + country + "'"
                    + ")";
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
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
