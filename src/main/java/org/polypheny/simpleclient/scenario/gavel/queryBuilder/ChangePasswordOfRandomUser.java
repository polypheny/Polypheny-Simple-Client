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
import java.util.concurrent.ThreadLocalRandom;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class ChangePasswordOfRandomUser extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private final int numberOfUsers;


    public ChangePasswordOfRandomUser( int numberOfUsers ) {
        this.numberOfUsers = numberOfUsers;
    }


    @Override
    public Query getNewQuery() {
        Fairy fairy = Fairy.create();
        Person person = fairy.person();
        return new ChangePasswordOfRandomUserQuery(
                ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 ),
                person.getPassword()
        );
    }


    private static class ChangePasswordOfRandomUserQuery extends Query {

        private final int userId;
        private final String password;


        private ChangePasswordOfRandomUserQuery( int userId, String password ) {
            super( EXPECT_RESULT );
            this.userId = userId;
            this.password = password;
        }


        @Override
        public String getSql() {
            return "UPDATE \"user\" SET \"password\" ="
                    + "'" + password + "' "
                    + "WHERE \"id\" = "
                    + userId;
        }


        @Override
        public String getRest() {
            return null;
        }
    }

}
