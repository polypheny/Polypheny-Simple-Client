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
import org.polypheny.simpleclient.main.QueryBuilder;


public class ChangePasswordOfRandomUser extends QueryBuilder {

    private final int numberOfUsers;


    public ChangePasswordOfRandomUser( int numberOfUsers ) {
        super( false );
        this.numberOfUsers = numberOfUsers;
    }


    @Override
    public String generateSql() {
        Fairy fairy = Fairy.create();
        Person person = fairy.person();
        return "UPDATE \"user\" SET \"password\" ="
                + "'" + person.getPassword() + "' "
                + "WHERE \"id\" = "
                + ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 );
    }

}
