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


import java.util.concurrent.ThreadLocalRandom;
import org.joda.time.DateTime;
import org.polypheny.simpleclient.main.QueryBuilder;


public class InsertRandomBid extends QueryBuilder {

    private final int numberOfAuctions;
    private final int numberOfUsers;

    private static int nextId = 1;


    public InsertRandomBid( int numberOfAuctions, int numberOfUsers ) {
        super( false );
        this.numberOfAuctions = numberOfAuctions;
        this.numberOfUsers = numberOfUsers;
    }


    @Override
    public String generateSql() {
        StringBuilder sb = new StringBuilder();
        sb.append( "INSERT INTO bid(id, amount, \"timestamp\", \"user\", auction) VALUES (" );
        sb.append( nextId++ ).append( "," );
        sb.append( ThreadLocalRandom.current().nextInt( 1, 1000 ) ).append( "," );
        sb.append( "timestamp '" ).append( new DateTime().toString( "yyyy-MM-dd HH:mm:ss" ) ).append( "'," );
        sb.append( ThreadLocalRandom.current().nextInt( 1, numberOfUsers + 1 ) ).append( "," );
        sb.append( ThreadLocalRandom.current().nextInt( 1, numberOfAuctions + 1 ) );
        sb.append( ")" );
        return sb.toString();
    }


    public static void setNextId( int nextId ) {
        InsertRandomBid.nextId = nextId;
    }
}
