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

package org.polypheny.simpleclient.executor;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import org.polypheny.simpleclient.main.Query;


public class MonetdbExecutor extends Executor {

    public MonetdbExecutor( String host ) {

        try {
            Class.forName( "nl.cwi.monetdb.jdbc.MonetDriver" );
        } catch ( ClassNotFoundException e ) {
            throw new RuntimeException( "Connection failed.", e );
        }

        try {
            connection = DriverManager.getConnection( "jdbc:monetdb://" + host + ":50000/test", "monetdb", "monetdb" );
            connection.setAutoCommit( false );
            executeStatement = connection.createStatement();
        } catch ( SQLException e ) {
            throw new RuntimeException( "Connection failed.", e );
        }

    }


    public void reset() throws SQLException {
        List<String> tables = getListOfTables();
        for ( String table : tables ) {
            executeStatement( new Query( "DROP TABLE \"" + table + "\";", false ) );
        }
    }


    public List<String> getListOfTables() throws SQLException {
        ResultSet resultSet = executeStatement.executeQuery( "select tables.name from tables where tables.system=false;" );
        List<String> list = new LinkedList<>();
        while ( resultSet.next() ) {
            list.add( resultSet.getString( 1 ) );
        }
        return list;
    }


    public static class MonetdbExecutorFactory extends ExecutorFactory {

        private final String host;


        public MonetdbExecutorFactory( String host ) {
            this.host = host;
        }


        @Override
        public Executor createInstance() {
            return new MonetdbExecutor( host );
        }
    }
}
