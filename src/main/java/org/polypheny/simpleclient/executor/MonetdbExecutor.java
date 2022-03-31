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

package org.polypheny.simpleclient.executor;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.cli.ChronosCommand;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.RawQuery;


@Slf4j
public class MonetdbExecutor extends JdbcExecutor {


    public MonetdbExecutor( String host, CsvWriter csvWriter, boolean prepareStatements ) {
        super( csvWriter, prepareStatements );
        try {
            Class.forName( "nl.cwi.monetdb.jdbc.MonetDriver" );
        } catch ( ClassNotFoundException e ) {
            throw new RuntimeException( "Connection failed.", e );
        }

        try {
            connection = DriverManager.getConnection( "jdbc:monetdb://" + host + ":50000/test", "monetdb", "monetdb" );
            connection.setAutoCommit( false );
            executeStatement = connection.createStatement();
            executeStatement.setFetchSize( 100 );
        } catch ( SQLException e ) {
            throw new RuntimeException( "Connection failed.", e );
        }

    }


    @Override
    public void reset() throws ExecutorException {
        try {
            List<String> tables = getListOfTables();
            for ( String table : tables ) {
                executeQuery( new RawQuery( "DROP TABLE IF EXISTS \"" + table + "\";", null, false ) );
                executeQuery( new RawQuery( "DROP TABLE IF EXISTS \"public\".\"" + table + "\";", null, false ) );
            }
            executeQuery( new RawQuery( "CREATE SCHEMA IF NOT EXISTS \"public\";", null, false ) );
        } catch ( SQLException e ) {
            throw new ExecutorException( e );
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
        private final boolean prepareStatements;


        public MonetdbExecutorFactory( String host, boolean prepareStatements ) {
            this.host = host;
            this.prepareStatements = prepareStatements;
        }


        @Override
        public JdbcExecutor createExecutorInstance( CsvWriter csvWriter ) {
            return new MonetdbExecutor( host, csvWriter, prepareStatements );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 1;
        }

    }


    public static class MonetdbInstance extends DatabaseInstance {

        public MonetdbInstance() {
            reset();
        }


        @Override
        public void tearDown() {
            reset();
        }


        public static void reset() {
            JdbcExecutor monetdbExecutor = new MonetdbExecutor( ChronosCommand.hostname, null, false );
            try {
                monetdbExecutor.reset();
                monetdbExecutor.executeCommit();
            } catch ( ExecutorException e ) {
                throw new RuntimeException( "Exception while dropping tables on monetdb", e );
            } finally {
                try {
                    monetdbExecutor.closeConnection();
                } catch ( ExecutorException e ) {
                    log.error( "Exception while closing connection", e );
                }
            }
        }

    }

}
