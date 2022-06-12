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
import java.sql.SQLException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.RawQuery;


@Slf4j
public class PolyphenyDbJdbcExecutor extends JdbcExecutor implements PolyphenyDbExecutor {


    private PolyphenyDbJdbcExecutor( String polyphenyHost, CsvWriter csvWriter, boolean prepareStatements ) {
        super( csvWriter, prepareStatements );

        try {
            Class.forName( "org.polypheny.jdbc.Driver" );
        } catch ( ClassNotFoundException e ) {
            throw new RuntimeException( "Driver not found.", e );
        }

        try {
            String url = "jdbc:polypheny://" + polyphenyHost + "/?serialization=PROTOBUF";

            Properties props = new Properties();
            props.setProperty( "user", "pa" );

            connection = DriverManager.getConnection( url, props );
            executeStatement = connection.createStatement();
            executeStatement.setFetchSize( 100 );
        } catch ( SQLException e ) {
            throw new RuntimeException( "Connection failed.", e );
        }
    }


    @Override
    public void reset() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public void dropStore( String name ) throws ExecutorException {
        executeQuery( new RawQuery( "ALTER ADAPTERS DROP \"" + name + "\"", null, false ) );
    }


    @Override
    public void deployStore( String name, String clazz, String config ) throws ExecutorException {
        executeQuery( new RawQuery( "ALTER ADAPTERS ADD \"" + name + "\" USING '" + clazz + "' WITH '" + config + "'", null, false ) );
    }


    @Override
    public void setConfig( String key, String value ) {
        try {
            executeQuery( new RawQuery( "ALTER CONFIG '" + key + "' SET '" + value + "'", null, false ) );
        } catch ( ExecutorException e ) {
            log.error( "Exception while setting config \"" + key + "\"!", e );
        }
    }


    public static void commitAndCloseJdbcExecutor( JdbcExecutor executor ) throws ExecutorException {
        if ( executor != null ) {
            try {
                executor.executeCommit();
            } catch ( ExecutorException e ) {
                try {
                    executor.executeRollback();
                } catch ( ExecutorException ex ) {
                    log.error( "Error while rollback connection", e );
                }
            } finally {
                try {
                    executor.closeConnection();
                } catch ( ExecutorException e ) {
                    log.error( "Error while closing connection", e );
                }
            }
        }
    }


    public static class PolyphenyDbJdbcExecutorFactory extends ExecutorFactory {

        private final String host;
        private final boolean prepareStatements;


        public PolyphenyDbJdbcExecutorFactory( String host, boolean prepareStatements ) {
            this.host = host;
            this.prepareStatements = prepareStatements;
        }


        @Override
        public PolyphenyDbJdbcExecutor createExecutorInstance( CsvWriter csvWriter ) {
            return new PolyphenyDbJdbcExecutor( host, csvWriter, prepareStatements );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }

}
