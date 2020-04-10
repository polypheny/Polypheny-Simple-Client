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


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.main.Query;
import org.polypheny.simpleclient.scenario.gavel.Config;


@Slf4j
public class PolyphenyDbExecutor extends Executor {

    private final Statement executeStatement;
    private final Connection connection;

    //private final ComboPooledDataSource cpds;


    public PolyphenyDbExecutor( String polyphenyHost, Config config ) {

        /*cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass("org.polypheny.jdbc.Driver"); //loads the jdbc driver
            cpds.setJdbcUrl("jdbc:polypheny://localhost/?wire_protocol=PROTO3");
            cpds.setUser("pa");

            cpds.setMinPoolSize(config.numberOfThreads);
            cpds.setMaxPoolSize(config.numberOfThreads);
            cpds.setInitialPoolSize(config.numberOfThreads);

            cpds.setPreferredTestQuery("SELECT 1;");
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }*/

        try {
            Class.forName( "org.polypheny.jdbc.Driver" );
        } catch ( ClassNotFoundException e ) {
            throw new RuntimeException( "Driver not found." );
        }

        try {
            String url = "jdbc:polypheny://" + polyphenyHost + "/?serialization=PROTOBUF";

            Properties props = new Properties();
            props.setProperty( "user", "pa" );

            connection = DriverManager.getConnection( url, props );
            executeStatement = connection.createStatement();
        } catch ( SQLException e ) {
            throw new RuntimeException( "Connection Failed." );
        }
    }


    @Override
    public long executeQuery( Query query ) throws SQLException {
        log.debug( query.sqlQuery );

        long start = System.nanoTime();
        //try ( Connection connection = cpds.getConnection() ) {
        //ResultSet resultSet = connection.createStatement().executeQuery( query.sqlQuery );

        ResultSet resultSet = executeStatement.executeQuery( query.sqlQuery );

        while ( resultSet.next() ) {
            // walk to whole result set
        }
        return System.nanoTime() - start;
        //}
    }


    @Override
    public long executeQueryAndGetNumber( Query query ) throws SQLException {
        log.debug( query.sqlQuery );

        //try ( Connection connection = cpds.getConnection() ) {
        //ResultSet resultSet = connection.createStatement().executeQuery( query.sqlQuery );
        ResultSet resultSet = executeStatement.executeQuery( query.sqlQuery );

        long count;

        if ( resultSet.next() ) {
            count = resultSet.getLong( 1 );
            /*if ( !resultSet.isLast() ) {
                throw new RuntimeException( "Result Set is too big" );
            }*/
        } else {
            throw new RuntimeException( "Result Set has size 0." );
        }

        return count;
        //}

    }


    @Override
    public void executeCommit() throws SQLException {
        connection.commit();
    }


    @Override
    public void executeRollback() throws SQLException {
        connection.rollback();
    }


    @Override
    public void closeConnection() throws SQLException {
        if ( executeStatement != null ) {
            executeStatement.close();
        }
        if ( connection != null ) {
            connection.close();
        }
    }


    @Override
    public long executeStatement( Query statement ) throws SQLException {
        log.info( statement.sqlQuery );

        long start = System.nanoTime();
        //try ( Connection connection = cpds.getConnection() ) {
        //connection.createStatement().executeQuery( statement.sqlQuery );
        executeStatement.execute( statement.sqlQuery );

        return System.nanoTime() - start;
        //}
    }
}
