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
import java.sql.SQLException;
import java.sql.Statement;
import org.polypheny.simpleclient.main.Query;


public class PostgresExecutor extends Executor {

    private final Connection connection;
    private final Statement executeStatement;


    public PostgresExecutor( String host ) {

        try {
            Class.forName( "org.postgresql.Driver" );
        } catch ( ClassNotFoundException e ) {
            throw new RuntimeException( "Driver not found." );
        }

        try {
            //TODO pass as parameter
            connection = DriverManager.getConnection( "jdbc:postgresql://" + host + ":5432/test", "postgres", "12345" );
            connection.setAutoCommit( false );
            executeStatement = connection.createStatement();
        } catch ( SQLException e ) {
            throw new RuntimeException( "Connection Failed." );

        }

    }


    @Override
    public long executeQuery( Query query ) throws SQLException {
        executeStatement.executeQuery( query.sqlQuery );
        //TODO
        return -1;
    }


    @Override
    public long executeStatement( Query statement ) throws SQLException {
        executeStatement.execute( statement.sqlQuery );
        //TODO
        return -1;
    }


    @Override
    public long executeQueryAndGetNumber( Query query ) throws SQLException {
        //TODO
        return -1;
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
}
