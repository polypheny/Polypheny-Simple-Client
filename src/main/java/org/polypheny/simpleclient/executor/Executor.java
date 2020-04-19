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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.main.Query;


@Slf4j
public abstract class Executor {

    protected Connection connection;
    protected Statement executeStatement;

    public abstract void reset() throws SQLException;


    public long executeQuery( Query query ) throws SQLException {
        log.debug( query.sqlQuery );

        long start = System.nanoTime();

        ResultSet resultSet = executeStatement.executeQuery( query.sqlQuery );
        while ( resultSet.next() ) {
            // walk to whole result set
        }

        return System.nanoTime() - start;
    }


    public long executeQueryAndGetNumber( Query query ) throws SQLException {
        log.debug( query.sqlQuery );

        ResultSet resultSet = executeStatement.executeQuery( query.sqlQuery );

        long count;

        if ( resultSet.next() ) {
            count = resultSet.getLong( 1 );
        } else {
            throw new RuntimeException( "Result Set has size 0." );
        }

        return count;
    }


    public void executeCommit() throws SQLException {
        connection.commit();
    }


    public void executeRollback() throws SQLException {
        connection.rollback();
    }


    public void closeConnection() throws SQLException {
        if ( executeStatement != null ) {
            executeStatement.close();
        }
        if ( connection != null ) {
            connection.close();
        }
    }


    public long executeStatement( Query statement ) throws SQLException {
        log.info( statement.sqlQuery );

        long start = System.nanoTime();
        executeStatement.execute( statement.sqlQuery );
        return System.nanoTime() - start;
    }


    public abstract static class ExecutorFactory {

        public abstract Executor createInstance();

        // Allows to limit number of concurrent executor threads, 0 means no limit
        public abstract int getMaxNumberOfThreads();
    }

}
