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


import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;


@Slf4j
public abstract class JdbcExecutor implements Executor {

    protected Connection connection;
    protected Statement executeStatement;

    protected final CsvWriter csvWriter;


    public JdbcExecutor( CsvWriter csvWriter ) {
        this.csvWriter = csvWriter;
    }


    @Override
    public abstract void reset() throws ExecutorException;


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        try {
            log.debug( query.getSql() );

            long start = System.nanoTime();

            if ( query.isExpectResultSet() ) {
                ResultSet resultSet = executeStatement.executeQuery( query.getSql() );
                while ( resultSet.next() ) {
                    // walk through the whole result set
                }
            } else {
                executeStatement.execute( query.getSql() );
            }

            long time = System.nanoTime() - start;
            if ( csvWriter != null ) {
                csvWriter.appendToCsv( query.getSql(), time );
            }
            return time;
        } catch ( SQLException e ) {
            throw new ExecutorException( e );
        }
    }


    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        try {
            log.debug( query.getSql() );

            ResultSet resultSet = executeStatement.executeQuery( query.getSql() );

            long count;

            if ( resultSet.next() ) {
                count = resultSet.getLong( 1 );
            } else {
                throw new RuntimeException( "ResultSet has size 0." );
            }

            return count;
        } catch ( SQLException e ) {
            throw new ExecutorException( e );
        }
    }


    @Override
    public void executeCommit() throws ExecutorException {
        try {
            connection.commit();
        } catch ( SQLException e ) {
            throw new ExecutorException( e );
        }
    }


    @Override
    public void executeRollback() throws ExecutorException {
        try {
            connection.rollback();
        } catch ( SQLException e ) {
            throw new ExecutorException( e );
        }
    }


    @Override
    public void closeConnection() throws ExecutorException {
        try {
            if ( executeStatement != null ) {
                executeStatement.close();
            }
            if ( connection != null ) {
                connection.close();
            }
        } catch ( SQLException e ) {
            throw new ExecutorException( e );
        }
    }


    @Override
    public void executeInsertList( List<Query> queryList ) throws ExecutorException {
        if ( queryList.size() > 0 ) {
            StringBuilder stringBuilder = new StringBuilder();
            boolean first = true;
            for ( Query query : queryList ) {
                if ( !query.getSql().startsWith( "INSERT" ) ) {
                    throw new RuntimeException( "Not a insert statement: " + query.getSql() );
                }
                if ( first ) {
                    stringBuilder.append( query.getSql() );
                    first = false;
                } else {
                    String statement = query.getSql();
                    String[] split = statement.split( "VALUES" );
                    stringBuilder.append( "," ).append( split[split.length - 1] );
                }
            }
            executeQuery( new RawQuery( stringBuilder.toString(), null, false ) );
        }
    }


    @Override
    public void flushCsvWriter() {
        if ( csvWriter != null ) {
            try {
                csvWriter.flush();
            } catch ( IOException e ) {
                log.warn( "Exception while flushing csv writer", e );
            }
        }
    }

}