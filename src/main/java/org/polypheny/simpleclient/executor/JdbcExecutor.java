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
 */

package org.polypheny.simpleclient.executor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.Query.DataTypes;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.AbstractConfig;


@Slf4j
public abstract class JdbcExecutor implements Executor {

    protected Connection connection;
    protected Statement executeStatement;

    protected final CsvWriter csvWriter;

    protected final boolean prepareStatements;
    protected final Map<String, PreparedStatement> preparedStatements = new HashMap<>();


    public JdbcExecutor( CsvWriter csvWriter, boolean prepareStatements ) {
        this.csvWriter = csvWriter;
        this.prepareStatements = prepareStatements;
    }


    @Override
    public abstract void reset() throws ExecutorException;


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        try {
            ArrayList<File> files = new ArrayList<>();
            long start = System.nanoTime();

            if ( prepareStatements && query.getParameterizedSqlQuery() != null ) {
                if ( !preparedStatements.containsKey( query.getClass().getCanonicalName() ) ) {
                    PreparedStatement statement = connection.prepareStatement( query.getParameterizedSqlQuery() );
                    preparedStatements.put( query.getClass().getCanonicalName(), statement );
                }

                PreparedStatement preparedStatement = preparedStatements.get( query.getClass().getCanonicalName() );
                Map<Integer, ImmutablePair<DataTypes, Object>> values = query.getParameterValues();
                for ( Entry<Integer, ImmutablePair<DataTypes, Object>> entry : values.entrySet() ) {
                    switch ( entry.getValue().left ) {
                        case INTEGER:
                            preparedStatement.setInt( entry.getKey(), (Integer) entry.getValue().right );
                            break;
                        case VARCHAR:
                            preparedStatement.setString( entry.getKey(), (String) entry.getValue().right );
                            break;
                        case TIMESTAMP:
                            preparedStatement.setTimestamp( entry.getKey(), (Timestamp) entry.getValue().right );
                            break;
                        case DATE:
                            preparedStatement.setDate( entry.getKey(), (Date) entry.getValue().right );
                            break;
                        case ARRAY_INT:
                            preparedStatement.setArray( entry.getKey(), connection.createArrayOf( "INTEGER", (Object[]) entry.getValue().right ) );
                            break;
                        case ARRAY_REAL:
                            preparedStatement.setArray( entry.getKey(), connection.createArrayOf( "REAL", (Object[]) entry.getValue().right ) );
                            break;
                        case BYTE_ARRAY:
                            preparedStatement.setBytes( entry.getKey(), (byte[]) entry.getValue().right );
                            break;
                        case FILE:
                            File f = (File) entry.getValue().right;
                            files.add( f );
                            preparedStatement.setBinaryStream( entry.getKey(), new FileInputStream( f ) );
                            break;
                    }
                }
                if ( query.isExpectResultSet() ) {
                    ResultSet resultSet = preparedStatement.executeQuery();
                    List<String> result = new ArrayList<>();
                    while ( resultSet.next() ) {
                        // walk through the whole result set
                        result.add( resultSet.toString() );
                    }
                    log.debug( "Number of result rows: " + result.size() );
                } else {
                    preparedStatement.execute();
                }
            } else {
                if ( query.isExpectResultSet() ) {
                    ResultSet resultSet = executeStatement.executeQuery( query.getSql() );
                    List<String> result = new ArrayList<>();
                    while ( resultSet.next() ) {
                        // walk through the whole result set
                        result.add( resultSet.toString() );
                    }
                    log.debug( "Number of result rows: " + result.size() );
                } else {
                    executeStatement.execute( query.getSql() );
                }
            }

            long time = System.nanoTime() - start;
            files.forEach( File::delete );
            if ( csvWriter != null ) {
                csvWriter.appendToCsv( query.getSql(), time );
            }
            return time;
        } catch ( SQLException | FileNotFoundException e ) {
            log.error( "Error while executing: " + query.getSql() );
            throw new ExecutorException( e );
        }
    }


    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        try {
            //log.debug( query.getSql() );

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
            for ( PreparedStatement preparedStatement : preparedStatements.values() ) {
                preparedStatement.close();
            }
            if ( connection != null ) {
                connection.close();
            }
        } catch ( SQLException e ) {
            throw new ExecutorException( e );
        }
    }


    @Override
    public void executeInsertList( List<BatchableInsert> queryList, AbstractConfig config ) throws ExecutorException {
        if ( queryList.size() > 0 ) {
            if ( config.usePreparedBatchForDataInsertion() ) {
                executeInsertListAsPreparedBatch( queryList );
            } else {
                executeInsertListAsMultiInsert( queryList );
            }
        }
    }


    protected void executeInsertListAsPreparedBatch( List<BatchableInsert> queryList ) throws ExecutorException {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement( queryList.get( 0 ).getParameterizedSqlQuery() );
            ArrayList<File> files = new ArrayList<>();
            for ( BatchableInsert insert : queryList ) {
                Map<Integer, ImmutablePair<DataTypes, Object>> data = insert.getParameterValues();
                for ( Map.Entry<Integer, ImmutablePair<DataTypes, Object>> entry : data.entrySet() ) {
                    switch ( entry.getValue().left ) {
                        case INTEGER:
                            preparedStatement.setInt( entry.getKey(), (Integer) entry.getValue().right );
                            break;
                        case VARCHAR:
                            preparedStatement.setString( entry.getKey(), (String) entry.getValue().right );
                            break;
                        case TIMESTAMP:
                            preparedStatement.setTimestamp( entry.getKey(), (Timestamp) entry.getValue().right );
                            break;
                        case DATE:
                            preparedStatement.setDate( entry.getKey(), (Date) entry.getValue().right );
                            break;
                        case ARRAY_INT:
                            preparedStatement.setArray( entry.getKey(), connection.createArrayOf( "INTEGER", (Object[]) entry.getValue().right ) );
                            break;
                        case ARRAY_REAL:
                            preparedStatement.setArray( entry.getKey(), connection.createArrayOf( "REAL", (Object[]) entry.getValue().right ) );
                            break;
                        case BYTE_ARRAY:
                            preparedStatement.setBytes( entry.getKey(), (byte[]) entry.getValue().right );
                            break;
                        case FILE:
                            File f = (File) entry.getValue().right;
                            byte[] b = Files.readAllBytes( f.toPath() );
                            preparedStatement.setBytes( entry.getKey(), b );
                            break;
                    }
                }
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
            files.forEach( File::delete );
        } catch ( SQLException | FileNotFoundException e ) {
            throw new ExecutorException( e );
        } catch ( IOException e ) {
            throw new ExecutorException( e );
        }
    }


    protected void executeInsertListAsMultiInsert( List<BatchableInsert> queryList ) throws ExecutorException {
        StringBuilder stringBuilder = new StringBuilder();
        boolean first = true;
        for ( BatchableInsert query : queryList ) {
            if ( first ) {
                stringBuilder.append( query.getSql() );
                first = false;
            } else {
                String rowExpression = Objects.requireNonNull( query.getSqlRowExpression() );
                stringBuilder.append( "," ).append( rowExpression );
            }
        }
        executeQuery( RawQuery.builder()
                .sql( stringBuilder.toString() )
                .expectResultSet( false )
                .build() );
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
