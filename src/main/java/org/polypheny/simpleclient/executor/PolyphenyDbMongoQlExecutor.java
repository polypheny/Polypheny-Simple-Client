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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.UnirestException;
import kong.unirest.json.JSONArray;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.MultipartInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.AbstractConfig;


@Slf4j
public class PolyphenyDbMongoQlExecutor extends PolyphenyDbHttpExecutor {


    public PolyphenyDbMongoQlExecutor( String host, CsvWriter csvWriter ) {
        super( "Mongo", Query::getMongoQl, host, csvWriter );
    }



    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        //query.debug();
        if ( query instanceof MultipartInsert ) {
            long l = executeQuery( new RawQuery( null, ((MultipartInsert) query).buildMultipartInsert(), query.isExpectResultSet() ) );
            ((MultipartInsert) query).cleanup();
            return l;
        }
        long time;

        HttpRequest<?> request = getRequest( query.getMongoQl() );
        log.debug( request.getUrl() );
        try {
            long start = System.nanoTime();
            @SuppressWarnings("rawtypes") HttpResponse result = request.asBytes();
            if ( !result.isSuccess() ) {
                throw new ExecutorException( "Error while executing MongoQl query. Message: " + result.getStatusText() + "  |  URL: " + request.getUrl() );
            }
            time = System.nanoTime() - start;
            if ( csvWriter != null ) {
                csvWriter.appendToCsv( request.getUrl(), time );
            }
        } catch ( UnirestException e ) {
            throw new ExecutorException( e );
        }

        return time;
    }



    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        query.debug();
        if ( query instanceof MultipartInsert ) {
            throw new RuntimeException( "not supported" );
        }

        HttpRequest<?> request = getRequest( query.getMongoQl() );
        log.debug( request.getUrl() );
        try {
            long start = System.nanoTime();
            HttpResponse<JsonNode> result = request.asJson();
            if ( !result.isSuccess() ) {
                throw new ExecutorException( "Error while executing MongoQl query. Message: " + result.getStatusText() + "  |  URL: " + request.getUrl() );
            }
            if ( csvWriter != null ) {
                csvWriter.appendToCsv( request.getUrl(), System.nanoTime() - start );
            }
            // Get result of a count query
            JSONArray res = result.getBody().getObject().getJSONArray( "data" );
            if ( res.length() != 1 ) {
                throw new ExecutorException( "Invalid result: " + res.toString() );
            }

            return res.getJSONArray( 0 ).getLong( 0 );
        } catch ( UnirestException e ) {
            throw new ExecutorException( e );
        }
    }


    @Override
    public void executeCommit() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeRollback() throws ExecutorException {
        log.error( "Unsupported operation: Rollback" );
    }


    @Override
    public void closeConnection() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeInsertList( List<BatchableInsert> batchList, AbstractConfig config ) throws ExecutorException {
        String currentTable = null;
        List<String> rows = new ArrayList<>();
        for ( BatchableInsert query : batchList ) {
            query.debug();
            if ( query instanceof MultipartInsert ) {
                continue;
            }
            if ( currentTable == null ) {
                currentTable = query.getTable();
            }

            if ( currentTable.equals( query.getTable() ) ) {
                rows.add( Objects.requireNonNull( query.getMongoQlRowExpression() ) );
            } else {
                throw new RuntimeException( "Different tables in multi-inserts. This should not happen!" );
            }
        }
        if ( rows.size() > 0 ) {
            executeQuery( new RawQuery( null, null, Query.buildMongoQlManyInsert( currentTable, rows ), null, false ) );
        }
    }



    public static class PolyphenyDbMongoQlExecutorFactory extends ExecutorFactory {

        private final String host;


        public PolyphenyDbMongoQlExecutorFactory( String host ) {
            this.host = host;
        }


        @Override
        public PolyphenyDbMongoQlExecutor createExecutorInstance( CsvWriter csvWriter ) {
            return new PolyphenyDbMongoQlExecutor( host, csvWriter );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }

}
