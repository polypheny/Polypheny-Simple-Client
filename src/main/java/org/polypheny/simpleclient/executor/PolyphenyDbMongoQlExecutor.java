package org.polypheny.simpleclient.executor;

import static org.polypheny.simpleclient.executor.PolyphenyDbRestExecutor.commitAndCloseJdbcExecutor;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.UnirestException;
import kong.unirest.json.JSONArray;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.MultipartInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.AbstractConfig;

@Slf4j
public class PolyphenyDbMongoQlExecutor implements PolyphenyDbExecutor {

    private final PolyphenyDbJdbcExecutorFactory jdbcExecutorFactory;

    private final CsvWriter csvWriter;


    public PolyphenyDbMongoQlExecutor( String host, CsvWriter csvWriter ) {
        this.csvWriter = csvWriter;
        this.jdbcExecutorFactory = new PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory( host, false );
    }


    @Override
    public void reset() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
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


    private HttpRequest<?> buildQuery( String mql ) {
        JsonObject data = new JsonObject();
        data.addProperty( "query", mql );
        data.addProperty( "database", "test" );

        return Unirest.post( "{protocol}://{host}:{port}/mongo" )
                .header( "Content-Type", "application/json" )
                .body( data );

    }


    private HttpRequest<?> getRequest( String mql ) {
        HttpRequest<?> request = buildQuery( mql );
        request.basicAuth( "pa", "" );
        request.routeParam( "protocol", "http" );
        request.routeParam( "host", "127.0.0.1" );
        request.routeParam( "port", "2717" );
        return request;
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
            executeQuery( new RawQuery( null, null, Query.buildMongoQlManyInsert( currentTable, rows ), false ) );
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


    @Override
    public void dropStore( String name ) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance( csvWriter );
            executor.dropStore( name );
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            throw new ExecutorException( "Error while executing query via JDBC", e );
        } finally {
            commitAndCloseJdbcExecutor( executor );
        }
    }


    @Override
    public void deployStore( String name, String clazz, String config ) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance( csvWriter );
            executor.deployStore( name, clazz, config );
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            throw new ExecutorException( "Error while executing query via JDBC", e );
        } finally {
            commitAndCloseJdbcExecutor( executor );
        }
    }


    @Override
    public void setConfig( String key, String value ) {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance( csvWriter );
            executor.setConfig( key, value );
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            log.error( "Exception while setting config \"" + key + "\"!", e );
        } finally {
            try {
                commitAndCloseJdbcExecutor( executor );
            } catch ( ExecutorException e ) {
                log.error( "Exception while closing JDBC executor", e );
            }
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