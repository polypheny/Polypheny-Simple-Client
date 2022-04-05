package org.polypheny.simpleclient.executor;

import static org.polypheny.simpleclient.executor.PolyphenyDbRestExecutor.commitAndCloseJdbcExecutor;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.Locale;
import java.util.function.Function;
import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.UnirestException;
import kong.unirest.json.JSONArray;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.MultipartInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;

@Slf4j
public abstract class PolyphenyDbHttpExecutor implements PolyphenyDbExecutor {

    @Getter
    public final String name;

    @Getter
    public final Function<Query, String> queryAccessor;

    protected final PolyphenyDbJdbcExecutorFactory jdbcExecutorFactory;
    protected final CsvWriter csvWriter;



    public PolyphenyDbHttpExecutor( String name, Function<Query, String> queryAccessor, String host, CsvWriter csvWriter ) {
        this.name = name;
        this.queryAccessor = queryAccessor;
        this.jdbcExecutorFactory = new PolyphenyDbJdbcExecutorFactory( host, false );
        this.csvWriter = csvWriter;
    }


    @Override
    public void reset() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
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


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        if ( query instanceof MultipartInsert ) {
            long l = executeQuery( new RawQuery( null, ((MultipartInsert) query).buildMultipartInsert(), query.isExpectResultSet() ) );
            ((MultipartInsert) query).cleanup();
            return l;
        }
        long time;

        HttpRequest<?> request = getRequest( queryAccessor.apply( query ) );
        try {
            long start = System.nanoTime();
            @SuppressWarnings("rawtypes") HttpResponse result = request.asBytes();
            if ( !result.isSuccess() ) {
                throw new ExecutorException( "Error while executing " + name +  " query. Message: " + result.getStatusText() + "  |  URL: " + request.getUrl() );
            }
            time = System.nanoTime() - start;
            if ( csvWriter != null ) {
                csvWriter.appendToCsv( queryAccessor.apply( query ), time );
            }
        } catch ( UnirestException e ) {
            throw new ExecutorException( e );
        }

        return time;
    }


    HttpRequest<?> buildQuery( String mql ) {
        JsonObject data = new JsonObject();
        data.addProperty( "query", mql );
        data.addProperty( "database", "test" );

        return Unirest.post( "{protocol}://{host}:{port}/"+name.toLowerCase( Locale.ROOT ) )
                .header( "Content-Type", "application/json" )
                .body( data );

    }


    HttpRequest<?> getRequest( String query ) {
        HttpRequest<?> request = buildQuery( query );
        request.basicAuth( "pa", "" );
        request.routeParam( "protocol", "http" );
        request.routeParam( "host", "127.0.0.1" );
        request.routeParam( "port", "13137" );
        return request;
    }


    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        query.debug();
        if ( query instanceof MultipartInsert ) {
            throw new RuntimeException( "not supported" );
        }

        HttpRequest<?> request = getRequest( queryAccessor.apply( query ) );

        try {
            long start = System.nanoTime();
            HttpResponse<JsonNode> result = request.asJson();
            if ( !result.isSuccess() ) {
                throw new ExecutorException( "Error while executing " + name + " query. Message: " + result.getStatusText() + "  |  URL: " + request.getUrl() );
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

}
