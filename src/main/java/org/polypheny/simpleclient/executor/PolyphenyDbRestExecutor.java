package org.polypheny.simpleclient.executor;

import java.io.IOException;
import java.util.List;
import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.UnirestException;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.Query;


@Slf4j
public class PolyphenyDbRestExecutor implements PolyphenyDbExecutor {

    private final PolyphenyDbJdbcExecutorFactory jdbcExecutorFactory;
    private final String host;

    private final CsvWriter csvWriter;


    public PolyphenyDbRestExecutor( String host, CsvWriter csvWriter ) {
        super();
        this.host = host;
        this.csvWriter = csvWriter;
        jdbcExecutorFactory = new PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory( host );
    }


    @Override
    public void reset() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        long time;
        if ( query.getRest() != null ) {
            HttpRequest<?> request = query.getRest();
            request.basicAuth( "pa", "" );
            request.routeParam( "protocol", "http" );
            request.routeParam( "host", host );
            request.routeParam( "port", "8089" );
            log.debug( request.getUrl() );
            try {
                long start = System.nanoTime();
                HttpResponse<JsonNode> result = request.asJson();
                if ( !result.isSuccess() ) {
                    log.error( "Err!" );
                }
                time = System.nanoTime() - start;
                if ( csvWriter != null ) {
                    csvWriter.appendToCsv( request.getUrl(), time );
                }
            } catch ( UnirestException e ) {
                throw new ExecutorException( e );
            }
        } else {
            // There is no REST expression available for this query. Executing SQL expression via JDBC.
            log.warn( query.getSql() );
            JdbcExecutor executor = null;
            try {
                executor = jdbcExecutorFactory.createInstance( csvWriter );
                time = executor.executeQuery( query );
                if ( csvWriter != null ) {
                    csvWriter.appendToCsv( query.getSql(), time );
                }
            } catch ( ExecutorException e ) {
                throw new ExecutorException( "Error while executing query via JDBC", e );
            } finally {
                commitAndCloseJdbcExecutor( executor );
            }
        }

        return time;
    }


    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        if ( query.getRest() != null ) {
            // TODO: get result of a count query
            throw new RuntimeException();
        } else {
            // There is no REST expression available for this query. Executing SQL expression via JDBC.
            log.warn( query.getSql() );
            JdbcExecutor executor = null;
            try {
                executor = jdbcExecutorFactory.createInstance( csvWriter );
                return executor.executeQueryAndGetNumber( query );
            } catch ( ExecutorException e ) {
                throw new ExecutorException( "Error while executing query via JDBC", e );
            } finally {
                commitAndCloseJdbcExecutor( executor );
            }
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
    public void executeInsertList( List<Query> batchList ) throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public void dropStore( String name ) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createInstance( csvWriter );
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
            executor = jdbcExecutorFactory.createInstance( csvWriter );
            executor.deployStore( name, clazz, config );
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            throw new ExecutorException( "Error while executing query via JDBC", e );
        } finally {
            commitAndCloseJdbcExecutor( executor );
        }
    }


    @Override
    public void setConfig( String key, String value ) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createInstance( csvWriter );
            executor.setConfig( key, value );
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            throw new ExecutorException( "Error while executing query via JDBC", e );
        } finally {
            commitAndCloseJdbcExecutor( executor );
        }
    }


    private void commitAndCloseJdbcExecutor( JdbcExecutor executor ) throws ExecutorException {
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


    public static class PolyphenyDbRestExecutorFactory extends ExecutorFactory {

        private final String host;


        public PolyphenyDbRestExecutorFactory( String host ) {
            this.host = host;
        }


        @Override
        public PolyphenyDbRestExecutor createInstance( CsvWriter csvWriter ) {
            return new PolyphenyDbRestExecutor( host, csvWriter );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }
    }
}
