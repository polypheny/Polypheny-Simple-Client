package org.polypheny.simpleclient.executor;

import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.UnirestException;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.query.Query;


@Slf4j
public class PolyphenyDbRestExecutor implements PolyphenyDbExecutor {

    private final PolyphenyDbJdbcExecutorFactory jdbcExecutorFactory;
    private final String host;


    public PolyphenyDbRestExecutor( String host ) {
        super();
        this.host = host;
        jdbcExecutorFactory = new PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory( host );
    }


    @Override
    public void reset() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
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
                return System.nanoTime() - start;
            } catch ( UnirestException e ) {
                throw new ExecutorException( e );
            }
        } else {
            // There is no REST expression available for this query. Executing SQL expression via JDBC.
            log.warn( query.getSql() );
            JdbcExecutor executor = null;
            try {
                executor = jdbcExecutorFactory.createInstance();
                return executor.executeQuery( query );
            } catch ( ExecutorException e ) {
                throw new ExecutorException( "Error while executing query via JDBC", e );
            } finally {
                commitAndCloseJdbcExecutor( executor );
            }
        }
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
                executor = jdbcExecutorFactory.createInstance();
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
    public void dropStore( String name ) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createInstance();
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
            executor = jdbcExecutorFactory.createInstance();
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
            executor = jdbcExecutorFactory.createInstance();
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


    public static class PolyphenyDbRestExecutorFactory extends ExecutorFactory {

        private final String host;


        public PolyphenyDbRestExecutorFactory( String host ) {
            this.host = host;
        }


        @Override
        public PolyphenyDbRestExecutor createInstance() {
            return new PolyphenyDbRestExecutor( host );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }
    }
}
