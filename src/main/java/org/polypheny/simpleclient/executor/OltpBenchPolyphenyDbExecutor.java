package org.polypheny.simpleclient.executor;

import java.io.File;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.control.client.PolyphenyControlConnector;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.AbstractConfig;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchConfig;


@Slf4j
public class OltpBenchPolyphenyDbExecutor extends OltpBenchExecutor implements PolyphenyDbExecutor {

    private final PolyphenyDbJdbcExecutorFactory jdbcExecutorFactory;
    private final String host;


    public OltpBenchPolyphenyDbExecutor( String host ) {
        super();
        this.host = host;
        jdbcExecutorFactory = new PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory( host, false );
    }


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance( null );
            long result = executor.executeQuery( query );
            executor.executeCommit();
            return result;
        } catch ( ExecutorException e ) {
            throw new ExecutorException( "Error while executing query via JDBC", e );
        } finally {
            PolyphenyDbJdbcExecutor.commitAndCloseJdbcExecutor( executor );
        }
    }


    @Override
    protected String getConfigXml( AbstractOltpBenchConfig config ) {
        return config.toXml(
                "POLYPHENY",
                "org.polypheny.jdbc.Driver",
                "jdbc:polypheny://" + host + "/",
                "pa",
                "",
                config.scenario + "-polypheny-ddl.sql",
                "polypheny-dialects.xml",
                "TRANSACTION_SERIALIZABLE"
        );
    }


    @Override
    public void dropStore( String name ) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance( null );
            executor.dropStore( name );
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            throw new ExecutorException( "Error while executing query via JDBC", e );
        } finally {
            PolyphenyDbJdbcExecutor.commitAndCloseJdbcExecutor( executor );
        }
    }


    @Override
    public void deployStore( String name, String clazz, String config ) throws ExecutorException {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance( null );
            executor.deployStore( name, clazz, config );
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            throw new ExecutorException( "Error while executing query via JDBC", e );
        } finally {
            PolyphenyDbJdbcExecutor.commitAndCloseJdbcExecutor( executor );
        }
    }


    @Override
    public void setConfig( String key, String value ) {
        PolyphenyDbJdbcExecutor executor = null;
        try {
            executor = jdbcExecutorFactory.createExecutorInstance( null );
            executor.setConfig( key, value );
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            log.error( "Exception while setting config \"" + key + "\"!", e );
        } finally {
            try {
                PolyphenyDbJdbcExecutor.commitAndCloseJdbcExecutor( executor );
            } catch ( ExecutorException e ) {
                log.error( "Exception while closing JDBC executor", e );
            }
        }
    }


    public static class OltpBenchPolyphenyInstance extends PolyphenyDbInstance {

        public OltpBenchPolyphenyInstance( PolyphenyControlConnector polyphenyControlConnector, ExecutorFactory executorFactory, File outputDirectory, AbstractConfig config ) {
            super( polyphenyControlConnector, executorFactory, outputDirectory, config );
        }

    }


    public static class OltpBenchPolyphenyDbExecutorFactory extends OltpBenchExecutorFactory {

        private final String host;


        public OltpBenchPolyphenyDbExecutorFactory( String host ) {
            this.host = host;
        }

        @Override
        public OltpBenchPolyphenyDbExecutor createExecutorInstance() {
            return new OltpBenchPolyphenyDbExecutor( host );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }

}
