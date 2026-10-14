package org.polypheny.simpleclient.main;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbMongoQlExecutor.PolyphenyDbMongoQlExecutorFactory;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.ycsb.YcsbConfig;
import org.polypheny.simpleclient.scenario.ycsb.YcsbExternal;
import org.polypheny.simpleclient.scenario.ycsb.YcsbRunner;


@Slf4j
public class YcsbExternalScenario {

    public static void schema( String polyphenyDbHost, String ycsbHost ) {
        YcsbConfig config = new YcsbConfig( getProperties(), 1 );
        String adapterName = deployStore( polyphenyDbHost, config );
        System.out.println( "Deployed benchmark adapter: " + adapterName );
        createScenario( polyphenyDbHost, ycsbHost, config, adapterName ).createSchema( null, true );
    }


    private static String deployStore( String host, YcsbConfig config ) {
        PolyphenyDbJdbcExecutor executor = new PolyphenyDbJdbcExecutorFactory( host, false ).createExecutorInstance( null );
        try {
            String name = "postgres".equals( config.storageBackend )
                    ? executor.deployPostgres( true )
                    : executor.deployMongoDb();
            executor.executeCommit();
            return name;
        } catch ( ExecutorException e ) {
            rollback( executor );
            throw new IllegalStateException( "Could not deploy benchmark store: " + config.storageBackend, e );
        } finally {
            close( executor );
        }
    }


    public static void data( String polyphenyDbHost, String ycsbHost, int multiplier ) {
        YcsbConfig config = new YcsbConfig( getProperties(), multiplier );
        createScenario( polyphenyDbHost, ycsbHost, config, null ).generateData( null, null );
    }


    public static void workload( String polyphenyDbHost, String ycsbHost, int multiplier ) {
        YcsbConfig config = new YcsbConfig( getProperties(), multiplier );

        YcsbExternal scenario = createScenario( polyphenyDbHost, ycsbHost, config, null );

        File outputDirectory = new File( "results/ycsb" );

        scenario.execute( null, null, outputDirectory, config.numberOfThreads );

        Properties results = new Properties();
        scenario.analyze( results, outputDirectory );
    }


    public static void cleanup( String polyphenyDbHost, String adapterName ) {
        YcsbConfig config = new YcsbConfig( getProperties(), 1 );
        if ( !adapterName.matches( config.storageBackend + "[0-9]+" ) ) {
            throw new IllegalArgumentException( "Adapter name does not match the benchmark backend: " + adapterName );
        }

        Executor executor = "mongoql".equals( config.accessMode )
                ? createExecutorFactory( polyphenyDbHost, config ).createExecutorInstance( null, YcsbExternal.DOCUMENT_NAMESPACE )
                : createExecutorFactory( polyphenyDbHost, config ).createExecutorInstance();
        try {
            if ( "mongoql".equals( config.accessMode ) ) {
                executor.executeQuery( RawQuery.builder().mongoQl( "db.usertable.drop()" ).expectResultSet( false ).build() );
            } else {
                executor.executeQuery( RawQuery.builder().sql( "DROP TABLE IF EXISTS USERTABLE" ).expectResultSet( false ).build() );
            }
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            if ( "sql".equals( config.accessMode ) ) {
                rollback( executor );
            }
            throw new IllegalStateException( "Could not remove YCSB schema", e );
        } finally {
            close( executor );
        }

        PolyphenyDbJdbcExecutor setupExecutor = new PolyphenyDbJdbcExecutorFactory( polyphenyDbHost, false ).createExecutorInstance( null );
        try {
            setupExecutor.dropStore( adapterName );
            setupExecutor.executeCommit();
            System.out.println( "Removed benchmark adapter: " + adapterName );
        } catch ( ExecutorException e ) {
            rollback( setupExecutor );
            throw new IllegalStateException( "Could not remove benchmark adapter: " + adapterName, e );
        } finally {
            close( setupExecutor );
        }
    }


    private static ExecutorFactory createExecutorFactory( String host, YcsbConfig config ) {
        if ( "mongoql".equals( config.accessMode ) ) {
            return new PolyphenyDbMongoQlExecutorFactory( host );
        }
        return new PolyphenyDbJdbcExecutorFactory( host, false );
    }


    private static YcsbExternal createScenario( String host, String ycsbHost, YcsbConfig config, String adapterName ) {
        YcsbRunner runner = new YcsbRunner( "jdbc:polypheny://" + ycsbHost + "/", "pa", "", config );
        return new YcsbExternal( createExecutorFactory( host, config ), config, runner, adapterName );
    }


    private static void rollback( Executor executor ) {
        try {
            executor.executeRollback();
        } catch ( ExecutorException e ) {
            log.warn( "Could not roll back benchmark setup", e );
        }
    }


    private static void close( Executor executor ) {
        try {
            executor.closeConnection();
        } catch ( ExecutorException e ) {
            log.warn( "Could not close benchmark connection", e );
        }
    }


    private static Properties getProperties() {
        String resource = "/org/polypheny/simpleclient/scenario/external/ycsbexternal.properties";

        Properties properties = new Properties();

        try ( InputStream input =
                YcsbExternalScenario.class.getResourceAsStream( resource ) ) {
            if ( input == null ) {
                throw new IllegalStateException( "Properties file not found: " + resource );
            }

            properties.load( input );
            return properties;
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not read properties file: " + resource, e );
        }
    }
}
