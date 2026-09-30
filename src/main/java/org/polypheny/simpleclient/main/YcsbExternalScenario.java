package org.polypheny.simpleclient.main;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.scenario.ycsb.YcsbConfig;
import org.polypheny.simpleclient.scenario.ycsb.YcsbExternal;
import org.polypheny.simpleclient.scenario.ycsb.YcsbRunner;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbMongoQlExecutor.PolyphenyDbMongoQlExecutorFactory;


public class YcsbExternalScenario {

    public static void schema( String polyphenyDbHost, String ycsbHost ) {
        YcsbConfig config = new YcsbConfig( getProperties(), 1 );

        YcsbExternal scenario = createScenario( polyphenyDbHost, config, ycsbHost );

        scenario.createSchema( null, true );
    }


    public static void data( String polyphenyDbHost, String ycsbHost, int multiplier ) {
        YcsbConfig config = new YcsbConfig( getProperties(), multiplier );
        YcsbExternal scenario = createScenario( polyphenyDbHost, config, ycsbHost );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );

        scenario.generateData( null, progressReporter );
    }


    public static void workload( String polyphenyDbHost, String ycsbHost, int multiplier ) {
        YcsbConfig config = new YcsbConfig( getProperties(), multiplier );
        YcsbExternal scenario = createScenario( polyphenyDbHost, config, ycsbHost );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );

        scenario.execute( progressReporter, null, new File( "results" ), config.numberOfThreads );
    }


    private static YcsbExternal createScenario( String polyphenyDbHost, YcsbConfig config, String ycsbHost ) {
        ExecutorFactory executorFactory;

        if ( "polypheny-jdbc".equals( config.system ) ) {
            executorFactory = new PolyphenyDbJdbcExecutorFactory( polyphenyDbHost, false );
        } else if ( "polypheny-mongoql".equals( config.system ) ) {
            executorFactory = new PolyphenyDbMongoQlExecutorFactory( polyphenyDbHost );
        } else {
            throw new IllegalArgumentException( "Unsupported YCSB system: " + config.system );
        }

        YcsbRunner runner = new YcsbRunner( "jdbc:polypheny://" + ycsbHost + "/", "pa", "", config );

        return new YcsbExternal( executorFactory, config, runner, false, QueryMode.TABLE );
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
