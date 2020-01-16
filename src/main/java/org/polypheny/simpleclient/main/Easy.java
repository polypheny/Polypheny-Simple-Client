package org.polypheny.simpleclient.main;


import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.scenario.gavel.Config;
import org.polypheny.simpleclient.scenario.gavel.Gavel;


@Slf4j
public class Easy {

    public static void data( String polyphenyDbUrl, int multiplier ) {
        try {
            Properties props = new Properties();
            props.load( ClassLoader.getSystemResourceAsStream( "gavel/easy.properties" ) );
            Config config = new Config( props, multiplier );
            Gavel gavel = new Gavel( polyphenyDbUrl, config );

            ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
            gavel.buildDatabase( progressReporter );
        } catch ( IOException | SQLException e ) {
            log.warn( "Exception while executing workload" );
        }
    }


    public static void workload( String polyphenyDbUrl, int multiplier ) {
        try {
            Properties props = new Properties();
            props.load( ClassLoader.getSystemResourceAsStream( "gavel/easy.properties" ) );
            Config config = new Config( props, multiplier );
            Gavel gavel = new Gavel( polyphenyDbUrl, config );

            CsvWriter csvWriter = new CsvWriter( "results.csv" );
            ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
            long runtime = gavel.execute( progressReporter, csvWriter, null, new PolyphenyDbExecutor( polyphenyDbUrl, config ), false );
        } catch ( IOException | SQLException e ) {
            log.warn( "Exception while executing workload" );
        }
    }


    public static void schema( String polyphenyDbUrl ) {
        try {
            Properties props = new Properties();
            props.load( ClassLoader.getSystemResourceAsStream( "gavel/easy.properties" ) );
            Config config = new Config( props, 1 );
            Gavel gavel = new Gavel( polyphenyDbUrl, config );

            gavel.createSchema();
        } catch ( IOException | SQLException e ) {
            log.warn( "Exception while executing workload" );
        }
    }

}