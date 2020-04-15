package org.polypheny.simpleclient.main;


import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.cli.Main;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor.PolyphenyDBExecutorFactory;
import org.polypheny.simpleclient.scenario.gavel.Config;
import org.polypheny.simpleclient.scenario.gavel.Gavel;


@Slf4j
public class Easy {

    public static void schema( String polyphenyDbUrl ) {
        Config config = new Config( getProperties(), 1 );
        Gavel gavel = new Gavel( new PolyphenyDBExecutorFactory( polyphenyDbUrl ), config );
        gavel.createSchema( true );
    }


    public static void data( String polyphenyDbUrl, int multiplier ) {
        Config config = new Config( getProperties(), multiplier );
        Gavel gavel = new Gavel( new PolyphenyDBExecutorFactory( polyphenyDbUrl ), config );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        gavel.generateData( progressReporter );
    }


    public static void workload( String polyphenyDbUrl, int multiplier ) {
        Config config = new Config( getProperties(), multiplier );
        Gavel gavel = new Gavel( new PolyphenyDBExecutorFactory( polyphenyDbUrl ), config );

        final CsvWriter csvWriter;
        if ( Main.WRITE_CSV ) {
            csvWriter = new CsvWriter( "results.csv" );
        } else {
            csvWriter = null;
        }
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        gavel.execute( progressReporter, csvWriter, null );
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "gavel/easy.properties" ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }

}
