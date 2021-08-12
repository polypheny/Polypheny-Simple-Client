package org.polypheny.simpleclient.main;


import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.scenario.gavel.Gavel;
import org.polypheny.simpleclient.scenario.gavel.GavelConfig;


@Slf4j
public class Easy {

    public static void schema( ExecutorFactory executorFactory, boolean commitAfterEveryQuery, QueryMode queryMode ) {
        GavelConfig config = new GavelConfig( getProperties(), 1 );
        Gavel gavel = new Gavel( executorFactory, config, commitAfterEveryQuery, false, queryMode );
        gavel.createSchema( true );
    }


    public static void data( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery, QueryMode queryMode ) {
        GavelConfig config = new GavelConfig( getProperties(), multiplier );
        Gavel gavel = new Gavel( executorFactory, config, commitAfterEveryQuery, false, queryMode );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        gavel.generateData( progressReporter );
    }


    public static void workload( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery, boolean writeCsv, boolean dumpQueryList, QueryMode queryMode ) {
        GavelConfig config = new GavelConfig( getProperties(), multiplier );
        Gavel gavel = new Gavel( executorFactory, config, commitAfterEveryQuery, dumpQueryList, queryMode );

        final CsvWriter csvWriter;
        if ( writeCsv ) {
            csvWriter = new CsvWriter( "results.csv" );
        } else {
            csvWriter = null;
        }
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        gavel.execute( progressReporter, csvWriter, new File( "." ), config.numberOfThreads );
    }


    public static void warmup( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        GavelConfig config = new GavelConfig( getProperties(), 1 );
        Gavel gavel = new Gavel( executorFactory, config, commitAfterEveryQuery, dumpQueryList, queryMode );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        gavel.warmUp( progressReporter, multiplier );
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavel/easy.properties" ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }

}
