package org.polypheny.simpleclient.main;


import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.scenario.knnbench.KnnBench;
import org.polypheny.simpleclient.scenario.knnbench.KnnBenchConfig;


@Slf4j
public class Knn {

    public static void schema( ExecutorFactory executorFactory, boolean commitAfterEveryQuery ) {
        KnnBenchConfig config = new KnnBenchConfig( getProperties(), 1 );
        KnnBench knnBench = new KnnBench( executorFactory, config, commitAfterEveryQuery, false );
        knnBench.createSchema( true );
    }


    public static void data( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery ) {
        KnnBenchConfig config = new KnnBenchConfig( getProperties(), multiplier );
        KnnBench knnBench = new KnnBench( executorFactory, config, commitAfterEveryQuery, false );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        knnBench.generateData( progressReporter );
    }


    public static void workload( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery, boolean writeCsv, boolean dumpQueryList ) {
        KnnBenchConfig config = new KnnBenchConfig( getProperties(), multiplier );
        KnnBench knnBench = new KnnBench( executorFactory, config, commitAfterEveryQuery, dumpQueryList );

        final CsvWriter csvWriter;
        if ( writeCsv ) {
            csvWriter = new CsvWriter( "results.csv" );
        } else {
            csvWriter = null;
        }

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        knnBench.execute( progressReporter, csvWriter, new File( "." ), config.numberOfThreads );
    }


    public static void warmup( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        KnnBenchConfig config = new KnnBenchConfig( getProperties(), multiplier );
        KnnBench knnBench = new KnnBench( executorFactory, config, commitAfterEveryQuery, dumpQueryList );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        knnBench.warmUp( progressReporter, multiplier );
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/knnbench/knn.properties" ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }

}
