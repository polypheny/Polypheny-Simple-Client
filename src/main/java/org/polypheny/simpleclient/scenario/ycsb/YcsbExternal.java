package org.polypheny.simpleclient.scenario.ycsb;

import java.io.File;
import java.util.Properties;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.scenario.Scenario;

public class YcsbExternal extends Scenario {
    private final YcsbConfig config;
    private final YcsbRunner runner;
    private final File targetDirectory;

    public YcsbExternal( YcsbConfig config, YcsbRunner runner, File targetDirectory ) {
        super( null, true, false, QueryMode.TABLE );
        this.config = config;
        this.runner = runner;
        this.targetDirectory = targetDirectory;
    }

    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        runner.setup( new File( targetDirectory, "setup" ) );
    }

    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        runner.load( new File( targetDirectory, "load" ) );
    }

    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        long start = System.nanoTime();
        runner.run( outputDirectory );
        return System.nanoTime() - start;
    }

    @Override
    public void analyze( Properties properties, File outputDirectory ) {
        runner.analyze( properties, outputDirectory );
    }

    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        throw new UnsupportedOperationException( "External YCSB has no separate warmup phase" );
    }

    @Override
    public int getNumberOfInsertThreads() {
        return config.numberOfThreads;
    }
}
