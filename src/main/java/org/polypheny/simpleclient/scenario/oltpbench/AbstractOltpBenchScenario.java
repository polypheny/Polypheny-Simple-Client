package org.polypheny.simpleclient.scenario.oltpbench;

import java.io.File;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.OltpBenchExecutor;
import org.polypheny.simpleclient.executor.OltpBenchExecutor.OltpBenchExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.scenario.Scenario;
import org.polypheny.simpleclient.scenario.oltpbench.auctionmark.AuctionMarkConfig;


@Slf4j
public abstract class AbstractOltpBenchScenario extends Scenario {

    private final AbstractOltpBenchConfig config;
    private final OltpBenchExecutorFactory executorFactory;

    public AbstractOltpBenchScenario( ExecutorFactory executorFactory, AbstractOltpBenchConfig config, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, true, dumpQueryList, queryMode );
        this.config = config;
        if (executorFactory instanceof OltpBenchExecutorFactory) {
            this.executorFactory = (OltpBenchExecutorFactory) executorFactory;
        } else {
            throw new RuntimeException("Unsupported executor factory: " + executorFactory.getClass().getName());
        }
    }


    @Override
    public void createSchema( boolean includingKeys ) {
        if ( queryMode != QueryMode.TABLE ) {
            throw new UnsupportedOperationException( "Unsupported query mode: " + queryMode.name() );
        }

        log.info( "Creating schema using OLTPbench..." );
        OltpBenchExecutor executor;
        try {
            executor = executorFactory.createExecutorInstance();
            executor.createSchema( config );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        }
    }


    @Override
    public void generateData( ProgressReporter progressReporter ) {
        if ( queryMode != QueryMode.TABLE ) {
            throw new UnsupportedOperationException( "Unsupported query mode: " + queryMode.name() );
        }

        log.info( "Loading data using OLTPbench..." );
        OltpBenchExecutor executor;
        try {
            executor = executorFactory.createExecutorInstance();
            executor.loadData( config );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        }
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        if ( queryMode != QueryMode.TABLE ) {
            throw new UnsupportedOperationException( "Unsupported query mode: " + queryMode.name() );
        }

        log.info( "Executing workload using OLTPbench..." );
        long startTime = System.nanoTime();
        OltpBenchExecutor executor;
        try {
            executor = executorFactory.createExecutorInstance();
            executor.executeWorkload( config, outputDirectory );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        }

        long runTime = System.nanoTime() - startTime;
        log.info( "run time: {} s", runTime / 1000000000 );
        return runTime;
    }


    @Override
    public void warmUp( ProgressReporter progressReporter, int iterations ) {

    }


    @Override
    public void analyze( Properties properties ) {

    }


    @Override
    public int getNumberOfInsertThreads() {
        return 0;
    }

}