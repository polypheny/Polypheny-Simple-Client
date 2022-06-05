package org.polypheny.simpleclient.scenario.oltpbench;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            throw new RuntimeException( "Exception while loading data", e );
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
            throw new RuntimeException( "Exception while executing workload", e );
        }

        long runTime = System.nanoTime() - startTime;
        log.info( "run time: {} s", runTime / 1000000000 );
        return runTime;
    }


    @Override
    public void warmUp( ProgressReporter progressReporter, int iterations ) {
        log.info( "Executing warmup workload using OLTPbench..." );
        OltpBenchExecutor executor;
        try {
            executor = executorFactory.createExecutorInstance();
            executor.executeWorkload( config, new File( System.getProperty( "java.io.tmpdir" ) ) );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while executing warmup workload", e );
        }
    }


    @Override
    public void analyze( Properties properties, File outputDirectory ) {
        File csvFile = new File( outputDirectory, "oltpbench.csv" );
        if ( !csvFile.exists() ) {
            throw new RuntimeException( "Something went wrong, there should be an oltpbench.csv file!" );
        }

        Map<String, List<Long>> latencyPerTransactionType = new HashMap<>();
        List<Long> latency = new ArrayList<>();
        try ( CSVReader csvReader = new CSVReader( new FileReader( csvFile ) ) ) {
            String[] values;
            csvReader.readNextSilently();
            while ( (values = csvReader.readNext()) != null ) {
                if ( !latencyPerTransactionType.containsKey( values[1] ) ) {
                    latencyPerTransactionType.put( values[1], new ArrayList<>() );
                }
                latencyPerTransactionType.get( values[1] ).add( Long.parseLong( values[3] ) );
                latency.add( Long.parseLong( values[3] ) );
            }
        } catch ( IOException | CsvValidationException e ) {
            throw new RuntimeException( "Error while reading csv file", e );
        }

        properties.put( "MeanLatency", calculateMean( latency ) );
        for ( Map.Entry<String, List<Long>> entry : latencyPerTransactionType.entrySet() ) {
            properties.put( entry.getKey() + "MeanLatency", calculateMean( entry.getValue() ) );
        }

        properties.put( "NumberOfTransactions", latency.size() );
        double runtimeSeconds = Long.parseLong( properties.get( "runtime" ).toString() ) / 1000000000.0;
        double throughput = latency.size() / runtimeSeconds;
        properties.put( "Throughput", throughput );
    }


    @Override
    public int getNumberOfInsertThreads() {
        return 0;
    }

}