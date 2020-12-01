package org.polypheny.simpleclient.scenario.knnbench;


import java.io.File;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.scenario.Scenario;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.CreateIntFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.CreateMetadata;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.CreateRealFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.MetadataKnnIntFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.MetadataKnnRealFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.SimpleKnnIntFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.SimpleKnnRealFeature;


@Slf4j
public class KnnBench extends Scenario {

    private final Config config;


    public KnnBench( Executor.ExecutorFactory executorFactory, Config config, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList );
        this.config = config;
    }

    @Override
    public void createSchema( boolean includingKeys ) {
        log.info( "Creating schema..." );
        Executor executor = null;

        try {
            executor = executorFactory.createInstance();
            executor.executeQuery( (new CreateMetadata( config.dataStoreMetadata )).getNewQuery() );
            executor.executeQuery( (new CreateIntFeature( config.dataStoreFeature, config.dimensionFeatureVectors )).getNewQuery() );
            executor.executeQuery( (new CreateRealFeature( config.dataStoreFeature, config.dimensionFeatureVectors )).getNewQuery() );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public void generateData( ProgressReporter progressReporter ) {
        log.info( "Generating data..." );
        Executor executor1 = executorFactory.createInstance();
        DataGenerator dataGenerator = new DataGenerator( executor1, config, progressReporter );

        try {
            dataGenerator.generateMetadata();
            dataGenerator.generateIntFeatures();
            dataGenerator.generateRealFeatures();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while generating data", e );
        } finally {
            commitAndCloseExecutor( executor1 );
        }
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        log.info( "Executing benchmark..." );
        long startTime = System.nanoTime();

        Executor executor = executorFactory.createInstance( csvWriter );

        SimpleKnnIntFeature simpleKnnIntFeatureBuilder = new SimpleKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleKnnRealFeature simpleKnnRealFeatureBuilder = new SimpleKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        MetadataKnnIntFeature metadataKnnIntFeature = new MetadataKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        MetadataKnnRealFeature metadataKnnRealFeature = new MetadataKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        try {
            for ( int i = 0; i < config.numberOfPureKnnQueries; i++ ) {
                executor.executeQuery( simpleKnnIntFeatureBuilder.getNewQuery() );
            }
            for ( int i = 0; i < config.numberOfPureKnnQueries; i++ ) {
                executor.executeQuery( simpleKnnRealFeatureBuilder.getNewQuery() );
            }
            for ( int i = 0; i < config.numberOfPureKnnQueries; i++ ) {
                executor.executeQuery( metadataKnnIntFeature.getNewQuery() );
            }
            for ( int i = 0; i < config.numberOfPureKnnQueries; i++ ) {
                executor.executeQuery( metadataKnnRealFeature.getNewQuery() );
            }
            executor.flushCsvWriter();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Error occured during workload.", e );
        } finally {
            commitAndCloseExecutor( executor );
        }

        long runTime = System.nanoTime() - startTime;
        log.info( "run time: {} s", runTime / 1000000000 );

        return runTime;
    }


    @Override
    public void warmUp( ProgressReporter progressReporter, int iterations ) {
        log.info( "Warm-up..." );

        Executor executor = null;
        SimpleKnnIntFeature simpleKnnIntFeatureBuilder = new SimpleKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        SimpleKnnRealFeature simpleKnnRealFeatureBuilder = new SimpleKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        MetadataKnnIntFeature metadataKnnIntFeature = new MetadataKnnIntFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
        MetadataKnnRealFeature metadataKnnRealFeature = new MetadataKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );

        for ( int i = 0; i < iterations; i++ ) {
            try {
                executor = executorFactory.createInstance();
                executor.executeQuery( simpleKnnIntFeatureBuilder.getNewQuery() );
                executor.executeQuery( simpleKnnRealFeatureBuilder.getNewQuery() );
                executor.executeQuery( metadataKnnIntFeature.getNewQuery() );
                executor.executeQuery( metadataKnnRealFeature.getNewQuery() );
            } catch ( ExecutorException e ) {
                throw new RuntimeException( "Error while executing warm-up queries", e );
            } finally {
                commitAndCloseExecutor( executor );
            }
            try {
                Thread.sleep( 10000 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        }
    }


    @Override
    public void analyze( Properties properties ) {

    }


    private void commitAndCloseExecutor( Executor executor ) {
        if ( executor != null ) {
            try {
                executor.executeCommit();
            } catch ( ExecutorException e ) {
                try {
                    executor.executeRollback();
                } catch ( ExecutorException ex ) {
                    log.error( "Error while rollback connection", e );
                }
            }
            try {
                executor.closeConnection();
            } catch ( ExecutorException e ) {
                log.error( "Error while closing connection", e );
            }
        }
    }
}
