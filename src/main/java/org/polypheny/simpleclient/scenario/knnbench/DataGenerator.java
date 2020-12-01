package org.polypheny.simpleclient.scenario.knnbench;


import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.InsertIntFeature;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.InsertMetadata;
import org.polypheny.simpleclient.scenario.knnbench.queryBuilder.InsertRealFeature;


@Slf4j
public class DataGenerator {
    private final Executor theExecutor;
    private final Config config;
    private final ProgressReporter progressReporter;

    private final List<BatchableInsert> batchList;

    private boolean aborted;

    DataGenerator( Executor executor, Config config, ProgressReporter progressReporter ) {
        theExecutor = executor;
        this.config = config;
        this.progressReporter = progressReporter;
        batchList = new LinkedList<>();

        aborted = false;
    }

    void generateMetadata() throws ExecutorException {
        InsertMetadata queryBuilder = new InsertMetadata();
        for ( int i = 0; i < config.numberOfEntries; i++ ) {
            if ( aborted ) {
                break;
            }

            addToInsertList( queryBuilder.getNewQuery() );
        }
        executeInsertList();
    }


    void generateIntFeatures() throws ExecutorException {
        InsertIntFeature queryBuilder = new InsertIntFeature( config.randomSeedInsert, config.dimensionFeatureVectors );
        for ( int i = 0; i < config.numberOfEntries; i++ ) {
            if ( aborted ) {
                break;
            }

            addToInsertList( queryBuilder.getNewQuery() );
        }
        executeInsertList();
    }

    void generateRealFeatures() throws ExecutorException {
        InsertRealFeature queryBuilder = new InsertRealFeature( config.randomSeedInsert, config.dimensionFeatureVectors );
        for ( int i = 0; i < config.numberOfEntries; i++ ) {
            if ( aborted ) {
                break;
            }

            addToInsertList( queryBuilder.getNewQuery() );
        }
        executeInsertList();
    }


    private void addToInsertList( BatchableInsert query ) throws ExecutorException {
        batchList.add( query );
        if ( batchList.size() >= config.batchSizeInserts ) {
            executeInsertList();
        }
    }


    private void executeInsertList() throws ExecutorException {
        theExecutor.executeInsertList( batchList, config );
        theExecutor.executeCommit();
        batchList.clear();
    }


    public void abort() {
        aborted = true;
    }
}
