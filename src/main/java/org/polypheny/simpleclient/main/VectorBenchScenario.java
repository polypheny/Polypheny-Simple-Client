/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2026 The Polypheny Project
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.polypheny.simpleclient.main;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.PostgresExecutor.PostgresExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.JdbcExecutor;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.vectorbench.PgVectorBench;
import org.polypheny.simpleclient.scenario.vectorbench.RecallEvaluator;
import org.polypheny.simpleclient.scenario.vectorbench.VectorBench;
import org.polypheny.simpleclient.scenario.vectorbench.VectorBenchConfig;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.dql.SimpleKnnRealFeature;
import org.polypheny.simpleclient.scenario.vectorbench.queryBuilder.postgres.dql.PgSimpleKnnRealFeature;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

@Slf4j
public class VectorBenchScenario {

    public static void schema( ExecutorFactory executorFactory, boolean commitAfterEveryQuery ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), 1 );
        VectorBench vectorBench = new VectorBench( executorFactory, config, commitAfterEveryQuery, false );
        vectorBench.createSchema( null, true );
    }


    public static void pgSchema( boolean commitAfterEveryQuery ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), 1 );
        ExecutorFactory factory = new PostgresExecutorFactory( config.postgresHost, false );
        PgVectorBench vectorBench = new PgVectorBench( factory, config, commitAfterEveryQuery, false );
        vectorBench.createSchema( null, true );
    }


    public static void index( ExecutorFactory executorFactory, boolean commitAfterEveryQuery ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), 1 );
        VectorBench vectorBench = new VectorBench( executorFactory, config, commitAfterEveryQuery, false );
        vectorBench.createIndex();
    }


    public static void pgIndex( boolean commitAfterEveryQuery ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), 1 );
        ExecutorFactory factory = new PostgresExecutorFactory( config.postgresHost, false );
        PgVectorBench vectorBench = new PgVectorBench( factory, config, commitAfterEveryQuery, false );
        vectorBench.createIndex();
    }


    public static void groundTruth( ExecutorFactory executorFactory ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), 1 );
        runRecall( executorFactory, config, polyphenyKnnBuilder( config ), true );
    }


    public static void recall( ExecutorFactory executorFactory ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), 1 );
        runRecall( executorFactory, config, polyphenyKnnBuilder( config ), false );
    }


    public static void pgGroundTruth() {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), 1 );
        ExecutorFactory factory = new PostgresExecutorFactory( config.postgresHost, false );
        runRecall( factory, config, pgKnnBuilder( config ), true );
    }


    public static void pgRecall() {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), 1 );
        ExecutorFactory factory = new PostgresExecutorFactory( config.postgresHost, false );
        runRecall( factory, config, pgKnnBuilder( config ), false );
    }


    private static QueryBuilder polyphenyKnnBuilder( VectorBenchConfig config ) {
        return new SimpleKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
    }


    private static QueryBuilder pgKnnBuilder( VectorBenchConfig config ) {
        return new PgSimpleKnnRealFeature( config.randomSeedQuery, config.dimensionFeatureVectors, config.limitKnnQueries, config.distanceNorm );
    }


    private static void runRecall( ExecutorFactory executorFactory, VectorBenchConfig config, QueryBuilder knnBuilder, boolean capture ) {
        JdbcExecutor executor = (JdbcExecutor) executorFactory.createExecutorInstance();
        RecallEvaluator evaluator = new RecallEvaluator( config, executor, knnBuilder, RecallEvaluator.DEFAULT_GROUND_TRUTH_FILE );
        try {
            if ( capture ) {
                evaluator.captureGroundTruth();
            } else {
                double recall = evaluator.evaluate();
                writeRecall( config, recall );
            }
        } finally {
            try {
                executor.closeConnection();
            } catch ( ExecutorException e ) {
                log.error( "Error while closing connection", e );
            }
        }
    }


    /** Writes the recall@k of the last `recall` run to recall.csv in the working directory. */
    private static void writeRecall( VectorBenchConfig config, double recall ) {
        try ( FileWriter fw = new FileWriter( "recall.csv" ) ) {
            fw.write( "k,recall\n" );
            fw.write( config.limitKnnQueries + "," + recall + "\n" );
        } catch ( IOException e ) {
            log.error( "Could not write recall.csv", e );
        }
    }


    public static void data( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), multiplier );
        VectorBench vectorBench = new VectorBench( executorFactory, config, commitAfterEveryQuery, false );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        vectorBench.generateData( null, progressReporter );
    }


    public static void pgData( int multiplier, boolean commitAfterEveryQuery ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), multiplier );
        ExecutorFactory factory = new PostgresExecutorFactory( config.postgresHost, false );
        PgVectorBench bench = new PgVectorBench( factory, config, commitAfterEveryQuery, false );
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        bench.generateData( null, progressReporter );
    }


    public static void workload( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery, boolean writeCsv, boolean dumpQueryList ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), multiplier );
        VectorBench vectorBench = new VectorBench( executorFactory, config, commitAfterEveryQuery, dumpQueryList );

        final CsvWriter csvWriter;
        if ( writeCsv ) {
            csvWriter = new CsvWriter( "results.csv" );
        } else {
            csvWriter = null;
        }

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        vectorBench.execute( progressReporter, csvWriter, new File( "." ), config.numberOfThreads );
    }


    public static void pgWorkload( int multiplier, boolean commitAfterEveryQuery, boolean writeCsv, boolean dumpQueryList ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), multiplier );
        ExecutorFactory factory = new PostgresExecutorFactory( config.postgresHost, false );
        PgVectorBench bench = new PgVectorBench( factory, config, commitAfterEveryQuery, dumpQueryList );
        CsvWriter csvWriter = writeCsv ? new CsvWriter( "results-pg.csv" ) : null;
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        bench.execute( progressReporter, csvWriter, new File( "." ), config.numberOfThreads );
    }



    public static void warmup( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), multiplier );
        VectorBench vectorBench = new VectorBench( executorFactory, config, commitAfterEveryQuery, dumpQueryList );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        vectorBench.warmUp( progressReporter );
    }


    public static void pgWarmup( int multiplier, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        VectorBenchConfig config = new VectorBenchConfig( getProperties(), multiplier );
        ExecutorFactory factory = new PostgresExecutorFactory( config.postgresHost, false );
        PgVectorBench bench = new PgVectorBench( factory, config, commitAfterEveryQuery, dumpQueryList );
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        bench.warmUp( progressReporter );
    }


    public static boolean isPostgresMode() {
        return new VectorBenchConfig( getProperties(), 1 ).mode.equalsIgnoreCase( "postgres" );
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/vectorbench/vector.properties" ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }

}
