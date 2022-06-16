/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-14.06.22, 09:32 The Polypheny Project
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

package org.polypheny.simpleclient.scenario.multibench;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.PolyphenyDbMultiExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.scenario.Scenario;
import org.polypheny.simpleclient.scenario.docbench.DocBench;
import org.polypheny.simpleclient.scenario.gavel.Gavel;
import org.polypheny.simpleclient.scenario.graph.GraphBench;
import org.polypheny.simpleclient.scenario.knnbench.KnnBench;


@Slf4j
public class MultiBench extends Scenario {

    private final Gavel gavel;
    private final GraphBench graphBench;
    private final DocBench docBench;
    private final KnnBench knnBench;


    public MultiBench( Executor.ExecutorFactory multiExecutorFactory, MultiBenchConfig config, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        super( multiExecutorFactory, commitAfterEveryQuery, dumpQueryList, QueryMode.TABLE );

        if ( !(multiExecutorFactory instanceof PolyphenyDbMultiExecutorFactory) ) {
            throw new RuntimeException( "This benchmark requires a multi executor" );
        }
        PolyphenyDbMultiExecutorFactory executorFactory = (PolyphenyDbMultiExecutorFactory) multiExecutorFactory;

        // Initialize underlying benchmarks
        gavel = new Gavel(
                executorFactory.getJdbcExecutorFactory(),
                config.getGavelConfig(),
                commitAfterEveryQuery,
                dumpQueryList,
                QueryMode.TABLE
        );
        graphBench = new GraphBench(
                executorFactory.getCypherExecutorFactory(),
                config.getGraphBenchConfig(),
                commitAfterEveryQuery,
                dumpQueryList
        );
        docBench = new DocBench(
                executorFactory.getMongoQlExecutorFactory(),
                config.getDocBenchConfig(),
                commitAfterEveryQuery,
                dumpQueryList
        );
        knnBench = new KnnBench(
                executorFactory.getJdbcExecutorFactory(),
                config.getKnnBenchConfig(),
                commitAfterEveryQuery,
                dumpQueryList
        );
    }


    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        log.info( "Creating MultiBench schemas..." );
        docBench.createSchema( databaseInstance, includingKeys );
        gavel.createSchema( databaseInstance, includingKeys );
        graphBench.createSchema( databaseInstance, includingKeys );
        knnBench.createSchema( databaseInstance, includingKeys );
    }


    @Override
    public void generateData( ProgressReporter progressReporter ) {
        log.info( "Generating MultiBench data..." );
        progressReporter.update( 0 );
        System.out.println( "Generating DocBench data..." );
        docBench.generateData( progressReporter );
        System.out.println( "Generating DocBench data... done" );
        progressReporter.update( 0 );
        System.out.println( "Generating Gavel data..." );
        gavel.generateData( progressReporter );
        System.out.println( "Generating Gavel data... done" );
        progressReporter.update( 0 );
        System.out.println( "Generating KnnBench data..." );
        knnBench.generateData( progressReporter );
        System.out.println( "Generating KnnBench data... done" );
        progressReporter.update( 0 );
        System.out.println( "Generating GraphBench data..." );
        graphBench.generateData( progressReporter );
        System.out.println( "Generating GraphBench data... done" );
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        log.info( "MultiBench Warm-up..." );
        docBench.warmUp( progressReporter );
        knnBench.warmUp( progressReporter );
        gavel.warmUp( progressReporter );
        graphBench.warmUp( progressReporter );
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        log.info( "MultiBench workloads..." );
        long runtime = 0;
        progressReporter.update( 0 );
        System.out.println( "Executing GraphBench..." );
        runtime += graphBench.execute( progressReporter, csvWriter, outputDirectory, numberOfThreads );
        System.out.println( "Executing GraphBench... done" );
        progressReporter.update( 0 );
        System.out.println( "Executing DocBench..." );
        runtime += docBench.execute( progressReporter, csvWriter, outputDirectory, numberOfThreads );
        System.out.println( "Executing DocBench... done" );
        progressReporter.update( 0 );
        System.out.println( "Executing KnnBench..." );
        runtime += knnBench.execute( progressReporter, csvWriter, outputDirectory, numberOfThreads );
        System.out.println( "Executing KnnBench... done" );
        progressReporter.update( 0 );
        System.out.println( "Executing Gavel..." );
        runtime += gavel.execute( progressReporter, csvWriter, outputDirectory, numberOfThreads );
        System.out.println( "Executing Gavel... done" );
        return runtime;
    }


    @Override
    public void analyze( Properties properties, File outputDirectory ) {
        log.info( "MultiBench Analyze..." );

        // DocBench
        Properties docBenchResults = new Properties();
        docBench.analyze( docBenchResults, new File( outputDirectory, "docbench" ) );
        for ( Map.Entry<Object, Object> entry : docBenchResults.entrySet() ) {
            properties.put( "docbench." + entry.getKey(), entry.getValue() );
        }

        // GraphBench
        Properties graphBenchResults = new Properties();
        graphBench.analyze( graphBenchResults, new File( outputDirectory, "graphbench" ) );
        for ( Map.Entry<Object, Object> entry : graphBenchResults.entrySet() ) {
            properties.put( "graphbench." + entry.getKey(), entry.getValue() );
        }

        // KnnhBench
        Properties knnBenchResults = new Properties();
        knnBench.analyze( knnBenchResults, new File( outputDirectory, "knnbench" ) );
        for ( Map.Entry<Object, Object> entry : knnBenchResults.entrySet() ) {
            properties.put( "knnbench." + entry.getKey(), entry.getValue() );
        }

        // Gavel
        Properties gavelResults = new Properties();
        gavel.analyze( gavelResults, new File( outputDirectory, "gavel" ) );
        for ( Map.Entry<Object, Object> entry : gavelResults.entrySet() ) {
            properties.put( "gavel." + entry.getKey(), entry.getValue() );
        }
    }


    @Override
    public int getNumberOfInsertThreads() {
        return 1;
    }

}