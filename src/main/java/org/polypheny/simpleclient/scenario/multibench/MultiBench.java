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
        if ( config.numberOfGavelQueries > 0 ) {
            gavel = new Gavel(
                    executorFactory.getJdbcExecutorFactory(),
                    config.getGavelConfig(),
                    commitAfterEveryQuery,
                    dumpQueryList,
                    QueryMode.TABLE
            );
        } else {
            gavel = null;
        }
        if ( config.numberOfGraphBenchQueries > 0 ) {
            graphBench = new GraphBench(
                    executorFactory.getCypherExecutorFactory(),
                    config.getGraphBenchConfig(),
                    commitAfterEveryQuery,
                    dumpQueryList
            );
        } else {
            graphBench = null;
        }
        if ( config.numberOfDocBenchQueries > 0 ) {
            docBench = new DocBench(
                    executorFactory.getMongoQlExecutorFactory(),
                    config.getDocBenchConfig(),
                    commitAfterEveryQuery,
                    dumpQueryList
            );
        } else {
            docBench = null;
        }
        if ( config.numberOfKnnBenchQueries > 0 ) {
            knnBench = new KnnBench(
                    executorFactory.getJdbcExecutorFactory(),
                    config.getKnnBenchConfig(),
                    commitAfterEveryQuery,
                    dumpQueryList
            );
        } else {
            knnBench = null;
        }
    }


    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        log.info( "Creating MultiBench schemas..." );
        if ( docBench != null ) {
            docBench.createSchema( databaseInstance, includingKeys );
        }
        if ( gavel != null ) {
            gavel.createSchema( databaseInstance, includingKeys );
        }
        if ( graphBench != null ) {
            graphBench.createSchema( databaseInstance, includingKeys );
        }
        if ( knnBench != null ) {
            knnBench.createSchema( databaseInstance, includingKeys );
        }
    }


    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        if ( docBench != null ) {
            progressReporter.update( 0 );
            log.info( "Generating DocBench data..." );
            docBench.generateData( databaseInstance, progressReporter );
        }

        if ( gavel != null ) {
            progressReporter.update( 0 );
            log.info( "Generating Gavel data..." );
            gavel.generateData( databaseInstance, progressReporter );
        }

        if ( knnBench != null ) {
            progressReporter.update( 0 );
            log.info( "Generating KnnBench data..." );
            knnBench.generateData( databaseInstance, progressReporter );
        }

        if ( graphBench != null ) {
            progressReporter.update( 0 );
            log.info( "Generating GraphBench data..." );
            graphBench.generateData( databaseInstance, progressReporter );
        }
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        if ( docBench != null ) {
            log.info( "DocBench Warm-up..." );
            docBench.warmUp( progressReporter );
        }

        if ( knnBench != null ) {
            log.info( "KnnBench Warm-up..." );
            knnBench.warmUp( progressReporter );
        }

        if ( gavel != null ) {
            log.info( "Gavel Warm-up..." );
            gavel.warmUp( progressReporter );
        }

        if ( graphBench != null ) {
            log.info( "GraphBench Warm-up..." );
            graphBench.warmUp( progressReporter );
        }
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        long runtime = 0;

        if ( graphBench != null ) {
            progressReporter.update( 0 );
            log.info( "Executing GraphBench..." );
            runtime += graphBench.execute( progressReporter, csvWriter, outputDirectory, numberOfThreads );
        }

        if ( docBench != null ) {
            progressReporter.update( 0 );
            log.info( "Executing DocBench..." );
            runtime += docBench.execute( progressReporter, csvWriter, outputDirectory, numberOfThreads );
        }

        if ( knnBench != null ) {
            progressReporter.update( 0 );
            log.info( "Executing KnnBench..." );
            runtime += knnBench.execute( progressReporter, csvWriter, outputDirectory, numberOfThreads );
        }

        if ( gavel != null ) {
            progressReporter.update( 0 );
            log.info( "Executing Gavel..." );
            runtime += gavel.execute( progressReporter, csvWriter, outputDirectory, numberOfThreads );
        }

        return runtime;
    }


    @Override
    public void analyze( Properties properties, File outputDirectory ) {
        log.info( "MultiBench Analyze..." );
        double totalExecuteRuntime = 0;
        long totalNumberOfQueries = 0;

        // DocBench
        if ( docBench != null ) {
            Properties docBenchResults = new Properties();
            docBench.analyze( docBenchResults, new File( outputDirectory, "docbench" ) );
            for ( Map.Entry<Object, Object> entry : docBenchResults.entrySet() ) {
                properties.put( "docbench." + entry.getKey(), entry.getValue() );
            }
            totalExecuteRuntime += Double.parseDouble( docBenchResults.get( "executeRuntime" ).toString() );
            totalNumberOfQueries += Long.parseLong( docBenchResults.get( "numberOfQueries" ).toString() );
        }

        // GraphBench
        if ( graphBench != null ) {
            Properties graphBenchResults = new Properties();
            graphBench.analyze( graphBenchResults, new File( outputDirectory, "graphbench" ) );
            for ( Map.Entry<Object, Object> entry : graphBenchResults.entrySet() ) {
                properties.put( "graphbench." + entry.getKey(), entry.getValue() );
            }
            totalExecuteRuntime += Double.parseDouble( graphBenchResults.get( "executeRuntime" ).toString() );
            totalNumberOfQueries += Long.parseLong( graphBenchResults.get( "numberOfQueries" ).toString() );
        }

        // KnnhBench
        if ( knnBench != null ) {
            Properties knnBenchResults = new Properties();
            knnBench.analyze( knnBenchResults, new File( outputDirectory, "knnbench" ) );
            for ( Map.Entry<Object, Object> entry : knnBenchResults.entrySet() ) {
                properties.put( "knnbench." + entry.getKey(), entry.getValue() );
            }
            totalExecuteRuntime += Double.parseDouble( knnBenchResults.get( "executeRuntime" ).toString() );
            totalNumberOfQueries += Long.parseLong( knnBenchResults.get( "numberOfQueries" ).toString() );
        }

        // Gavel
        if ( gavel != null ) {
            Properties gavelResults = new Properties();
            gavel.analyze( gavelResults, new File( outputDirectory, "gavel" ) );
            for ( Map.Entry<Object, Object> entry : gavelResults.entrySet() ) {
                properties.put( "gavel." + entry.getKey(), entry.getValue() );
            }
            totalExecuteRuntime += Double.parseDouble( gavelResults.get( "executeRuntime" ).toString() );
            totalNumberOfQueries += Long.parseLong( gavelResults.get( "numberOfQueries" ).toString() );
        }

        // Calculate mean throughput
        properties.put( "executeRuntime", totalExecuteRuntime );
        properties.put( "numberOfQueries", totalNumberOfQueries );
        properties.put( "throughput", totalNumberOfQueries / totalExecuteRuntime );
    }


    @Override
    public int getNumberOfInsertThreads() {
        return 1;
    }

}