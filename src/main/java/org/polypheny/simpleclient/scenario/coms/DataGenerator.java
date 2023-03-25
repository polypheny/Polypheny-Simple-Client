/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/11/23, 11:19 AM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.coms;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.ProgressBar;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.coms.simulation.Graph;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator;

@Slf4j
public class DataGenerator {

    public NetworkGenerator generator;


    public void generateData( ComsConfig config, Executor executor ) throws ExecutorException {
        // generateSchema network (simulation)
        // -> generateSchema structure of network -> nodes, edges
        // -> generateSchema parameters per nodes -> fixed & dynamic
        // -> generateSchema configs depending on type of node/edge
        // -> generateSchema logs, execute 5% untimed
        // -> generateSchema queries on network, execute 5%

        log.info( "Generating data..." );
        // generateSchema simulation setting
        updateNetworkGenerator( config );
        Graph graph = generator.network.toGraph();

        // insert into db

        executeAndUpdateProgress( config, executor, graph.getGraphQueries( config.graphCreateBatch ), "Generating Graph Data..." );

        executeAndUpdateProgress( config, executor, graph.getDocQueries( config.docCreateBatch ), "\nGenerating Document Data..." );

        executeAndUpdateProgress( config, executor, graph.getRelQueries( config.relCreateBatch ), "\nGenerating Relational Data..." );

        log.info( "Finished..." );

        // -> generateSchema logs

        // -> generateSchema queries for simulation

        // -> generateSchema %5 of log queries

        // -> generate 5% of logs for simulation

    }


    public void updateNetworkGenerator( ComsConfig config ) {
        this.generator = new NetworkGenerator( config );
    }


    private static void executeAndUpdateProgress( ComsConfig config, Executor executor, List<Query> queries, String msg ) throws ExecutorException {
        int size = queries.size();
        System.out.println( msg );
        ProgressReporter progress = new ProgressBar( 1, config.progressReportBase );
        int mod = queries.size() / progress.base;
        if ( mod == 0 ) {
            mod = progress.base / queries.size();
            for ( Query query : queries ) {
                executor.executeQuery( query );
                for ( int j = 0; j < mod; j++ ) {
                    progress.updateProgress();
                }
            }
        } else {
            for ( int i = 0; i < size; i++ ) {
                executor.executeQuery( queries.get( i ) );
                if ( (i % mod) == 0 ) {
                    progress.updateProgress();
                }
            }
        }
    }


    public List<Query> generateWorkload() {
        if ( log.isInfoEnabled() ) {
            log.info( "Start Configuration:\n" + generator.network );
        }

        List<Query> queries = new ArrayList<>( generator.network.simulateRun() );

        if ( log.isInfoEnabled() ) {
            log.info( "End Configuration:\n" + generator.network );
        }
        return queries;
    }

}
