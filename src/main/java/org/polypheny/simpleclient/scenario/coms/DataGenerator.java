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

import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.scenario.coms.simulation.Graph;
import org.polypheny.simpleclient.scenario.coms.simulation.NetworkGenerator;

public class DataGenerator {

    public void generateData( ComsConfig config ) throws ExecutorException {
        // generate network (simulation)
        // -> generate structure of network -> nodes, edges
        // -> generate parameters per nodes -> fixed & dynamic
        // -> generate configs depending on type of node/edge
        // -> generate logs, execute 5% untimed
        // -> generate queries on network, execute 5%

        // generate simulation setting
        NetworkGenerator generator = new NetworkGenerator( config );
        Graph graph = generator.network.toGraph();

        graph.getGraphQueries();
        graph.getDocQueries();
        graph.getRelQueries();

        // -> generate logs

        // -> generate queries for simulation

        // -> generate %5 of log queries

        // -> genrate 5% of logs for simulation

    }

}
