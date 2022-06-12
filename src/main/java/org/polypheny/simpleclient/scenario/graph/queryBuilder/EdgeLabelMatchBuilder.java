/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2022 The Polypheny Project
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

package org.polypheny.simpleclient.scenario.graph.queryBuilder;

import java.util.Random;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.graph.GraphBenchConfig;
import org.polypheny.simpleclient.scenario.graph.GraphQuery.SimpleGraphQuery;

public class EdgeLabelMatchBuilder extends QueryBuilder {


    private final String nodeLeftLabel;
    private final String properties;
    private final String nodeRightLabel;
    private final Random random;
    private final int highestLabel;


    public EdgeLabelMatchBuilder( GraphBenchConfig config ) {
        int randomSeed = config.seed;
        this.random = new Random( randomSeed );

        this.highestLabel = config.maxClusterSize - 1;
        this.nodeLeftLabel = null;
        this.properties = null;
        this.nodeRightLabel = null;
    }


    @Override
    public Query getNewQuery() {
        return new SimpleGraphQuery(
                String.format(
                        "MATCH (n%s)-[rel%s %s]-(m%s) RETURN rel",
                        nodeLeftLabel == null ? "" : ":" + nodeLeftLabel,
                        ":Label" + random.nextInt( this.highestLabel ),
                        properties == null ? "" : properties,
                        nodeRightLabel == null ? "" : ":" + nodeRightLabel ) );
    }

}