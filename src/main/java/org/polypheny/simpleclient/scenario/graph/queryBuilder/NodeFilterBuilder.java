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
import org.polypheny.simpleclient.scenario.graph.GraphQuery;

public class NodeFilterBuilder extends QueryBuilder {

    private final Random random;
    private final GraphBenchConfig config;


    public NodeFilterBuilder( GraphBenchConfig config ) {
        this.random = new Random( config.seed );
        this.config = config;
    }


    @Override
    public Query getNewQuery() {
        int label = random.nextInt( config.highestLabel );
        int key1 = random.nextInt( config.highestProperty );
        int key3 = random.nextInt( config.highestProperty );
        return new GraphQuery( String.format(
                "MATCH (n:Label%s { key_%s_%s: 'value_%s_%s', key_%s_0: 'value_%s_0' }) WHERE n.key_1_%s <> 'value%s' RETURN n", label * 2, key1, key1, key1, key1, key1 + 1, key1 + 1, key3, 1 ) );
    }

}
