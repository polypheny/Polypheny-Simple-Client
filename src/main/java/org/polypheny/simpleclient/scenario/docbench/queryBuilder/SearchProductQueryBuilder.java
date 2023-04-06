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

package org.polypheny.simpleclient.scenario.docbench.queryBuilder;

import java.util.List;
import java.util.Random;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.docbench.DocBenchConfig;
import org.polypheny.simpleclient.scenario.docbench.MongoQlQuery;


public class SearchProductQueryBuilder extends QueryBuilder {

    private final Random random;
    private final DocBenchConfig config;
    private final List<String> valuesPool;


    public SearchProductQueryBuilder( Random random, List<String> valuesPool, DocBenchConfig config ) {
        this.random = random;
        this.config = config;
        this.valuesPool = valuesPool;
    }


    @Override
    public Query getNewQuery() {
        //String attribute = "attribute" + random.nextInt( config.sizeOfAttributesPool );
        String attribute = "attribute1";
        String value = valuesPool.get( random.nextInt( config.sizeOfValuesPool ) );

        // Find all products (documents) having a field with the name "attribute" and containing "value"
        return new MongoQlQuery( "db.product.find({\"" + attribute + "\":\"" + value + "\"})", true );
    }

}
