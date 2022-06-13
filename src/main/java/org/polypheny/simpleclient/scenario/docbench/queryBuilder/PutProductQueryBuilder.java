/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 The Polypheny Project
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
 *
 */

package org.polypheny.simpleclient.scenario.docbench.queryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.docbench.DataGenerator;
import org.polypheny.simpleclient.scenario.docbench.DocBenchConfig;
import org.polypheny.simpleclient.scenario.docbench.MongoQlInsertQuery;


public class PutProductQueryBuilder extends QueryBuilder {

    private final Random random;
    private final DocBenchConfig config;

    private final List<String> valuesPool;


    public PutProductQueryBuilder( Random random, List<String> valuesPool, DocBenchConfig config ) {
        this.random = random;
        this.config = config;
        this.valuesPool = valuesPool;
    }


    @Override
    public BatchableInsert getNewQuery() {
        int numberOfAttributes = DataGenerator.boundedRandom( random, config.minNumberOfAttributes, config.maxNumberOfAttributes );

        // Build attributes pool
        List<String> attributesPool = new ArrayList<>( config.sizeOfAttributesPool );
        for ( int i = 0; i < config.sizeOfAttributesPool; i++ ) {
            attributesPool.add( "attribute" + i );
        }

        // Build query
        StringBuilder expression = new StringBuilder( "{" );
        for ( int i = 0; i < numberOfAttributes; i++ ) {
            String key = attributesPool.get( random.nextInt( config.sizeOfAttributesPool - i ) );
            String value = valuesPool.get( random.nextInt( config.sizeOfValuesPool - 1 ) );
            if ( i > 0 ) {
                expression.append( "," );
            }
            expression.append( "\"" ).append( key ).append( "\":\"" ).append( value ).append( "\"" );
        }
        expression.append( "}" );
        return new MongoQlInsertQuery( "product", expression.toString(), false );
    }

}
