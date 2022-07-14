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

package org.polypheny.simpleclient.scenario.docbench;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.scenario.docbench.queryBuilder.PutProductQueryBuilder;


@Slf4j
public class DataGenerator {

    private final Executor theExecutor;
    private final DocBenchConfig config;

    private final List<BatchableInsert> batchList;

    private boolean aborted;

    private final Random random;

    private final ProgressReporter progressReporter;

    private final List<String> valuesPool;


    DataGenerator( Random random, Executor executor, DocBenchConfig config, ProgressReporter progressReporter, List<String> valuesPool ) {
        theExecutor = executor;
        this.config = config;
        batchList = new LinkedList<>();
        aborted = false;
        this.valuesPool = valuesPool;
        this.progressReporter = progressReporter;
        this.random = random;
    }


    public static int boundedRandom( Random random, int min, int max ) {
        if ( min > max ) {
            throw new RuntimeException( "Min must be smaller than max" );
        }
        if ( min == max ) {
            return min;
        }
        return random.nextInt( max - min ) + min;
    }


    private void addToInsertList( BatchableInsert query ) throws ExecutorException {
        batchList.add( query );
        if ( batchList.size() >= config.batchSize ) {
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


    void generateData() throws ExecutorException {
        int mod = config.numberOfDocuments / (progressReporter.base * config.numberOfThreads);
        PutProductQueryBuilder queryBuilder = new PutProductQueryBuilder( random, valuesPool, config );
        for ( int i = 0; i < config.numberOfDocuments; i++ ) {
            if ( aborted ) {
                break;
            }
            addToInsertList( queryBuilder.getNewQuery() );
            if ( (i % mod) == 0 ) {
                progressReporter.updateProgress();
            }
        }
        executeInsertList();
    }


    public static String randomString( Random random, int length ) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        return random.ints( leftLimit, rightLimit + 1 )
                .limit( length )
                .collect( StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append )
                .toString();
    }

}
