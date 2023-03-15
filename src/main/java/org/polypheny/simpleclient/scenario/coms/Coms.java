/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/11/23, 11:17 AM The Polypheny Project
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

import java.io.File;
import java.util.Properties;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.scenario.Scenario;

@Slf4j
public class Coms extends Scenario {

    public static final String NAMESPACE = "coms";

    private final Random random;
    private final ComsConfig config;


    public Coms( ExecutorFactory executorFactory, ComsConfig config,  boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, queryMode );
        this.random = new Random( config.seed );
        this.config = config;
    }




    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {

    }



    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        log.info( "Generating data..." );
        Executor executor = null;//executorFactory.createExecutorInstance( null, NAMESPACE );
        org.polypheny.simpleclient.scenario.coms.DataGenerator dataGenerator = new DataGenerator();
        try {
            dataGenerator.generateData( config );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while generating data", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        return 0;
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {

    }


    @Override
    public void analyze( Properties properties, File outputDirectory ) {

    }


    @Override
    public int getNumberOfInsertThreads() {
        return 0;
    }

}
