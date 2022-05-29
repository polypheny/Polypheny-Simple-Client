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

package org.polypheny.simpleclient.executor;


import java.io.File;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.AbstractConfig;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchConfig;


@Slf4j
public abstract class OltpBenchExecutor implements Executor {


    public OltpBenchExecutor() {
        downloadOltpBench();
    }


    @Override
    public void reset() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }

    @Override
    public abstract long executeQuery( Query query ) throws ExecutorException;


    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        throw new ExecutorException( "Unsupported Operation" );
    };


    @Override
    public void executeCommit() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeRollback() throws ExecutorException {
        throw new ExecutorException( "Unsupported operation" );
    }


    @Override
    public void closeConnection() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeInsertList( List<BatchableInsert> queryList, AbstractConfig config ) throws ExecutorException {
        throw new ExecutorException( "Unsupported operation" );
    }


    private void downloadOltpBench() {
        // Check if already downloaded
        throw new RuntimeException("Implement");
    }


    public void createSchema( AbstractOltpBenchConfig config ) throws ExecutorException {
        String xmlConfig = getConfigXml( config );
        // Write config file
        // Execute OLTPBench with config
    }


    public void loadData( AbstractOltpBenchConfig config ) throws ExecutorException {
        // Write config file
        // Execute OLTPBench with config
    }


    public void executeWorkload( AbstractOltpBenchConfig config, File outputDirectory ) throws ExecutorException {
        // Write config file
        // Execute OLTPBench with config
    }


    public abstract void collectResults() throws ExecutorException ;


    protected abstract String getConfigXml( AbstractOltpBenchConfig config );


    @Override
    public void flushCsvWriter() {
        // CSV writer is not supported with OLTPBench Executor
    }


    public abstract static class OltpBenchExecutorFactory extends ExecutorFactory {

        @Override
        public abstract OltpBenchExecutor createExecutorInstance();


        @Override
        public OltpBenchExecutor createExecutorInstance( CsvWriter csvWriter ) {
            if ( csvWriter == null ) {
                return  createExecutorInstance();
            } else {
                throw new RuntimeException("CSV writer is not supported with OltpBench!");
            }
        }

        // Allows to limit number of concurrent executor threads, 0 means no limit
        @Override
        public abstract int getMaxNumberOfThreads();

    }



}
