/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/15/23, 4:47 PM The Polypheny Project
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

package org.polypheny.simpleclient.executor;

import java.util.List;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.AbstractConfig;

public class NeoExecutor implements Executor {

    @Override
    public void reset() throws ExecutorException {

    }


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        return 0;
    }


    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        return 0;
    }


    @Override
    public void executeCommit() throws ExecutorException {

    }


    @Override
    public void executeRollback() throws ExecutorException {

    }


    @Override
    public void closeConnection() throws ExecutorException {

    }


    @Override
    public void executeInsertList( List<BatchableInsert> batchList, AbstractConfig config ) throws ExecutorException {

    }


    @Override
    public void flushCsvWriter() {

    }



}
