/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
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

package org.polypheny.simpleclient.scenario;


import java.io.File;
import java.util.Properties;
import lombok.Getter;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;


public abstract class Scenario {

    @Getter
    protected final ExecutorFactory executorFactory;
    protected final boolean commitAfterEveryQuery;
    protected final boolean dumpQueryList;


    protected Scenario( ExecutorFactory executorFactory, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        this.executorFactory = executorFactory;
        this.commitAfterEveryQuery = commitAfterEveryQuery;
        this.dumpQueryList = dumpQueryList;
    }


    public abstract void createSchema( boolean includingKeys );

    public abstract void generateData( ProgressReporter progressReporter );

    public abstract long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads );

    public abstract void warmUp( ProgressReporter progressReporter );

    public abstract void analyze( Properties properties );

}
