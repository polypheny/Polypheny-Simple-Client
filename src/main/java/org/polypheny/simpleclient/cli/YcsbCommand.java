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

package org.polypheny.simpleclient.cli;

import com.github.rvesse.airline.annotations.Command;
import java.io.File;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.main.ProgressBar;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.scenario.oltpbench.ycsb.Ycsb;
import org.polypheny.simpleclient.scenario.oltpbench.ycsb.YcsbConfig;


@Slf4j
@Command(name = "ycsb", description = "Mode for quick testing of Polypheny-DB using the YCSB benchmark.")
public class YcsbCommand extends AbstractOltpBenchCommand {

    @Override
    protected void schema( ExecutorFactory executorFactory ) {
        YcsbConfig config = new YcsbConfig( getProperties( "ycsb.properties" ), 1 );
        Ycsb ycsb = new Ycsb( executorFactory, config, false, QueryMode.TABLE );
        ycsb.createSchema( true );
    }


    @Override
    protected void data( ExecutorFactory executorFactory, int multiplier ) {
        YcsbConfig config = new YcsbConfig( getProperties( "ycsb.properties" ), multiplier );
        Ycsb ycsb = new Ycsb( executorFactory, config, false, QueryMode.TABLE );
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        ycsb.generateData( progressReporter );
    }


    @Override
    protected void workload( ExecutorFactory executorFactory, int multiplier ) {
        YcsbConfig config = new YcsbConfig( getProperties( "ycsb.properties" ), multiplier );
        Ycsb ycsb = new Ycsb( executorFactory, config, false, QueryMode.TABLE );
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        ycsb.execute( progressReporter, null, new File( "." ), config.numberOfThreads );
    }

}
