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

package org.polypheny.simpleclient.cli;

import com.github.rvesse.airline.annotations.Command;
import java.io.File;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.main.ProgressBar;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.scenario.oltpbench.smallbank.SmallBank;
import org.polypheny.simpleclient.scenario.oltpbench.smallbank.SmallBankConfig;


@Slf4j
@Command(name = "smallbank", description = "Mode for quick testing of Polypheny-DB using the SmallBank benchmark.")
public class SmallBankCommand extends AbstractOltpBenchCommand {

    @Override
    protected void schema( ExecutorFactory executorFactory ) {
        SmallBankConfig config = new SmallBankConfig( getProperties( "smallbank.properties" ), 1 );
        SmallBank smallBank = new SmallBank( executorFactory, config, false, QueryMode.TABLE );
        smallBank.createSchema( true );
    }


    @Override
    protected void data( ExecutorFactory executorFactory, int multiplier ) {
        SmallBankConfig config = new SmallBankConfig( getProperties( "smallbank.properties" ), multiplier );
        SmallBank smallBank = new SmallBank( executorFactory, config, false, QueryMode.TABLE );
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        smallBank.generateData( progressReporter );
    }


    @Override
    protected void workload( ExecutorFactory executorFactory, int multiplier ) {
        SmallBankConfig config = new SmallBankConfig( getProperties( "smallbank.properties" ), multiplier );
        SmallBank smallBank = new SmallBank( executorFactory, config, false, QueryMode.TABLE );
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        smallBank.execute( progressReporter, null, new File( "." ), config.numberOfThreads );
    }

}
