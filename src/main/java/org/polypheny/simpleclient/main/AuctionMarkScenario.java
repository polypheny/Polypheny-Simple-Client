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

package org.polypheny.simpleclient.main;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.scenario.oltpbench.auctionmark.AuctionMark;
import org.polypheny.simpleclient.scenario.oltpbench.auctionmark.AuctionMarkConfig;


@Slf4j
public class AuctionMarkScenario {

    public static void schema( ExecutorFactory executorFactory, boolean dumpQueryList ) {
        AuctionMarkConfig config = new AuctionMarkConfig( getProperties(), 1 );
        AuctionMark auctionMark = new AuctionMark( executorFactory, config, dumpQueryList, QueryMode.TABLE );
        auctionMark.createSchema( true );
    }


    public static void data( ExecutorFactory executorFactory, int multiplier, boolean dumpQueryList ) {
        AuctionMarkConfig config = new AuctionMarkConfig( getProperties(), multiplier );
        AuctionMark auctionMark = new AuctionMark( executorFactory, config, dumpQueryList, QueryMode.TABLE );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        auctionMark.generateData( progressReporter );
    }


    public static void workload( ExecutorFactory executorFactory, int multiplier, boolean dumpQueryList ) {
        AuctionMarkConfig config = new AuctionMarkConfig( getProperties(), multiplier );
        AuctionMark auctionMark = new AuctionMark( executorFactory, config, dumpQueryList, QueryMode.TABLE );
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        auctionMark.execute( progressReporter, null, new File( "." ), config.numberOfThreads );
    }


    public static void warmup( ExecutorFactory executorFactory, int multiplier, boolean dumpQueryList ) {
        throw new RuntimeException( "Unsupported task" );
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/oltpbench/auctionmark.properties" ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }

}
