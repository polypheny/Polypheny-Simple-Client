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
 *
 */

package org.polypheny.simpleclient.scenario;


import java.io.File;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.List;
import java.util.OptionalDouble;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;


@Slf4j
public abstract class Scenario {

    @Getter
    protected final ExecutorFactory executorFactory;
    protected final ExecutorFactory mqlExecutorFactory;
    protected final boolean commitAfterEveryQuery;
    protected final boolean dumpQueryList;
    protected final QueryMode queryMode;


    protected Scenario( ExecutorFactory executorFactory, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        this.executorFactory = executorFactory;
        this.mqlExecutorFactory = null;
        this.commitAfterEveryQuery = commitAfterEveryQuery;
        this.dumpQueryList = dumpQueryList;
        this.queryMode = queryMode;
    }

    protected Scenario( ExecutorFactory jdbcExecutorFactory, ExecutorFactory mqlExecutorFactory, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        this.executorFactory = jdbcExecutorFactory;
        this.mqlExecutorFactory = mqlExecutorFactory;
        this.commitAfterEveryQuery = commitAfterEveryQuery;
        this.dumpQueryList = dumpQueryList;
        this.queryMode = queryMode;
    }


    public abstract void createSchema( boolean includingKeys );

    public abstract void generateData( ProgressReporter progressReporter );

    public abstract long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads );

    public abstract void warmUp( ProgressReporter progressReporter, int iterations );

    public abstract void analyze( Properties properties );


    protected double calculateMean( List<Long> times ) {
        DecimalFormat df = new DecimalFormat( "0.000" );
        OptionalDouble meanOptional = times.stream().mapToLong( Long::longValue ).average();
        if ( meanOptional.isPresent() ) {
            // scale
            double mean = meanOptional.getAsDouble() / 1000000;
            String roundFormat = df.format( mean );
            try {
                return df.parse( roundFormat ).doubleValue();
            } catch ( ParseException e ) {
                log.error( "Exception", e );
            }
        }
        return -1;
    }


    protected double calculateSampleStandardDeviation( List<Long> times, double mean ) {
        double preVariance = times.stream().mapToDouble( it -> Math.pow( (it - mean), 2 ) ).sum();
        double variance = preVariance / (times.size() - 1.0);
        return Math.sqrt( variance );
    }


    protected double processDoubleValue( double value ) {
        DecimalFormat df = new DecimalFormat( "0.000" );
        double temp1 = value / 1_000_000;
        String roundFormat = df.format( temp1 );
        try {
            return df.parse( roundFormat ).doubleValue();
        } catch ( ParseException e ) {
            log.error( "Exception", e );
        }
        return -1;
    }


    public abstract int getNumberOfInsertThreads();

}
