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

package org.polypheny.simpleclient.scenario;

import com.google.common.base.Joiner;
import java.io.File;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.ChronosAgent;
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


    public abstract void createSchema( DatabaseInstance databaseInstance, boolean includingKeys );

    public abstract void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter );

    public abstract long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads );

    public abstract void warmUp( ProgressReporter progressReporter );

    public abstract void analyze( Properties properties, File outputDirectory );


    protected void calculateResults( Map<Integer, String> queryTypes, Properties properties, int templateId, List<Long> time ) {
        LongSummaryStatistics summaryStatistics = time.stream().mapToLong( Long::longValue ).summaryStatistics();
        double mean = summaryStatistics.getAverage();
        long max = summaryStatistics.getMax();
        long min = summaryStatistics.getMin();
        double stddev = calculateSampleStandardDeviation( time, mean );

        properties.put( "queryTypes_" + templateId + "_mean", processDoubleValue( mean ) );
        if ( ChronosAgent.STORE_INDIVIDUAL_QUERY_TIMES ) {
            properties.put( "queryTypes_" + templateId + "_all", Joiner.on( ',' ).join( time ) );
        }
        properties.put( "queryTypes_" + templateId + "_stddev", processDoubleValue( stddev ) );
        properties.put( "queryTypes_" + templateId + "_min", min / 1_000_000L );
        properties.put( "queryTypes_" + templateId + "_max", max / 1_000_000L );
        properties.put( "queryTypes_" + templateId + "_example", queryTypes.get( templateId ) );
    }


    protected double calculateMean( List<Long> times ) {
        DecimalFormat df = new DecimalFormat( "0.000" );
        OptionalDouble meanOptional = times.stream().mapToLong( Long::longValue ).average();
        if ( meanOptional.isPresent() ) {
            // scale
            double mean = meanOptional.getAsDouble() / 1000000.0;
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


    protected void commitAndCloseExecutor( Executor executor ) {
        if ( executor != null ) {
            try {
                executor.executeCommit();
            } catch ( ExecutorException e ) {
                try {
                    executor.executeRollback();
                } catch ( ExecutorException ex ) {
                    log.error( "Error while rollback connection", e );
                }
            }
            try {
                executor.closeConnection();
            } catch ( ExecutorException e ) {
                log.error( "Error while closing connection", e );
            }
        }
    }

}
