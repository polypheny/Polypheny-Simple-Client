/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2023 The Polypheny Project
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
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.main.ChronosAgent;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;


@Slf4j
public abstract class Scenario {

    @Getter
    protected final ExecutorFactory executorFactory;
    protected final boolean commitAfterEveryQuery;
    protected final boolean dumpQueryList;
    protected final QueryMode queryMode;


    protected Scenario( ExecutorFactory executorFactory, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        this.executorFactory = executorFactory;
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
        if ( !time.isEmpty() ) {
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
        } else {
            properties.put( "queryTypes_" + templateId + "_mean", 0 );
            properties.put( "queryTypes_" + templateId + "_stddev", 0 );
            properties.put( "queryTypes_" + templateId + "_min", 0 );
            properties.put( "queryTypes_" + templateId + "_max", 0 );
        }
        properties.put( "queryTypes_" + templateId + "_example", queryTypes.get( templateId ) );
    }


    protected double calculateMean( List<Long> times ) {
        OptionalDouble meanOptional = times.stream().mapToLong( Long::longValue ).average();
        if ( meanOptional.isPresent() ) {
            return Math.round( meanOptional.getAsDouble() / 1_000 ) / 1_000.0;
        }
        return -1;
    }


    protected double calculateSampleStandardDeviation( List<Long> times, double mean ) {
        double preVariance = times.stream().mapToDouble( it -> Math.pow( (it - mean), 2 ) ).sum();
        double variance = preVariance / (times.size() - 1.0);
        return Math.sqrt( variance );
    }


    protected double processDoubleValue( double value ) {
        return Math.round( value / 1_000 ) / 1_000.0;
    }


    public abstract int getNumberOfInsertThreads();


    protected static String findMatchingDataStoreName( String name ) {
        String match = null;

        for ( String item : PolyphenyDbExecutor.storeNames ) {
            if ( item.startsWith( name ) ) {
                if ( match != null ) {
                    throw new RuntimeException( "More than one matching data store found for " + name );
                }
                match = item;
            }
        }

        if ( match == null ) {
            throw new RuntimeException( "No matching data store found for " + name );
        }

        return match;
    }


    protected static void commitAndCloseExecutor( Executor executor ) {
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
