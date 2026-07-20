/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2026 The Polypheny Project
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

package org.polypheny.simpleclient.scenario.vectorbench;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.JdbcExecutor;
import org.polypheny.simpleclient.query.QueryBuilder;


/**
 * Measures recall@k for an approximate vector index.
 *
 * <ol>
 *     <li>{@link #captureGroundTruth()} is run while no vector index exists, so the regular query path
 *         performs an exact nearest-neighbor search. The true top-k ids of every query are
 *         written to the ground-truth file.</li>
 *     <li>The index is created.</li>
 *     <li>{@link #evaluate()} reruns the exact same queries, now answered approximately via the
 *         index, and computes recall = |returned \intersect ground_truth| / k averaged over all queries.</li>
 * </ol>
 *
 * <p>Determinism relies on the supplied {@link QueryBuilder} being freshly seeded with the same query seed
 * in both phases, so {@code getNewQuery()} yields the identical sequence of query vectors.
 */
@Slf4j
public class RecallEvaluator {

    public static final String DEFAULT_GROUND_TRUTH_FILE = "recall-groundtruth.csv";

    private final VectorBenchConfig config;
    private final JdbcExecutor executor;
    private final QueryBuilder knnBuilder;
    private final String groundTruthFile;


    public RecallEvaluator( VectorBenchConfig config, JdbcExecutor executor, QueryBuilder knnBuilder, String groundTruthFile ) {
        this.config = config;
        this.executor = executor;
        this.knnBuilder = knnBuilder;
        this.groundTruthFile = groundTruthFile;
    }


    /**
     * Runs the query set against the table and records the exact top-k ids of each query, returning
     * the result in memory.
     */
    public List<Set<Long>> captureGroundTruthInMemory() {
        int n = config.numberOfRecallQueries;
        List<Set<Long>> groundTruth = new ArrayList<>( n );
        try {
            for ( int i = 0; i < n; i++ ) {
                groundTruth.add( new HashSet<>( executor.executeQueryAndGetIds( knnBuilder.getNewQuery() ) ) );
                if ( ( i + 1 ) % 100 == 0 ) {
                    log.info( "Ground truth progress: {}/{}", i + 1, n );
                }
            }
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while capturing ground truth", e );
        }
        return groundTruth;
    }


    /**
     * Runs the query set against the table and records the exact top-k ids of each query to the
     * ground-truth file (used by the standalone CLI tasks).
     */
    public void captureGroundTruth() {
        List<Set<Long>> groundTruth = captureGroundTruthInMemory();
        try ( BufferedWriter writer = new BufferedWriter( new FileWriter( groundTruthFile ) ) ) {
            for ( Set<Long> ids : groundTruth ) {
                writer.write( ids.stream().map( String::valueOf ).collect( Collectors.joining( "," ) ) );
                writer.newLine();
            }
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not write ground-truth file " + groundTruthFile, e );
        }
        log.info( "Ground truth written to {}", groundTruthFile );
    }


    /**
     * Reruns the query set against the indexed table and compares each result to the stored ground truth.
     *
     * @return the mean recall@k over all queries.
     */
    public double evaluate() {
        return evaluate( readGroundTruth() );
    }


    /**
     * Reruns the query set against the indexed table and compares each result to the supplied ground truth.
     *
     * @return the mean recall@k over all queries.
     */
    public double evaluate( List<Set<Long>> groundTruth ) {
        int n = Math.min( groundTruth.size(), config.numberOfRecallQueries );
        log.info( "Evaluating recall@{} over {} queries...", config.limitKnnQueries, n );

        double recallSum = 0.0;
        int counted = 0;
        try {
            for ( int i = 0; i < n; i++ ) {
                Set<Long> expected = groundTruth.get( i );
                List<Long> returned = executor.executeQueryAndGetIds( knnBuilder.getNewQuery() );
                if ( expected.isEmpty() ) {
                    continue;
                }
                long hits = returned.stream().filter( expected::contains ).distinct().count();
                recallSum += (double) hits / expected.size();
                counted++;
                if ( counted % 100 == 0 ) {
                    log.info( "Recall progress: {}/{}", counted, n );
                }
            }
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while evaluating recall", e );
        }

        double meanRecall = counted == 0 ? 0.0 : recallSum / counted;
        log.info( "Mean recall@{} over {} queries: {}", config.limitKnnQueries, counted, meanRecall );
        return meanRecall;
    }


    private List<Set<Long>> readGroundTruth() {
        List<Set<Long>> groundTruth = new ArrayList<>();
        try ( BufferedReader reader = new BufferedReader( new FileReader( groundTruthFile ) ) ) {
            String line;
            while ( ( line = reader.readLine() ) != null ) {
                Set<Long> ids = new HashSet<>();
                if ( !line.isBlank() ) {
                    for ( String part : line.split( "," ) ) {
                        ids.add( Long.parseLong( part.trim() ) );
                    }
                }
                groundTruth.add( ids );
            }
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not read ground-truth file " + groundTruthFile
                    + ". Run the 'groundtruth' task (before creating the index) first.", e );
        }
        return groundTruth;
    }

}
