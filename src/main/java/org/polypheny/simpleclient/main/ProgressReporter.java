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

package org.polypheny.simpleclient.main;


import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.query.QueryListEntry;


@Slf4j
public abstract class ProgressReporter {

    private final AtomicLong progress;
    private final int numberOfThreads;
    public final int base;


    ProgressReporter( int numberOfThreads, int base ) {
        progress = new AtomicLong();
        this.numberOfThreads = numberOfThreads;
        this.base = base;
    }


    public void updateProgress() {
        double p = ((double) progress.incrementAndGet() / (double) (numberOfThreads));
        update( (int) p );
    }


    protected abstract void update( int progress );


    protected void update( int done, int totalNumber ) {
        if ( totalNumber == 0 ) { // Avoid / by zero
            update( 0 );
        } else {
            float denominator = totalNumber / 100.0f;
            float progress = done / denominator;
            update( Math.round( progress ) );
        }
    }


    public static class ReportQueryListProgress implements Runnable {

        private final int totalNumber;
        private final ProgressReporter theProgressReporter;
        private final List<QueryListEntry> theList;


        public ReportQueryListProgress( List<QueryListEntry> list, ProgressReporter progressReporter ) {
            this.totalNumber = list.size();
            this.theList = list;
            this.theProgressReporter = progressReporter;
        }


        @Override
        public void run() {
            while ( true ) {
                theProgressReporter.update( totalNumber - theList.size(), totalNumber );
                if ( theList.size() == 0 ) {
                    break;
                }
                try {
                    Thread.sleep( 1000 );
                } catch ( InterruptedException e ) {
                    throw new RuntimeException( "Unexpected interrupt", e );
                }
            }
        }
    }

}
