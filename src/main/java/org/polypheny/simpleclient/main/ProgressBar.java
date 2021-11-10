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


/**
 * Taken from: http://masterex.github.io/archive/2011/10/23/java-cli-progress-bar.html
 */
public class ProgressBar extends ProgressReporter {

    private StringBuilder progress;
    private volatile boolean finished;


    /**
     * initialize progress bar properties.
     */
    public ProgressBar( int numberOfThreads, int base ) {
        super( numberOfThreads, base );
        init();
    }


    /**
     * Called whenever the progress bar needs to be updated.
     * That is whenever progress was made.
     *
     * @param done an int representing the work done so far
     * @param total an int representing the total work
     */
    @Override
    public void update( int done, int total ) {
        char[] workchars = { '|', '/', '-', '\\' };
        String format = "\r%3d%% %s %c";

        int percent = (++done * 100) / total;
        int extrachars = (percent / 2) - this.progress.length();

        while ( extrachars-- > 0 ) {
            progress.append( '#' );
        }

        if ( !finished ) {
            System.out.printf( format, percent, progress, workchars[done % workchars.length] );

            if ( done == total ) {
                finished = true;
                System.out.flush();
                System.out.println();
                init();
            }
        }
    }


    private void init() {
        this.progress = new StringBuilder( 60 );
    }


    /**
     * @param progress Progress in thousandth
     */
    @Override
    protected void update( int progress ) {
        update( progress, base );
    }

}
