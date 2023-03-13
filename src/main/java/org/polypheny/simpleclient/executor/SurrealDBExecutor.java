/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/13/23, 5:13 PM The Polypheny Project
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

package org.polypheny.simpleclient.executor;

import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;

public class SurrealDBExecutor {

    public static class SurrealDBExecutorFactory extends ExecutorFactory {

        public SurrealDBExecutorFactory( String hostname, boolean prepareStatements ) {

        }


        @Override
        public Executor createExecutorInstance( CsvWriter csvWriter ) {
            return null;
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }

}
