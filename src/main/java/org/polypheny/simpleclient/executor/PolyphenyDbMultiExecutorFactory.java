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

package org.polypheny.simpleclient.executor;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbCypherExecutor.PolyphenyDbCypherExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbMongoQlExecutor.PolyphenyDbMongoQlExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbRestExecutor.PolyphenyDbRestExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;


@Slf4j
public class PolyphenyDbMultiExecutorFactory extends ExecutorFactory {


    private final String host;

    @Getter
    private final PolyphenyDbJdbcExecutorFactory jdbcExecutorFactory;
    @Getter
    private final PolyphenyDbMongoQlExecutorFactory mongoQlExecutorFactory;
    @Getter
    private final PolyphenyDbCypherExecutorFactory cypherExecutorFactory;
    @Getter
    private final PolyphenyDbRestExecutorFactory restExecutorFactory;


    public PolyphenyDbMultiExecutorFactory( String host ) {
        this.host = host;
        jdbcExecutorFactory = new PolyphenyDbJdbcExecutorFactory( host, false );
        mongoQlExecutorFactory = new PolyphenyDbMongoQlExecutorFactory( host );
        cypherExecutorFactory = new PolyphenyDbCypherExecutorFactory( host );
        restExecutorFactory = new PolyphenyDbRestExecutorFactory( host );
    }


    @Override
    public Executor createExecutorInstance( CsvWriter csvWriter ) {
        return jdbcExecutorFactory.createExecutorInstance( csvWriter );
    }


    @Override
    public int getMaxNumberOfThreads() {
        return 0;
    }

}
