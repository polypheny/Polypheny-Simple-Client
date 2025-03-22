/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/21/25, 2:02 PM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.scalingBench;

import java.util.Properties;
import org.polypheny.simpleclient.scenario.AbstractConfig;

public class LockingBenchConfig extends AbstractConfig {
    public final int sessionCount;
    public final int entityCount;
    public final double readWriteRatio;
    public final int namespaceCount;

    public int numberOfEntriesPerTable;
    public int maxBatchSize;
    public int progressReportBase;
    public int numberOfQueries;

    public LockingBenchConfig( Properties properties, int sessionCount, int namespaceCount, int entityCount, double readWriteRatio) {
        super("scalingBench", "polypheny-jdbc", properties);
        this.sessionCount = sessionCount;
        this.entityCount = entityCount;
        this.readWriteRatio = readWriteRatio;
        this.namespaceCount = namespaceCount;

        this.numberOfEntriesPerTable = getIntProperty( properties, "numberOfEntriesPerTable" );
        this.maxBatchSize = getIntProperty( properties, "maxBatchSize" );
        this.progressReportBase = getIntProperty( properties, "progressReportBase" );
        this.numberOfQueries = getIntProperty( properties, "numberOfQueries" );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return false;
    }

}
