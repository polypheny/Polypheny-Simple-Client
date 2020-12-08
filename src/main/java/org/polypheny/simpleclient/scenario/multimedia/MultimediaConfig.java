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

package org.polypheny.simpleclient.scenario.multimedia;


import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.scenario.AbstractConfig;


@Slf4j
public class MultimediaConfig extends AbstractConfig {

    public final int numberOfUsers;
    public final int albumSize;
    public final int postsPerUser;
    public final int numberOfFriends;
    public final int read;
    public final int write;

    public final int minImgSize;
    public final int maxImgSize;
    public final int numberOfFrames;
    public final int minFileSizeKB;
    public final int maxFileSizeKB;
    public final int maxBatchSize;


    public MultimediaConfig( Properties properties, int multiplier ) {
        super( "multimedia", "polypheny" );

        pdbBranch = null;
        puiBranch = null;
        resetCatalog = false;
        memoryCatalog = false;

        router = "icarus";
        planAndImplementationCaching = "Both";
        dataStores.add( "file" );

        progressReportBase = getIntProperty( properties, "progressReportBase" );
        numberOfThreads = getIntProperty( properties, "numberOfThreads" );
        numberOfWarmUpIterations = getIntProperty( properties, "numberOfWarmUpIterations" );

        numberOfUsers = getIntProperty( properties, "numberOfUsers" ) * multiplier;
        albumSize = getIntProperty( properties, "albumSize" );
        postsPerUser = getIntProperty( properties, "postsPerUser" );
        numberOfFriends = getIntProperty( properties, "numberOfFriends" );
        read = getIntProperty( properties, "read" );
        write = getIntProperty( properties, "write" );

        minImgSize = getIntProperty( properties, "minImgSize" );
        maxImgSize = getIntProperty( properties, "maxImgSize" );
        numberOfFrames = getIntProperty( properties, "numberOfFrames" );
        minFileSizeKB = getIntProperty( properties, "minFileSizeKB" );
        maxFileSizeKB = getIntProperty( properties, "maxFileSizeKB" );
        maxBatchSize = getIntProperty( properties, "maxBatchSize" );
    }


    public MultimediaConfig( Map<String, String> cdl ) {
        super( "multimedia", cdl.get( "store" ) );

        pdbBranch = cdl.get( "pdbBranch" );
        puiBranch = cdl.get( "puiBranch" );
        resetCatalog = Boolean.parseBoolean( cdl.get( "resetCatalog" ) );
        memoryCatalog = Boolean.parseBoolean( cdl.get( "memoryCatalog" ) );

        dataStores.add( cdl.get( "dataStore" ) );
        router = cdl.get( "router" );
        planAndImplementationCaching = cdl.getOrDefault( "planAndImplementationCaching", "Both" );

        progressReportBase = 100;
        numberOfThreads = Integer.parseInt( cdl.get( "numberOfThreads" ) );
        numberOfWarmUpIterations = Integer.parseInt( cdl.get( "numberOfWarmUpIterations" ) );

        numberOfUsers = Integer.parseInt( cdl.get( "numberOfUsers" ) );
        albumSize = Integer.parseInt( cdl.get( "albumSize" ) );
        postsPerUser = Integer.parseInt( cdl.get( "postsPerUser" ) );
        numberOfFriends = Integer.parseInt( cdl.get( "numberOfFriends" ) );
        read = Integer.parseInt( cdl.get( "read" ) );
        write = Integer.parseInt( cdl.get( "write" ) );

        minImgSize = Integer.parseInt( cdl.get( "minImgSize" ) );
        maxImgSize = Integer.parseInt( cdl.get( "maxImgSize" ) );
        numberOfFrames = Integer.parseInt( cdl.get( "numberOfFrames" ) );
        minFileSizeKB = Integer.parseInt( cdl.get( "minFileSizeKB" ) );
        maxFileSizeKB = Integer.parseInt( cdl.get( "maxFileSizeKB" ) );
        maxBatchSize = Integer.parseInt( cdl.get( "maxBatchSize" ) );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return true;
    }

}
