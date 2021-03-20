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

    public final String dataStore;
    public final String multimediaStore;
    public final int numberOfUsers;
    public final int postsPerUser;
    public final int numberOfFriends;
    public final int albumSize;
    public final int imgSize;
    public final int numberOfFrames;
    public final int fileSizeKB;
    public final int numberOfSelectUserQueries;
    public final int numberOfSelectProfilePicQueries;
    public final int numberOfSelectProfilePicsQueries;
    public final int numberOfSelectMediaQueries;
    public final int numberOfSelectTimelineQueries;
    public final int numberOfDeleteTimelineQueries;
    public final int numberOfInsertTimelineQueries;


    public final int maxBatchSize;


    public MultimediaConfig( Properties properties, int multiplier ) {
        super( "multimedia", "polypheny-rest" );

        pdbBranch = null;
        puiBranch = null;
        buildUi = false;
        resetCatalog = false;
        memoryCatalog = false;

        router = "icarus";
        planAndImplementationCaching = "Both";

        progressReportBase = getIntProperty( properties, "progressReportBase" );
        numberOfThreads = getIntProperty( properties, "numberOfThreads" );
        numberOfWarmUpIterations = getIntProperty( properties, "numberOfWarmUpIterations" );

        dataStore = getStringProperty( properties, "dataStore" );
        multimediaStore = getStringProperty( properties, "multimediaStore" );
        dataStores.add( dataStore );
        if ( !multimediaStore.equals( "same" ) ) {
            dataStores.add( multimediaStore );
        }
        numberOfUsers = getIntProperty( properties, "numberOfUsers" ) * multiplier;
        albumSize = getIntProperty( properties, "albumSize" );
        postsPerUser = getIntProperty( properties, "postsPerUser" );
        numberOfFriends = getIntProperty( properties, "numberOfFriends" );
        numberOfSelectUserQueries = getIntProperty( properties, "numberOfSelectUserQueries" );
        numberOfSelectProfilePicQueries = getIntProperty( properties, "numberOfSelectProfilePicQueries" );
        numberOfSelectProfilePicsQueries = getIntProperty( properties, "numberOfSelectProfilePicsQueries" );
        numberOfSelectMediaQueries = getIntProperty( properties, "numberOfSelectMediaQueries" );
        numberOfSelectTimelineQueries = getIntProperty( properties, "numberOfSelectTimelineQueries" );
        numberOfDeleteTimelineQueries = getIntProperty( properties, "numberOfDeleteTimelineQueries" );
        numberOfInsertTimelineQueries = getIntProperty( properties, "numberOfInsertTimelineQueries" );

        imgSize = getIntProperty( properties, "imgSize" );
        numberOfFrames = getIntProperty( properties, "numberOfFrames" );
        fileSizeKB = getIntProperty( properties, "fileSizeKB" );
        maxBatchSize = getIntProperty( properties, "maxBatchSize" );
    }


    public MultimediaConfig( Map<String, String> cdl ) {
        super( "multimedia", cdl.get( "store" ) );

        pdbBranch = cdl.get( "pdbBranch" );
        puiBranch = cdl.get( "puiBranch" );
        buildUi = Boolean.parseBoolean( cdl.getOrDefault( "buildUi", "false" ) );
        resetCatalog = Boolean.parseBoolean( cdl.get( "resetCatalog" ) );
        memoryCatalog = Boolean.parseBoolean( cdl.get( "memoryCatalog" ) );

        router = cdl.get( "router" );
        planAndImplementationCaching = cdl.getOrDefault( "planAndImplementationCaching", "Both" );

        progressReportBase = 100;
        numberOfThreads = Integer.parseInt( cdl.get( "numberOfThreads" ) );
        numberOfWarmUpIterations = Integer.parseInt( cdl.get( "numberOfWarmUpIterations" ) );

        dataStore = cdl.get( "dataStore" );
        multimediaStore = cdl.get( "multimediaStore" );
        dataStores.add( dataStore );
        if ( !multimediaStore.equals( "same" ) ) {
            dataStores.add( multimediaStore );
        }
        numberOfUsers = Integer.parseInt( cdl.get( "numberOfUsers" ) );
        albumSize = Integer.parseInt( cdl.get( "albumSize" ) );
        postsPerUser = Integer.parseInt( cdl.get( "postsPerUser" ) );
        numberOfFriends = Integer.parseInt( cdl.get( "numberOfFriends" ) );
        numberOfSelectUserQueries = Integer.parseInt( cdl.get( "numberOfSelectUserQueries" ) );
        numberOfSelectProfilePicQueries = Integer.parseInt( cdl.get( "numberOfSelectProfilePicQueries" ) );
        numberOfSelectProfilePicsQueries = Integer.parseInt( cdl.get( "numberOfSelectProfilePicsQueries" ) );
        numberOfSelectMediaQueries = Integer.parseInt( cdl.get( "numberOfSelectMediaQueries" ) );
        numberOfSelectTimelineQueries = Integer.parseInt( cdl.get( "numberOfSelectTimelineQueries" ) );
        numberOfDeleteTimelineQueries = Integer.parseInt( cdl.get( "numberOfDeleteTimelineQueries" ) );
        numberOfInsertTimelineQueries = Integer.parseInt( cdl.get( "numberOfInsertTimelineQueries" ) );

        imgSize = Integer.parseInt( cdl.get( "imgSize" ) );
        numberOfFrames = Integer.parseInt( cdl.get( "numberOfFrames" ) );
        fileSizeKB = Integer.parseInt( cdl.get( "fileSizeKB" ) );
        maxBatchSize = Integer.parseInt( cdl.get( "maxBatchSize" ) );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return true;
    }

}
