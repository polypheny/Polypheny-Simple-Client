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

package org.polypheny.simpleclient.scenario.knnbench;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.scenario.AbstractConfig;


@Slf4j
public class KnnBenchConfig extends AbstractConfig {

    public String dataStoreFeature;
    public String dataStoreMetadata;

    public long randomSeedInsert;
    public long randomSeedQuery;

    public int dimensionFeatureVectors;
    public int batchSizeInserts;
    public int batchSizeQueries;

    public int numberOfEntries;
    public int numberOfSimpleKnnIntFeatureQueries;
    public int numberOfSimpleKnnRealFeatureQueries;
    public int numberOfSimpleMetadataQueries;
    public int numberOfSimpleKnnIdIntFeatureQueries;
    public int numberOfSimpleKnnIdRealFeatureQueries;
    public int numberOfMetadataKnnIntFeatureQueries;
    public int numberOfMetadataKnnRealFeatureQueries;
//    public final int numberOfCombinedQueries;

    public int limitKnnQueries;
    public String distanceNorm;


    public KnnBenchConfig( Properties properties, int multiplier ) {
        super( "knnBench", "polypheny-jdbc", properties );

        dataStoreFeature = null;
        dataStoreMetadata = null;
        //dataStores.add( "cottontail" );

        if ( getBooleanProperty( properties, "useRandomSeeds" ) ) {
            Random tempRand = new Random();
            randomSeedInsert = tempRand.nextLong();
            randomSeedQuery = tempRand.nextLong();
        } else {
            randomSeedInsert = getLongProperty( properties, "randomSeedInsert" );
            randomSeedQuery = getLongProperty( properties, "randomSeedQuery" );
        }

        dimensionFeatureVectors = getIntProperty( properties, "dimensionFeatureVectors" );
        batchSizeInserts = getIntProperty( properties, "batchSizeInserts" );
        numberOfEntries = getIntProperty( properties, "numberOfEntries" ) * multiplier;

        batchSizeQueries = getIntProperty( properties, "batchSizeQueries" );
        numberOfSimpleKnnIntFeatureQueries = getIntProperty( properties, "numberOfSimpleKnnIntFeatureQueries" ) * multiplier;
        numberOfSimpleKnnRealFeatureQueries = getIntProperty( properties, "numberOfSimpleKnnRealFeatureQueries" ) * multiplier;
        numberOfSimpleMetadataQueries = getIntProperty( properties, "numberOfSimpleMetadataQueries" ) * multiplier;
        numberOfSimpleKnnIdIntFeatureQueries = getIntProperty( properties, "numberOfSimpleKnnIdIntFeatureQueries" ) * multiplier;
        numberOfSimpleKnnIdRealFeatureQueries = getIntProperty( properties, "numberOfSimpleKnnIdRealFeatureQueries" ) * multiplier;
        numberOfMetadataKnnIntFeatureQueries = getIntProperty( properties, "numberOfMetadataKnnIntFeatureQueries" ) * multiplier;
        numberOfMetadataKnnRealFeatureQueries = getIntProperty( properties, "numberOfMetadataKnnRealFeatureQueries" ) * multiplier;
        limitKnnQueries = getIntProperty( properties, "limitKnnQueries" );
        distanceNorm = getStringProperty( properties, "distanceNorm" );
    }


    public KnnBenchConfig( Map<String, String> cdl ) {
        super( "gavel", cdl.get( "store" ), cdl );

        dataStoreFeature = cdl.get( "dataStoreFeature" );
        dataStoreMetadata = cdl.get( "dataStoreMetadata" );
        if ( dataStoreFeature.equals( dataStoreMetadata ) ) {
            dataStores.add( dataStoreFeature );
        } else {
            dataStores.add( dataStoreFeature );
            dataStores.add( dataStoreMetadata );
        }

        if ( Boolean.parseBoolean( cdl.get( "useRandomSeeds" ) ) ) {
            Random tempRand = new Random();
            randomSeedInsert = tempRand.nextLong();
            randomSeedQuery = tempRand.nextLong();
        } else {
            randomSeedInsert = Long.parseLong( cdl.get( "randomSeedInsert" ) );
            randomSeedQuery = Long.parseLong( cdl.get( "randomSeedQuery" ) );
        }

        dimensionFeatureVectors = Integer.parseInt( cdl.get( "dimensionFeatureVectors" ) );
        batchSizeInserts = Integer.parseInt( cdl.get( "batchSizeInserts" ) );
        numberOfEntries = Integer.parseInt( cdl.get( "numberOfEntries" ) );

        batchSizeQueries = Integer.parseInt( cdl.get( "batchSizeQueries" ) );
        numberOfSimpleKnnIntFeatureQueries = Integer.parseInt( cdl.get( "numberOfSimpleKnnIntFeatureQueries" ) );
        numberOfSimpleKnnRealFeatureQueries = Integer.parseInt( cdl.get( "numberOfSimpleKnnRealFeatureQueries" ) );
        numberOfSimpleMetadataQueries = Integer.parseInt( cdl.get( "numberOfSimpleMetadataQueries" ) );
        numberOfSimpleKnnIdIntFeatureQueries = Integer.parseInt( cdl.get( "numberOfSimpleKnnIdIntFeatureQueries" ) );
        numberOfSimpleKnnIdRealFeatureQueries = Integer.parseInt( cdl.get( "numberOfSimpleKnnIdRealFeatureQueries" ) );
        numberOfMetadataKnnIntFeatureQueries = Integer.parseInt( cdl.get( "numberOfMetadataKnnIntFeatureQueries" ) );
        numberOfMetadataKnnRealFeatureQueries = Integer.parseInt( cdl.get( "numberOfMetadataKnnRealFeatureQueries" ) );
//        numberOfCombinedQueries = getIntProperty( properties, "numberOfCombinedQueries" ) * multiplier;
        limitKnnQueries = Integer.parseInt( cdl.get( "limitKnnQueries" ) );
        distanceNorm = cdl.get( "distanceNorm" ).trim();
    }


    // For MultiBench
    protected KnnBenchConfig( String scenario, String system, Map<String, String> cdl ) {
        super( scenario, system, cdl );
    }


    // For MultiBench
    protected KnnBenchConfig( String scenario, String system, Properties properties ) {
        super( scenario, system, properties );
    }


    @Override
    public boolean usePreparedBatchForDataInsertion() {
        return true;
    }

}
