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

package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class SimpleMetadata extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;

    private final long randomSeed;
    private final int numOfEntries;

    private final Random random;


    public SimpleMetadata( long randomSeed, int numOfEntries ) {
        this.randomSeed = randomSeed;
        this.numOfEntries = numOfEntries;

        this.random = new Random( randomSeed );
    }


    private int getRandomId() {
        return this.random.nextInt( this.numOfEntries );
    }


    @Override
    public Query getNewQuery() {
        return new SimpleMetadataQuery( this.getRandomId() );
    }


    private static class SimpleMetadataQuery extends Query {

        private static final String SQL = "SELECT id, textdata FROM knn_metadata WHERE id = ";

        private final int id;


        private SimpleMetadataQuery( int id ) {
            super( EXPECT_RESULT );
            this.id = id;
        }


        @Override
        public String getSql() {
            return SQL + id;
        }


        @Override
        public String getParameterizedSqlQuery() {
            return SQL + "?";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.INTEGER, id ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            throw new UnsupportedOperationException( "kNN benchmarking is not supported for the REST interface." );
        }


        @Override
        public String getMongoQl() {
            throw new UnsupportedOperationException( "kNN benchmarking is not supported for the MongoQl interface." );
        }


        @Override
        public String getCypher() {
            throw new UnsupportedOperationException( "kNN benchmarking is not supported for the Cypher interface." );
        }

    }

}
