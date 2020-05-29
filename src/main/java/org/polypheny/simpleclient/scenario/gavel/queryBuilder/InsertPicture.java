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

package org.polypheny.simpleclient.scenario.gavel.queryBuilder;


import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.HttpRequest;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertPicture extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;
    private final static String[] IMAGE_TYPES = { ".tif", ".tiff", ".gif", ".jpeg", ".jpg", ".jif", ".jfif", ".jp2", ".jpx", ".j2k", ".j2c", ".png", ".bmp" };

    private final int auctionId;


    public InsertPicture( int auctionId ) {
        this.auctionId = auctionId;
    }


    @Override
    public Query getNewQuery() {
        return new InsertPictureQuery(
                auctionId,
                UUID.randomUUID().toString(),
                ThreadLocalRandom.current().nextInt( 100, 2048 ),
                IMAGE_TYPES[ThreadLocalRandom.current().nextInt( 0, IMAGE_TYPES.length )]
        );
    }


    private static class InsertPictureQuery extends Query {

        private final int auctionId;
        private final String fileName;
        private final int size;
        private final String fileType;


        public InsertPictureQuery( int auctionId, String fileName, int size, String fileType ) {
            super( EXPECT_RESULT );
            this.auctionId = auctionId;
            this.fileName = fileName;
            this.size = size;
            this.fileType = fileType;
        }


        @Override
        public String getSql() {
            return "INSERT INTO picture(filename, type, size, auction) VALUES ("
                    + "'" + fileName + "',"
                    + "'" + fileType + "',"
                    + size + ","
                    + auctionId
                    + ")";
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }
    }
}
