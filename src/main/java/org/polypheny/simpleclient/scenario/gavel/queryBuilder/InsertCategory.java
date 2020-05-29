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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import kong.unirest.HttpRequest;
import org.apache.commons.lang.StringEscapeUtils;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class InsertCategory extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;

    private final ArrayList<String> categories;
    private static AtomicInteger nextCategoryId = new AtomicInteger( 1 );


    public InsertCategory() {
        categories = new ArrayList<>();
        try ( InputStream is = ClassLoader.getSystemResourceAsStream( "gavel/categories.txt" ) ) {
            if ( is == null ) {
                throw new RuntimeException( "Categories list file not found!" );
            }
            try ( InputStreamReader in = new InputStreamReader( is ) ) {
                try ( Stream<String> stream = new BufferedReader( in ).lines() ) {
                    stream.forEach( categories::add );
                }
            }
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public Query getNewQuery() {
        if ( categories.size() == 0 ) {
            throw new RuntimeException( "List of categories is empty" );
        }
        return new InsertCategoryQuery(
                nextCategoryId.getAndIncrement(),
                categories.remove( 0 )
        );
    }


    private static class InsertCategoryQuery extends Query {

        private final int categoryId;
        private final String category;


        public InsertCategoryQuery( int categoryId, String category ) {
            super( EXPECT_RESULT );
            this.categoryId = categoryId;
            this.category = category;
        }


        @Override
        public String getSql() {
            return "INSERT INTO category(id, name) VALUES ("
                    + categoryId + ","
                    + "'" + StringEscapeUtils.escapeSql( category ) + "'"
                    + ")";
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }
    }
}
