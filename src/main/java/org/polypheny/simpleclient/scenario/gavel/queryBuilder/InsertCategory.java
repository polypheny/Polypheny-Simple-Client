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
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.stream.Stream;
import org.apache.commons.lang.StringEscapeUtils;
import org.polypheny.simpleclient.main.QueryBuilder;


public class InsertCategory extends QueryBuilder {

    private final ArrayList<String> categories;
    private static int nextId = 1;


    public InsertCategory() {
        super( false );
        categories = new ArrayList<>();
        InputStreamReader in = new InputStreamReader( ClassLoader.getSystemResourceAsStream( "gavel/categories.txt" ) );
        try ( Stream<String> stream = new BufferedReader( in ).lines() ) {
            stream.forEach( categories::add );
        }
    }


    @Override
    public String generateSql() {
        if ( categories.size() == 0 ) {
            throw new RuntimeException( "List of categories is empty" );
        }
        StringBuilder sb = new StringBuilder();
        sb.append( "INSERT INTO category(id, name) VALUES (" );
        sb.append( nextId++ ).append( "," );
        sb.append( "'" ).append( StringEscapeUtils.escapeSql( categories.remove( 0 ) ) ).append( "'" );
        sb.append( ")" );
        return sb.toString();
    }

}
