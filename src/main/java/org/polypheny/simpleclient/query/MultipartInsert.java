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

package org.polypheny.simpleclient.query;

import com.google.gson.JsonArray;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import kong.unirest.core.HttpRequest;
import kong.unirest.core.MultipartBody;
import kong.unirest.core.Unirest;


public abstract class MultipartInsert extends BatchableInsert {

    Map<String, File> files = new HashMap<>();

    public MultipartInsert( boolean expectResult ) {
        super( expectResult );
    }

    public void setFile( String column, File f ) {
        files.put( column, f );
    }

    public abstract Map<String, String> getRestParameters();

    public HttpRequest<?> buildMultipartInsert() {
        MultipartBody body = Unirest.post( "{protocol}://{host}:{port}/restapi/v1/multipart" ).multiPartContent();
        body.field( "resName", getEntity() );
        if ( getRestRowExpression() != null ) {
            JsonArray jsonArray = new JsonArray();
            jsonArray.add( getRestRowExpression() );
            body.field( "data", jsonArray.toString() );
        }
        if ( getRestParameters() != null ) {
            for ( Entry<String, String> entry : getRestParameters().entrySet() ) {
                body.field( entry.getKey(), entry.getValue() );
            }
        }
        for ( Entry<String, File> entry : files.entrySet() ) {
            body.field( entry.getKey(), entry.getValue() );
        }
        return body;
    }

    public void cleanup() {
        files.forEach( ( key, file ) -> file.delete() );
    }

}
