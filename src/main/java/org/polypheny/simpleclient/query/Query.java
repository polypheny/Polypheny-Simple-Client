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

package org.polypheny.simpleclient.query;


import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.Map;
import kong.unirest.HttpRequest;
import kong.unirest.RequestBodyEntity;
import kong.unirest.Unirest;
import lombok.Getter;


public abstract class Query {

    @Getter
    private final boolean expectResultSet;


    public Query( boolean expectResultSet ) {
        this.expectResultSet = expectResultSet;
    }


    public abstract String getSql();


    public abstract HttpRequest<?> getRest();


    // TODO JS: Correct return type and implement in all knnBench query builders
    public Object getCottontail() {
        return null;
    }


    public static HttpRequest<?> buildRestInsert( String table, List<JsonObject> rows ) {
        JsonArray array = new JsonArray();
        rows.forEach( array::add );
        JsonObject data = new JsonObject();
        data.add( "data", array );

        return Unirest.post( "{protocol}://{host}:{port}/restapi/v1/res/" + table )
                .header( "Content-Type", "application/json" )
                .body( data );
    }


    public static HttpRequest<?> buildRestUpdate( String table, JsonObject set, Map<String, String> where ) {
        JsonArray array = new JsonArray();
        array.add( set );
        JsonObject data = new JsonObject();
        data.add( "data", array );

        RequestBodyEntity request = Unirest.patch( "{protocol}://{host}:{port}/restapi/v1/res/" + table )
                .header( "Content-Type", "application/json" )
                .body( data );

        for ( Map.Entry<String, String> entry : where.entrySet() ) {
            request.queryString( entry.getKey(), entry.getValue() );
        }

        return request;
    }

}
