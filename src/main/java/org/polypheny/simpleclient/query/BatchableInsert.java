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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;


public abstract class BatchableInsert extends Query {

    public BatchableInsert( boolean expectResult ) {
        super( expectResult );
    }


    public abstract String getSqlRowExpression();

    public abstract JsonObject getRestRowExpression();


    public List<String> getRowValues() {
        return null;
    }


    public String getMongoQlRowExpression() {
        List<String> fields = new ArrayList<>();
        for ( Entry<String, JsonElement> entry : getRestRowExpression().entrySet() ) {
            String[] splits = entry.getKey().split( "\\." );
            fields.add( "\"" + splits[splits.length - 1] + "\"" + ":" + maybeQuote( entry.getValue() ) );
        }
        return "{" + String.join( ",", fields ) + "}";
    }


    protected String maybeQuote( JsonElement value ) {
        if ( value.isJsonPrimitive() && value.getAsJsonPrimitive().isString() ) {
            return "\"" + value.getAsString() + "\"";
        } else {
            return value.getAsString();
        }
    }


    public String getCypherRowExpression() {
        return null;
    }


    public abstract String getEntity();

}
