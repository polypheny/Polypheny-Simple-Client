/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-13.06.22, 15:47 The Polypheny Project
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

package org.polypheny.simpleclient.scenario.docbench;

import com.google.gson.JsonObject;
import java.util.Map;
import kong.unirest.HttpRequest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;


@Slf4j
public class MongoQlInsertQuery extends BatchableInsert {

    @Getter
    private final String mongoQl;
    private final String collection;
    private final String expression;


    public MongoQlInsertQuery( String collection, String expression, boolean expectResult ) {
        super( expectResult );
        this.mongoQl = "db." + collection + ".insert(" + expression + ")";
        this.collection = collection;
        this.expression = expression;
    }


    @Override
    public String getSqlRowExpression() {
        throw new UnsupportedOperationException();
    }


    @Override
    public String getParameterizedSqlQuery() {
        throw new UnsupportedOperationException();
    }


    @Override
    public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
        throw new UnsupportedOperationException();
    }


    @Override
    public JsonObject getRestRowExpression() {
        throw new UnsupportedOperationException();
    }


    @Override
    public String getMongoQlRowExpression() {
        return expression;
    }


    @Override
    public String getEntity() {
        return collection;
    }


    @Override
    public String getSql() {
        throw new UnsupportedOperationException();
    }


    @Override
    public HttpRequest<?> getRest() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void debug() {
        log.debug( mongoQl );
    }

}