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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import kong.unirest.HttpRequest;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;


public class RawQuery extends Query {

    @Getter
    private final String sql;

    @Getter
    private final HttpRequest<?> rest;

    @Getter
    private final String mongoQl;

    @Getter
    private final String cypher;

    @Getter
    private final String surrealQl;

    @Getter
    @Builder.Default
    private List<String> types = Collections.emptyList();


    public RawQuery( String sql, HttpRequest<?> rest, boolean expectResultSet ) {
        this( sql, rest, null, null, null, Collections.emptyList(), expectResultSet );
    }


    @Builder
    public RawQuery( String sql, HttpRequest<?> rest, String mongoQl, String cypher, String surrealQl, List<String> types, boolean expectResultSet ) {
        super( expectResultSet );
        this.sql = sql;
        this.rest = rest;
        this.mongoQl = mongoQl;
        this.cypher = cypher;
        this.surrealQl = surrealQl;
        this.types = types == null ? Collections.emptyList() : types;
    }


    @Override
    public String getParameterizedSqlQuery() {
        return null;
    }


    @Override
    public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
        return null;
    }

}
