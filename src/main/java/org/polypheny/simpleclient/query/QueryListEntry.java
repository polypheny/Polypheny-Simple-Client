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

package org.polypheny.simpleclient.query;


import lombok.Getter;
import org.polypheny.simpleclient.scenario.gavelEx.GavelEx.QueryLanguage;

public class QueryListEntry {

    public final Query query;
    public final int templateId;
    public final long delay;
    @Getter
    public final QueryLanguage queryLanguage;


    public QueryListEntry( Query query, int templateId ) {
        this.query = query;
        this.templateId = templateId;
        this.delay = 0;
        this.queryLanguage = QueryLanguage.SQL;
    }


    public QueryListEntry( Query query, int templateId, int delay, QueryLanguage queryLanguage ) {
        this.query = query;
        this.templateId = templateId;
        this.delay = delay * 1000L;
        this.queryLanguage = queryLanguage;
    }


}
