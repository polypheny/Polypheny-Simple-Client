/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/29/23, 11:38 PM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.coms;

import lombok.Getter;

public enum ComsType {
    RELATIONAL( 0 ),
    GRAPH( 1 ),
    DOCUMENT( 2 ),
    MODIFY( 3 ),
    QUERY( 4 ),
    ADD_LOGIN( 5 ),
    COMPLEX_LOGIN_1( 6 ),
    COMPLEX_LOGIN_2( 7 ),
    ADD_DEVICE( 8 ),
    REMOVE_DEVICE( 9 ),
    CHANGE_DEVICE( 10 ),
    CHANGE_USER( 11 );
    @Getter
    private final int i;


    ComsType( int i ) {
        this.i = i;
    }
}
