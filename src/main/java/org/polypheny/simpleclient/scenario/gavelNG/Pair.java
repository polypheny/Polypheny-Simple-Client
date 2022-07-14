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
 *
 */

package org.polypheny.simpleclient.scenario.gavelNG;

import java.util.Map;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

public class Pair<T1, T2> implements Comparable<Pair<T1, T2>>, Map.Entry<T1, T2> {


    public final T1 left;
    public final T2 right;


    /**
     * Creates a Pair.
     *
     * @param left left value
     * @param right right value
     */
    public Pair( T1 left, T2 right ) {
        this.left = left;
        this.right = right;
    }

    @Override
    public int compareTo( @NotNull Pair<T1, T2> that ) {
        //noinspection unchecked
        int c = compare( (Comparable) this.left, (Comparable) that.left );
        if ( c == 0 ) {
            //noinspection unchecked
            c = compare( (Comparable) this.right, (Comparable) that.right );
        }
        return c;
    }


    @Override
    public T1 getKey() {
        return left;
    }


    @Override
    public T2 getValue() {
        return right;
    }


    @Override
    public T2 setValue( T2 value ) {
        throw new UnsupportedOperationException();
    }

    public boolean equals( Object obj ) {
        return this == obj
                || (obj instanceof Pair)
                && Objects.equals( this.left, ((Pair) obj).left )
                && Objects.equals( this.right, ((Pair) obj).right );
    }

    /**
     * Compares a pair of comparable values of the same type. Null collates less than everything else, but equal to itself.
     *
     * @param c1 First value
     * @param c2 Second value
     * @return a negative integer, zero, or a positive integer if c1 is less than, equal to, or greater than c2.
     */
    private static <C extends Comparable<C>> int compare( C c1, C c2 ) {
        if ( c1 == null ) {
            if ( c2 == null ) {
                return 0;
            } else {
                return -1;
            }
        } else if ( c2 == null ) {
            return 1;
        } else {
            return c1.compareTo( c2 );
        }
    }

}
