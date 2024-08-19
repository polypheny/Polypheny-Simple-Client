/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-8/8/24, 3:07 PM The Polypheny Project
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

package org.polypheny.simpleclient.executor;

import kong.unirest.core.Unirest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GithubApi {

    public static GitCommit getCommit( String branch ) {
        log.info( "Getting commit hash for {}", branch );
        GitBranch b = Unirest.get( String.format( "https://api.github.com/repos/polypheny/Polypheny-DB/branches/%s", branch ) )
                .header( "Accept", "application/vnd.github+json" )
                .header( "X-GitHub-Api-Version", "2022-11-28" )
                .asObject( GitBranch.class )
                .getBody();

        return b.commit();
    }


    public record GitCommit( String sha ) {

    }


    record GitBranch( String name, GitCommit commit ) {

    }

}
