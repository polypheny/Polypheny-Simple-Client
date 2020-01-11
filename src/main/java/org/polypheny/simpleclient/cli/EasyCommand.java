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

package org.polypheny.simpleclient.cli;


import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.util.List;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.main.Easy;


@Slf4j
@Command(name = "easy", description = "Easy mode for fast testing.")
public class EasyCommand implements CliRunnable {

    @SuppressWarnings("SpringAutowiredFieldsWarningInspection")
    @Inject
    private HelpOption<EasyCommand> help;

    @Arguments(description = "Hostname or IP-Address of the Chronos server.")
    private List<String> args;

    @Option(name = { "-pdb", "--polyphenydb" }, title = "Polypheny-DB Host", arity = 1, description = "Polypheny-DB Host")
    public static String polyphenyDbHost = "127.0.0.1";


    @Override
    public int run() {
        if ( args == null || args.size() < 1 ) {
            System.err.println( "Missing task" );
            System.exit( 1 );
        }

        if ( args.get( 0 ).equalsIgnoreCase( "data" ) ) {
            Easy.data( polyphenyDbHost );
        } else if ( args.get( 0 ).equalsIgnoreCase( "workload" ) ) {
            Easy.workload( polyphenyDbHost );
        } else if ( args.get( 0 ).equalsIgnoreCase( "schema" ) ) {
            Easy.schema( polyphenyDbHost );
        } else {
            System.err.println( "Unknown task: " + args.get( 0 ) );
        }
        System.out.println();
        return 0;
    }


}