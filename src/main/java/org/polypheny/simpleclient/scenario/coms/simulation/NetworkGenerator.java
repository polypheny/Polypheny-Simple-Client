/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/11/23, 11:48 AM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.coms.simulation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.simpleclient.scenario.coms.ComsConfig;
import org.polypheny.simpleclient.scenario.coms.simulation.Graph.Edge;
import org.polypheny.simpleclient.scenario.coms.simulation.Graph.Node;

public class NetworkGenerator {

    public final ComsConfig config;
    private final Random random;
    private final Network network;


    /**
     * Network architecture relies on a low amount of main architecture
     * with a low connectivity
     * Depending on size of users a amount of Switches and APs is deploy.
     *
     * @param config
     */
    public NetworkGenerator( ComsConfig config ) {
        this.config = config;
        this.random = new Random( config.seed );

        // generate network (simulation)
        // -> generate structure of network -> nodes
        // -> generate parameters per nodes
        // -> generate configs depending on type of node
        // -> generate logs?

        this.network = new Network( random, config.networkScale );

    }


    @Value
    public static class Network {

        public static AtomicLong idBuilder = new AtomicLong();
        public static long SERVERS = 5;

        public static long CLIENTS = 100;

        public static double OS_DISTRIBUTION = 0.7;

        public static double MOBILE_DISTRIBUTION = 0.4;

        public static long MOBILES = 400;

        public static long APS = 100;

        public static long SWITCHES = 5;
        Random random;
        List<Server> servers = new ArrayList<>();
        List<Switch> switches = new ArrayList<>();

        List<AP> aps = new ArrayList<>();
        List<IoT> ioTs = new ArrayList<>();

        List<Mobile> mobiles = new ArrayList<>();

        List<PC> pcs = new ArrayList<>();

        List<Mac> macs = new ArrayList<>();

        List<Lan> lans = new ArrayList<>();

        List<WLan> wlans = new ArrayList<>();
        int scale;


        public Network( Random random, int scale ) {
            this.random = random;
            this.scale = scale;

            generateObject( SERVERS, () -> servers.add( new Server( random ) ) );
            generateObject( SWITCHES, () -> switches.add( new Switch( random ) ) );
            generateObject( APS, () -> aps.add( new AP( random ) ) );

            generateObject( CLIENTS * OS_DISTRIBUTION, () -> pcs.add( new PC( random ) ) );
            generateObject( CLIENTS * (1 - OS_DISTRIBUTION), () -> macs.add( new Mac( random ) ) );

            generateObject( MOBILES * MOBILE_DISTRIBUTION, () -> mobiles.add( new Mobile( random ) ) );
            generateObject( MOBILES * (1 - MOBILE_DISTRIBUTION), () -> ioTs.add( new IoT( random ) ) );

            generateConnection( this.servers, this.servers, 2, Connection.LAN );

            generateConnection( this.servers, this.switches, 2, Connection.LAN );

            generateConnection( this.switches, this.pcs, 2, Connection.LAN );
            generateConnection( this.switches, this.macs, 2, Connection.LAN );

            generateConnection( this.servers, this.aps, 2, Connection.LAN );

            generateConnection( this.aps, this.mobiles, 2, Connection.WLAN );
            generateConnection( this.aps, this.ioTs, 2, Connection.WLAN );
        }


        private <L extends GraphElement, R extends GraphElement> void generateConnection( List<L> fromElements, List<R> toElements, int max, Connection connection ) {

        }


        private void generateObject( double amount, Runnable runnable ) {
            for ( long i = 0; i < amount * this.scale; i++ ) {
                runnable.run();
            }
        }


        Graph toGraph() {
            return null;
        }

    }

    //// Backbone


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Server extends Node {

        Random random;

    }

    //// Distributors


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Switch extends Node {

        Random random;

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class AP extends Node {

        Random random;

    }

    //// Devices


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class IoT extends Node {

        Random random;

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Mobile extends Node {

        Random random;

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class PC extends Node {

        Random random;

    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Mac extends Node {

        Random random;

    }

    //// Connections


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class Lan extends Edge {


        Random random;




    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class WLan extends Edge {


        Random random;

    }

    enum Connection{
        WLAN,
        LAN
    }

}
