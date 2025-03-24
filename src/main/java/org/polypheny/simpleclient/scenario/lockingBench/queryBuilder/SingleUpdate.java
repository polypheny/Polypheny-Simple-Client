/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/21/25, 8:09 PM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.lockingBench.queryBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.core.HttpRequest;
import kong.unirest.core.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.lockingBench.NumberTracker;
import org.polypheny.simpleclient.scenario.lockingBench.LockingBenchSchema;

public class SingleUpdate extends QueryBuilder {

    private final LockingBenchSchema schema;
    private final NumberTracker numberTracker;

    public SingleUpdate( LockingBenchSchema schema, NumberTracker numberTracker ) {
        this.schema = schema;
        this.numberTracker = numberTracker;
    }

    @Override
    public Query getNewQuery() {
        String tableName = schema.getRandomEntityFullName();
        long id = numberTracker.getRandomId();
        double newScore = ThreadLocalRandom.current().nextDouble();
        boolean newVerified = ThreadLocalRandom.current().nextBoolean();

        return new SingleUpdateQuery(tableName, id, newScore, newVerified);
    }

    private static class SingleUpdateQuery extends Query {

        private final String tableName;
        private final long id;
        private final double newScore;
        private final boolean newVerified;

        public SingleUpdateQuery(String tableName, long id, double newScore, boolean newVerified) {
            super(false); // no result expected
            this.tableName = tableName;
            this.id = id;
            this.newScore = newScore;
            this.newVerified = newVerified;
        }

        @Override
        public String getSql() {
            return "UPDATE " + tableName + " SET score = " + newScore + ", is_verified = " + newVerified + " WHERE id = " + id;
        }

        @Override
        public String getParameterizedSqlQuery() {
            return "UPDATE " + tableName + " SET score = ?, is_verified = ? WHERE id = ?";
        }

        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put(1, new ImmutablePair<>(DataTypes.DECIMAL, newScore));
            map.put(2, new ImmutablePair<>(DataTypes.BOOLEAN, newVerified));
            map.put(3, new ImmutablePair<>(DataTypes.BIGINT, id));
            return map;
        }

        @Override
        public HttpRequest<?> getRest() {
            return Unirest.patch("{protocol}://{host}:{port}/restapi/v1/res/public." + tableName)
                    .queryString("_where", "public." + tableName + ".id.eq." + id)
                    .header("Content-Type", "application/json")
                    .body("{\"public." + tableName + ".score\": " + newScore +
                            ", \"public." + tableName + ".is_verified\": " + newVerified + "}");
        }

        @Override
        public String getMongoQl() {
            return "db." + tableName + ".updateOne({ \"id\": " + id + " }, { $set: { \"score\": " + newScore +
                    ", \"is_verified\": " + newVerified + " } })";
        }
    }
}
