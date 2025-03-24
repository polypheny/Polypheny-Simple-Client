/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/21/25, 7:37 PM The Polypheny Project
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

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import kong.unirest.core.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.scenario.lockingBench.NumberTracker;
import org.polypheny.simpleclient.scenario.lockingBench.LockingBenchSchema;

public class SingleInsert extends QueryBuilder {

    private static final boolean EXPECT_RESULT = false;
    private final Optional<String> entityName;
    private final Optional<LockingBenchSchema> schema;
    private final NumberTracker numberTracker;


    public SingleInsert( String entityName, NumberTracker numberTracker ) {
        this.entityName = Optional.of( entityName );
        this.numberTracker = numberTracker;
        this.schema = Optional.empty();
    }


    public SingleInsert( LockingBenchSchema schema, NumberTracker numberTracker ) {
        this.entityName = Optional.empty();
        this.numberTracker = numberTracker;
        this.schema = Optional.of( schema );
    }


    @Override
    public BatchableInsert getNewQuery() {
        long id = numberTracker.getNextId();
        int categoryId = numberTracker.getRandomCategoryId();
        double score = ThreadLocalRandom.current().nextDouble();
        boolean isVerified = ThreadLocalRandom.current().nextBoolean();


        String tableName;
        if (this.entityName.isPresent()) {
            tableName = this.entityName.get();
        } else if (this.schema.isPresent()) {
            tableName = schema.get().getRandomEntityFullName();
        } else {
            System.out.println(entityName);
            System.out.println(schema);
            throw new RuntimeException("WTF");
        }

        return new InsertEntryQuery(
                tableName,
                id,
                categoryId,
                score,
                isVerified
        );
    }


    static class InsertEntryQuery extends BatchableInsert {

        private static final String SQL = "INSERT INTO %s(id, category_id, score, is_verified) VALUES ";

        private final String tableName;
        private final long id;
        private final int categoryId;
        private final double score;
        private final boolean isVerified;


        public InsertEntryQuery( String tableName, long id, int categoryId, double score, boolean isVerified ) {
            super( EXPECT_RESULT );
            this.tableName = tableName;
            this.id = id;
            this.categoryId = categoryId;
            this.score = score;
            this.isVerified = isVerified;
        }


        @Override
        public String getSql() {
            return String.format( SQL, tableName ) + getSqlRowExpression();
        }


        @Override
        public String getSqlRowExpression() {
            return "("
                    + id + ", "
                    + categoryId + ", "
                    + score + ", "
                    + isVerified
                    + ")";
        }


        @Override
        public String getParameterizedSqlQuery() {
            return String.format( SQL, tableName ) + "(?, ?, ?, ?)";
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            Map<Integer, ImmutablePair<DataTypes, Object>> map = new HashMap<>();
            map.put( 1, new ImmutablePair<>( DataTypes.BIGINT, id ) );
            map.put( 2, new ImmutablePair<>( DataTypes.INTEGER, categoryId ) );
            map.put( 3, new ImmutablePair<>( DataTypes.DECIMAL, score ) );
            map.put( 6, new ImmutablePair<>( DataTypes.BOOLEAN, isVerified ) );
            return map;
        }


        @Override
        public HttpRequest<?> getRest() {
            return buildRestInsert( tableName, ImmutableList.of( getRestRowExpression() ) );
        }


        @Override
        public JsonObject getRestRowExpression() {
            JsonObject row = new JsonObject();
            row.addProperty( tableName + ".id", id );
            row.addProperty( tableName + ".category_id", categoryId );
            row.addProperty( tableName + ".score", score );
            row.addProperty( tableName + ".is_verified", isVerified );
            return row;
        }


        @Override
        public String getMongoQl() {
            return "db." + tableName + ".insert({"
                    + "\"id\":" + id + ", "
                    + "\"category_id\":" + categoryId + ", "
                    + "\"score\":" + score + ", "
                    + "\"is_verified\":" + isVerified
                    + "})";
        }


        @Override
        public String getEntity() {
            return tableName;
        }

    }

}
