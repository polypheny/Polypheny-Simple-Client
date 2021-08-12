package org.polypheny.simpleclient.query;

import java.util.Map;
import kong.unirest.HttpRequest;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class RawQuery extends Query {

    @Getter
    private final String sql;

    @Getter
    private final HttpRequest<?> rest;

    @Getter
    private final String mongoQl;


    public RawQuery( String sql, HttpRequest<?> rest, boolean expectResultSet ) {
        this( sql, rest, null, expectResultSet );
    }


    public RawQuery( String sql, HttpRequest<?> rest, String mongoQl, boolean expectResultSet ) {
        super( expectResultSet );
        this.sql = sql;
        this.rest = rest;
        this.mongoQl = mongoQl;
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
