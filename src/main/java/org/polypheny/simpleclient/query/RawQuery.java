package org.polypheny.simpleclient.query;

import lombok.Getter;

public class RawQuery extends Query {

    @Getter
    private final String sql;

    @Getter
    private final String rest;


    public RawQuery( String sql, String rest, boolean expectResultSet ) {
        super( expectResultSet );
        this.sql = sql;
        this.rest = rest;
    }

}
