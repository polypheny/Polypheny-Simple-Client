package org.polypheny.simpleclient.scenario.gavelEx.exception;

public class UnknownProfileException extends RuntimeException{

    public UnknownProfileException(String msg){
        super(msg);
    }

    public UnknownProfileException(int id){
        super( String.valueOf( id ) );
    }
}
