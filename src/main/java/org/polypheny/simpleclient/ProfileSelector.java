package org.polypheny.simpleclient;

import org.polypheny.simpleclient.scenario.gavelEx.exception.UnknownProfileException;

public enum ProfileSelector {
    PROFILE_1(1),
    PROFILE_2(2),
    PROFILE_3(3);

    private final int id;

    ProfileSelector( int id ) {
        this.id = id;
    }

    public int getId(){
        return id;
    }

    public static ProfileSelector getById(int id){
        for(ProfileSelector profile: values()){
            if(profile.id == ( id )){
                return profile;
            }
        }
        throw new UnknownProfileException( id );
    }

    public static ProfileSelector getByName(String name) throws UnknownProfileException {
        for(ProfileSelector profile: values()){
            if(profile.name().equalsIgnoreCase( name )){
                return profile;
            }
        }
        throw new UnknownProfileException( name );
    }

}
