package org.rojo.domain;

import org.rojo.annotations.Entity;
import org.rojo.annotations.Id;
import org.rojo.annotations.Value;

@Entity
public class Address {

    @Id
    private long id;
    
    @Value
    private String town;
    
    @Value
    private String street;

    public String getTown() {
        return town;
    }

    public void setTown(String town) {
        this.town = town;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public long getId() {
        return id;
    }
    
    
}
