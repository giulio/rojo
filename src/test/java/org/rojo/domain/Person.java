package org.rojo.domain;

import java.util.List;

import org.rojo.annotations.Entity;
import org.rojo.annotations.Id;
import org.rojo.annotations.Reference;
import org.rojo.annotations.Value;

@Entity
public class Person {

    @Id
    private long id;
    
    @Value
    private String name;
    
    @Value
    private int age;
    
    @Reference
    private Address address;
    
    @Reference
    private List<Person> friends;
    
    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address adress) {
        this.address = adress;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }
    
    public long getId() {
        return id;
    }
    
    public void setId(long id) {
        this.id = id;
    }
    
    
    public List<Person> getFriends() {
        return friends;
    }
    
}
