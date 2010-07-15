package org.rojo.domain;

import java.util.List;

import org.rojo.annotations.Entity;
import org.rojo.annotations.Id;
import org.rojo.annotations.Value;

@Entity
public class SomeStrings {
    
    @Id
    private long id;
    
    @Value
    private List<String> strings;

    public List<String> getStrings() {
        return strings;
    }
    
}
