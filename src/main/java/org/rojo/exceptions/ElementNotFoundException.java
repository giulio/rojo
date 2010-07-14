package org.rojo.exceptions;

@SuppressWarnings("serial")
public class ElementNotFoundException extends RojoException {

    private String type;
    private String id;
    
    public ElementNotFoundException(String type, String id) {
        super();
        this.type = type;
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }
    
    
}
