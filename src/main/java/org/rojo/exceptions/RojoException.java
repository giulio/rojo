package org.rojo.exceptions;

@SuppressWarnings("serial")
public class RojoException extends RuntimeException {

    public RojoException() {
        super();
    }

    public RojoException(Throwable e) {
        super(e);
    }

    public RojoException(String msg) {
        super(msg);
    }

    
}
