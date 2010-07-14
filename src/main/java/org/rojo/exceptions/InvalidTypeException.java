package org.rojo.exceptions;

@SuppressWarnings("serial")
public class InvalidTypeException extends RojoException {

    public InvalidTypeException(String message) {
        super(message);
    }

    public InvalidTypeException(Throwable e) {
        super(e);
    }

    public InvalidTypeException() {
        super();
    }

}
