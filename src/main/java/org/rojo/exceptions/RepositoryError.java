package org.rojo.exceptions;


@SuppressWarnings("serial")
public class RepositoryError extends RojoException {

    public RepositoryError(Exception e) {
        super(e);
    }

    public RepositoryError(String msg) {
        super(msg);
    }

    public RepositoryError(String msg, Exception e) {
        super(msg, e);
    }

}
