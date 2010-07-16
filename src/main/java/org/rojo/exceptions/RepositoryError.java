package org.rojo.exceptions;

import org.jredis.RedisException;

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
