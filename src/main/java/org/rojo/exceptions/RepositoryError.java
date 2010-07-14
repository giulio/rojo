package org.rojo.exceptions;

import org.jredis.RedisException;

@SuppressWarnings("serial")
public class RepositoryError extends RojoException {

    public RepositoryError(RedisException e) {
        super(e);
    }

    public RepositoryError(String msg) {
        super(msg);
    }

}
