package org.rojo.repository;

public interface Repository {

    public <T> T get(T entity, long id);

}
