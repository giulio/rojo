package org.rojo.repository;

public interface ReadRepository {

    public <T> T get(T entity, long id);

}
