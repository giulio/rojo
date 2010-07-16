package org.rojo.repository;

public interface EntityReader {

    public <T> T get(T entity, long id);

}
