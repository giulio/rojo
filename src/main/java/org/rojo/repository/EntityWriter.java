package org.rojo.repository;

public interface EntityWriter {
    
    long write(Object entity);
    
    void delete(Object entity);

}
