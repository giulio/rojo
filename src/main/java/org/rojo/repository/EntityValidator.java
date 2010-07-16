package org.rojo.repository;

public interface EntityValidator {

    void validateEntity(Class<? extends Object> entityClass);
    
}
