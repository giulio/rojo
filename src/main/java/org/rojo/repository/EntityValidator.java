package org.rojo.repository;

public interface EntityValidator {

    @SuppressWarnings("rawtypes")
    void validateEntity(Class entityClass);
    
}
