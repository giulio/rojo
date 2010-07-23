package org.rojo.repository;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;

public interface KeyValueStore {
    
    <T> long getReferredId(T type, long id, Field field);

    <T> T readValue(T type, long id, Field field);

    <T> void readValues(T type, long id, Field field,
            Collection<T> destination);

    <T> List<Long> getReferredIds(T type, long id, Field field);

    void write(Object entity, long id, Field field);

    void writeCollection(Object entity,
            Collection<? extends Object> collection, long id, Field field);

    void writeReference(Object entity, Field field, long id,
            long referencedId);

    void writeReferenceCollection(Object entity, Field field,
            long id, List<Long> referredIds);

    long nextId(Class<? extends Object> entity);

    boolean hasKey(Class<? extends Object> entity, long id,
            Field field);

    void delete(Object entity, long id, Field field);

    void writeId(Object entity, long id);
    
    boolean exists(Object entity, long id);

    void removeId(Object entity, long id);
    
}