package org.rojo.repository.utils;

import java.lang.reflect.Field;

import org.rojo.annotations.Id;
import org.rojo.exceptions.InvalidTypeException;

public class IdUtil {

    public static long readId(Object entity) {
        try {
            return getIdField(entity.getClass()).getLong(entity);
        } catch (Exception e) {
            throw new InvalidTypeException(e);
        }
    }
    
    public static Field getIdField(Class<? extends Object> entity) {
        for (Field field : entity.getDeclaredFields()) {
            if (field.getAnnotation(Id.class) != null)
                return field;
        }
        throw new InvalidTypeException("missing @Id field!");
    }
    
    
}
