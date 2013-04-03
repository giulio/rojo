package org.rojo.repository.utils;

import java.lang.reflect.Field;

import org.rojo.annotations.Id;
import org.rojo.exceptions.InvalidTypeException;

public class IdUtil {

    public static long readId(Object entity) {
        try {
            long id = 0;
            Field idField = getIdField(entity.getClass());
            
            boolean origFieldAccessibility = idField.isAccessible();
            if (!origFieldAccessibility) idField.setAccessible(true);
            
            id =  idField.getLong(entity);
            if (!origFieldAccessibility) idField.setAccessible(origFieldAccessibility); 

            return id;
            
        } catch (Exception e) {
            throw new InvalidTypeException(e);
        }
    }


    public static long readId(Object entity, Field id) {
        long returnValue = 0;
        boolean origFieldAccessibility = id.isAccessible();
        if (!origFieldAccessibility) id.setAccessible(true);
        try {
            returnValue =  id.getLong(entity);
        } catch (IllegalAccessException e) {
            throw new InvalidTypeException(e);
        } finally {
            if (!origFieldAccessibility) id.setAccessible(origFieldAccessibility);
        }
        return returnValue;
    }
    
    public static Field getIdField(Class<? extends Object> entity) {
        for (Field field : entity.getDeclaredFields()) {
            if (field.getAnnotation(Id.class) != null)
                return field;
        }
        throw new InvalidTypeException("missing @Id field!");
    }  

    public static long decodeId(String idString) {
        return Long.parseLong(idString);
    }

    public static String encodeId(long id) {
        return new Long(id).toString();
    }

    
}
