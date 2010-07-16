package org.rojo.repository;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.rojo.annotations.Entity;
import org.rojo.annotations.Id;
import org.rojo.annotations.Value;
import org.rojo.exceptions.InvalidTypeException;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class AnnotationValidator implements EntityValidator {

    private static List<Class> validatedEntityCache = new ArrayList<Class>();

    @Override
    public void validateEntity(Class entityClass) {
        if (!validatedEntityCache.contains(entityClass)) {
            verifyEntityAnnotation(entityClass);
            verifyIdAnnotationPresent(entityClass);
            verifyCollectionsConstraints(entityClass);
            validatedEntityCache.add(entityClass);
        }
    }

    private void verifyCollectionsConstraints(Class entityClass) {
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getAnnotation(Value.class) != null && Collection.class.isAssignableFrom(field.getType())) {
                if (!(field.getType() == Set.class || field.getType() == List.class || field.getType() == Collection.class)) {
                    error(entityClass, "only Collection, Set and List are supported");
                }
            }
        }
    }

    private void verifyIdAnnotationPresent(Class entityClass) {
        Field idField = null;
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getAnnotation(Id.class) != null) idField = field;
        }
        if (idField == null) {
            error(entityClass, "missing @Id field!");
        }
        if (!(idField.getType() == long.class || idField.getType() == Long.class)) {
            error(entityClass, "invalid @Id field type! accepted types are {long, Long}");
        }
    }

    private void verifyEntityAnnotation(Class entityClass) {
        if (entityClass.getAnnotation(Entity.class) == null) {
            error(entityClass, "missing @Entity annotation");
        }
    }

    private void error(Class entityClass, String msg) {
        throw new InvalidTypeException(entityClass.getCanonicalName() + ": " + msg);
    }

}
