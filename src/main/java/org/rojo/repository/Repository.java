package org.rojo.repository;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.rojo.annotations.Id;
import org.rojo.annotations.Reference;
import org.rojo.annotations.Value;
import org.rojo.exceptions.InvalidTypeException;

public class Repository implements ReadRepository {

    private final RedisFacade redisFacade;
    private final EntityValidator validator;

    public Repository(RedisFacade redisUtils, EntityValidator validator) {
        this.redisFacade = redisUtils;
        this.validator = validator;
    }

    @Override
    public <T> T get(T entity, long id) {

        validator.validateEntity(entity.getClass());

        setIdField(entity, id); 

        for (Field field : entity.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(Value.class) || field.isAnnotationPresent(Reference.class)) {

                // set accessibility for private fields
                boolean origFieldAccessibility = field.isAccessible();
                if (!origFieldAccessibility) field.setAccessible(true);

                if (field.isAnnotationPresent(Value.class)) {
                    processField(entity, id, field); 
                } else if (field.isAnnotationPresent(Reference.class)) {
                    processReference(entity, id, field);
                }

                // reset accessibility
                if (!origFieldAccessibility) field.setAccessible(origFieldAccessibility); 
            }
        }
        return entity;
    }

    private <T> void setIdField(T entity, long id) {
        Field idField = getIdField(entity);

        boolean idFieldAccessibility = idField.isAccessible();
        if (!idFieldAccessibility) idField.setAccessible(true);
        try {
            idField.set(entity, id);
        } catch (Exception e) {
            new InvalidTypeException(e);
        }

        if (!idFieldAccessibility) idField.setAccessible(idFieldAccessibility);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <T> void processReference(T entity, long id, Field field) {
        try {
            if (Collection.class.isAssignableFrom(field.getType())) {
                Collection holder = initCollectionHolder(field);
            
                for (long referredId : redisFacade.getReferredIds(entity, id, field)) {
                    holder.add(this.get(((Class)((java.lang.reflect.ParameterizedType)field.getGenericType()).getActualTypeArguments()[0]).newInstance()
                            , referredId));
                }
           
                field.set(entity, holder);
            } else {
                field.set(entity, this.get(field.getType().newInstance(), redisFacade.getReferredId(entity, id, field)));
            }
        } catch (Exception e) {
            throw new InvalidTypeException(e);
        }
    }

  

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <T> void processField(T entity, long id, Field field) {
        try {
            if (Collection.class.isAssignableFrom(field.getType())) {
                Collection holder = initCollectionHolder(field);
                redisFacade.readValues(entity, id, field, holder);
                field.set(entity, holder);
            } else {
                field.set(entity, redisFacade.readValue(entity, id, field));
            }
        } catch (Exception e) {
            new InvalidTypeException(e);
        }
    }
   
    private Field getIdField(Object type) {
        for (Field field : type.getClass().getDeclaredFields()) {
            if (field.getAnnotation(Id.class) != null)
                return field;
        }
        throw new InvalidTypeException("missing @Id field!");
    }
    
    @SuppressWarnings("rawtypes")
    private Collection initCollectionHolder(Field field) {
        if (field.getType() == List.class || field.getType() == Collection.class) {
            return new ArrayList();
        } else if (field.getType() == Set.class) {
            return new HashSet();
        } else {
            throw new InvalidTypeException("unsupported Collection subtype"); 
        }
    }


}
