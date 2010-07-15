package org.rojo.repository;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jredis.RedisException;
import org.jredis.ri.alphazero.JRedisClient;
import org.rojo.annotations.Entity;
import org.rojo.annotations.Id;
import org.rojo.annotations.Reference;
import org.rojo.annotations.Value;
import org.rojo.exceptions.InvalidTypeException;
import org.rojo.exceptions.RepositoryError;
import org.rojo.repository.converters.Converters;

public class RedisRepository implements Repository {

    private JRedisClient jrClient;
    private Converters converter;

    public RedisRepository(JRedisClient redisClient, Converters converter) {
        this.jrClient = redisClient;
        this.converter = converter;
    }

    @Override
    public <T> T get(T entity, long id) {

        assertThatAnnotationsArePresents(entity);

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
                // handling collections

                Collection holder;
                if (field.getType() == List.class) {
                    holder = new ArrayList();
                } else if (field.getType() == Set.class) {
                    holder = new HashSet();
                } else {
                    throw new InvalidTypeException("only List/Set are supported for the time beeing ..."); //TODO
                }

                for (byte[] ref : readSet(makeLabel(entity), id, field)) {
                    long refId = Long.parseLong(new String(ref));
                    holder.add(this.get(((Class)((java.lang.reflect.ParameterizedType)field.getGenericType()).getActualTypeArguments()[0]).newInstance()
                            , refId));
                }
                field.set(entity, holder);
            } else {
                // single value field
                long refId = Long.parseLong(new String(read(makeLabel(entity), id, field)));
                field.set(entity, this.get(field.getType().newInstance(), refId));
            }
        } catch (Exception e) {
            throw new InvalidTypeException(e);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <T> void processField(T entity, long id, Field field) {
        try {

            if (Collection.class.isAssignableFrom(field.getType())) {
                Collection holder;
                if (field.getType() == List.class) {
                    holder = new ArrayList();
                } else if (field.getType() == Set.class) {
                    holder = new HashSet();
                } else {
                    throw new InvalidTypeException("only List/Set are supported for the time beeing ..."); //TODO
                }
                for (byte[] value : readSet(makeLabel(entity), id, field)) {
                    holder.add(converter.getConverterFor((Class)((java.lang.reflect.ParameterizedType)field.getGenericType()).getActualTypeArguments()[0]).decode(value));
                }
                field.set(entity, holder);
            
            } else {
                byte[] value = read(makeLabel(entity), id, field);
                field.set(entity, converter.getConverterFor(field.getType()).decode(value));
                
            }

        } catch (Exception e) {
            new InvalidTypeException(e);
        }
    }

    private byte[] read(String type, long id, Field field) {
        try {
            return jrClient.get(type + ":" + id + ":" + field.getName().toLowerCase());
        } catch (RedisException e) {
            throw new RepositoryError(e);
        }
    }

    private List<byte[]> readSet(String type, long id, Field field) {
        try {
            return jrClient.lrange(type + ":" + id + ":" + field.getName().toLowerCase(), 0, -1);
        } catch (RedisException e) {
            throw new RepositoryError(e);
        }
    }

    private String makeLabel(Object entity) {
        return entity.getClass().getCanonicalName().toLowerCase();   
    }

    private <T> void assertThatAnnotationsArePresents(Object type) {
        if (type.getClass().getAnnotation(Entity.class) == null) {
            throw new InvalidTypeException();
        }
        Field idField = getIdField(type);
        if (idField == null) {
            throw new InvalidTypeException("missing @Id field");
        }
        if (!(idField.getType() == long.class || idField.getType() == Long.class)) {
            throw new InvalidTypeException("invalid @Id field type! accepted types are {long, Long}");
        }
    }


    private Field getIdField(Object type) {
        for (Field field : type.getClass().getDeclaredFields()) {
            if (field.getAnnotation(Id.class) != null)
                return field;
        }
        return null;
    }


}
