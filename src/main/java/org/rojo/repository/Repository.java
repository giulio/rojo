package org.rojo.repository;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.rojo.annotations.Reference;
import org.rojo.annotations.Value;
import org.rojo.exceptions.InvalidTypeException;
import org.rojo.exceptions.RepositoryError;
import org.rojo.repository.utils.IdUtil;

public class Repository implements EntityReader, EntityWriter {

    private final RedisFacade redisFacade;
    private final EntityValidator validator;

    public Repository(RedisFacade redisFacade, EntityValidator validator) {
        this.redisFacade = redisFacade;
        this.validator = validator;
    }

    @Override
    public long write(Object entity) {
        validator.validateEntity(entity.getClass());
        long id = IdUtil.readId(entity);
        if (id <= 0) {
            id = redisFacade.nextId(entity.getClass());
            setIdField(entity, id);
        }
        for (Field field : entity.getClass().getDeclaredFields()) {
            // set accessibility for private fields
            boolean origFieldAccessibility = field.isAccessible();
            if (!origFieldAccessibility) field.setAccessible(true);
            try {
                if (field.get(entity) != null) {

                    if (field.isAnnotationPresent(Value.class)) {
                        if (Collection.class.isAssignableFrom(field.getType())) {
                            @SuppressWarnings("unchecked")
                            Collection<? extends Object> collection = (Collection<? extends Object>)field.get(entity);
                            redisFacade.writeCollection(entity, collection, id, field);
                        } else {
                            redisFacade.write(entity, id, field);
                        }
                    }
                    if (field.isAnnotationPresent(Reference.class)) {
                        if (Collection.class.isAssignableFrom(field.getType())) {
                            @SuppressWarnings("unchecked")
                            Collection<? extends Object> referredEntities = (Collection<? extends Object>)field.get(entity);
                            if (referredEntities.size() != 0) {
                                List<Long> ids = new ArrayList<Long>(referredEntities.size());
                                for (Object referredEntity : referredEntities) {
                                    ids.add(this.write(referredEntity));
                                }
                                redisFacade.writeReferenceCollection(entity, field, id, ids);
                            }

                        } else {
                            redisFacade.writeReference(entity, field, id, this.write(field.get(entity)));
                        }
                    }
                }

            } catch (Exception e) {
                throw new RepositoryError("error writing " + entity.getClass() + " - " + id + " - " + field.getName(), e);
            } finally {
                if (!origFieldAccessibility) field.setAccessible(origFieldAccessibility); 
            }

        }
        return id;
    }

    @Override
    public <T> T get(T entity, long id) {

        validator.validateEntity(entity.getClass());

        setIdField(entity, id); 

        for (Field field : entity.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(Value.class) || field.isAnnotationPresent(Reference.class)) {
                if (redisFacade.hasKey(entity.getClass(), id, field)) {

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
        }
        return entity;
    }

    private <T> void setIdField(T entity, long id) {
        Field idField = IdUtil.getIdField(entity.getClass());

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
