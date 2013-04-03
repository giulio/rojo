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
import org.rojo.exceptions.MissingEntity;
import org.rojo.exceptions.RepositoryError;
import org.rojo.repository.utils.IdUtil;

public class Repository implements EntityReader, EntityWriter {

    private final KeyValueStore store;
    private final EntityValidator validator;

    public Repository(KeyValueStore store, EntityValidator validator) {
        this.store = store;
        this.validator = validator;
    }

    @Override
    public long write(Object entity) {

        EntityRepresentation representation = EntityRepresentation.forClass(entity.getClass());

        long id = representation.getId(entity);
        if (id <= 0) {
            id = store.nextId(entity.getClass());
            representation.setId(entity, id);
        }
        store.writeId(entity, id);


        for (Field field : representation.getValues()) {
            boolean origFieldAccessibility = field.isAccessible();
            if (!origFieldAccessibility) field.setAccessible(true);
            try {
                if (field.get(entity) != null) {
                    if (Collection.class.isAssignableFrom(field.getType())) {
                        @SuppressWarnings("unchecked")
                        Collection<? extends Object> collection = (Collection<? extends Object>)field.get(entity);
                        store.writeCollection(entity, collection, id, field);
                    } else {
                        store.write(entity, id, field);
                    }
                }
            } catch (Exception e) {
                throw new RepositoryError("error writing " + entity.getClass() + " - " + id + " - " + field.getName(), e);
            } finally {
                if (!origFieldAccessibility) field.setAccessible(origFieldAccessibility);
            }
        }

        for (Field field : representation.getReferences()) {
            boolean origFieldAccessibility = field.isAccessible();
            if (!origFieldAccessibility) field.setAccessible(true);
            try {
                if (field.get(entity) != null) {
                    if (Collection.class.isAssignableFrom(field.getType())) {
                        @SuppressWarnings("unchecked")
                        Collection<? extends Object> referredEntities = (Collection<? extends Object>)field.get(entity);
                        if (referredEntities.size() != 0) {
                            List<Long> ids = new ArrayList<Long>(referredEntities.size());
                            for (Object referredEntity : referredEntities) {
                                ids.add(this.write(referredEntity));
                            }
                            store.writeReferenceCollection(entity, field, id, ids);
                        }

                    } else {
                        store.writeReference(entity, field, id, this.write(field.get(entity)));
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

        EntityRepresentation representation = EntityRepresentation.forClass(entity.getClass());

        if (!store.exists(entity, id)) {
            throw new MissingEntity(entity.getClass(), id);
        }

        representation.setId(entity, id);

        for (Field field : representation.getValues()) {
            if (store.hasKey(entity.getClass(), id, field)) {
                // set accessibility for private fields
                boolean origFieldAccessibility = field.isAccessible();
                if (!origFieldAccessibility) field.setAccessible(true);
                processField(entity, id, field);
                if (!origFieldAccessibility) field.setAccessible(origFieldAccessibility);

            }
        }

        for (Field field : representation.getReferences()) {
            boolean origFieldAccessibility = field.isAccessible();
            if (!origFieldAccessibility) field.setAccessible(true);
            processReference(entity, id, field);
            if (!origFieldAccessibility) field.setAccessible(origFieldAccessibility);

        }

        return entity;
    }
    
    
    public void delete(Object entity) {
        EntityRepresentation representation = EntityRepresentation.forClass(entity.getClass());

        long id = representation.getId(entity);
        if (id <= 0) {
            throw new RepositoryError(entity.getClass() + ", invalid id: " + id);
        }
        store.removeId(entity, id);


        for (Field field : representation.getValues()) {
            if (store.hasKey(entity.getClass(), id, field)) {
                store.delete(entity, id, field);
            }
        }

        for (Field field : representation.getReferences()) {
            if (store.hasKey(entity.getClass(), id, field)) {
                store.delete(entity, id, field);
            }
        }
    }


    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <T> void processReference(T entity, long id, Field field) {
        try {
            if (Collection.class.isAssignableFrom(field.getType())) {
                Collection holder = initCollectionHolder(field);

                for (long referredId : store.getReferredIds(entity, id, field)) {
                    holder.add(this.get(((Class)((java.lang.reflect.ParameterizedType)field.getGenericType()).getActualTypeArguments()[0]).newInstance()
                            , referredId));
                }

                field.set(entity, holder);
            } else {
                field.set(entity, this.get(field.getType().newInstance(), store.getReferredId(entity, id, field)));
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
                store.readValues(entity, id, field, holder);
                field.set(entity, holder);
            } else {
                field.set(entity, store.readValue(entity, id, field));
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
