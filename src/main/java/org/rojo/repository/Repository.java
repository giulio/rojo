package org.rojo.repository;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.rojo.exceptions.InvalidTypeException;
import org.rojo.exceptions.RepositoryError;
import redis.clients.jedis.Jedis;

public class Repository {

    private final RedisFacade store;

    public Repository(Jedis je) {
        store = new RedisFacade(je);
    }

    public void flush() {
        store.flush();
    }

    public long writeAndFlush(Object entity) {
        long id;
        if ((id = write(entity)) > 0) {
            store.flush();
        }
        return id;
    }

    public long write(Object entity) {
        boolean exist = false;
        boolean noExist = false;
        EntityRepresentation representation = EntityRepresentation.forClass(entity.getClass());
        long id = representation.getId(entity);
        if (representation.getIdGenerator().isEmpty()) {
            if (id <= 0) {
                return 0;
            }
        } else {
            if (id <= 0) {
                id = store.incr(representation.getIdGenerator());
                noExist = true;
            }
        }
        Field unique = representation.getUnique();
        if (unique != null) {
            if (!noExist) {
                exist = store.exists(entity.getClass(), id);
            }
            if (!exist) {
                boolean result = store.writeUnique(entity, representation.getUnique(), id);
                if (!result) {
                    return 0;
                }
            }
        }
        store.writeId(entity.getClass(), id);
        for (Field field : representation.getFields()) {
            try {
                if (field.get(entity) != null) {
                    if (Collection.class.isAssignableFrom(field.getType())) {
                        @SuppressWarnings("unchecked")
                        Collection<? extends Object> collection = (Collection<? extends Object>) field.get(entity);
                        store.writeCollection(entity.getClass(), collection, id, field);
                    } else if (Map.class.isAssignableFrom(field.getType())) {
                        Map map = (Map) field.get(entity);
                        store.writeMap(entity.getClass(), map, id, field);
                    } else {
                        store.write(entity, id, field);
                    }
                }
            } catch (Exception e) {
                throw new RepositoryError("error writing " + entity.getClass() + " - " + id + " - " + field.getName(), e);
            }
        }
        representation.setId(entity, id);
        return id;
    }

    public boolean write(Class claz, long id, String p, Object v) {
        if (store.exists(claz, id)) {
            EntityRepresentation representation = EntityRepresentation.forClass(claz);
            Field f = representation.getField(p);
            if (Collection.class.isAssignableFrom(f.getType()) && Collection.class.isAssignableFrom(v.getClass())) {
                store.writeCollection(claz, (Collection) v, id, f);
            } else if (Map.class.isAssignableFrom(f.getType()) && Map.class.isAssignableFrom(v.getClass())) {
                store.writeMap(claz, (Map) v, id, f);
            } else {
                store.write(claz, id, f, v);
            }
            flush();
            return true;
        }
        return false;
    }

    public boolean writeAndFlush(Class claz, long id, String p, Object v) {
        if (this.write(claz, id, p, v)) {
            this.flush();
            return true;
        }
        return false;
    }

    public <T> T get(Class<T> claz, long id) {
        EntityRepresentation representation = EntityRepresentation.forClass(claz);
        if (!store.exists(claz, id)) {
            return null;
        }
        try {
            T entity = claz.newInstance();
            representation.setId(entity, id);
            for (Field field : representation.getFields()) {
                processField(entity, id, field);
            }
            return entity;
        } catch (Exception e) {
            throw new RepositoryError(e);
        }
    }

    public <T> T get(Class<T> claz, long id, String p) {
        try {
            EntityRepresentation representation = EntityRepresentation.forClass(claz);
            Field f = representation.getField(p);
            if (Collection.class.isAssignableFrom(f.getType())) {
                Collection holder = initCollectionHolder(f);
                store.readValues(claz, id, f, holder);
                return (T) holder;
            } else if (Map.class.isAssignableFrom(f.getType())) {
                Map map = initMapHolder(f);
                store.readValues(claz, id, f, map);
            } else {
                return store.readValue(claz, id, f);
            }
        } catch (Exception e) {
        }
        return null;
    }

    public void delete(Class claz, long id) {
        if (id <= 0) {
            return;
        }
        EntityRepresentation representation = EntityRepresentation.forClass(claz);
        store.removeId(claz, id);
        for (Field field : representation.getFields()) {
            store.delete(claz, id, field);
        }
        flush();
    }

    /**
     * the rank
     *
     * @param claz
     * @param p
     * @param start
     * @param end
     * @return
     */
    public List<Long> rank(Class claz, String p, long start, long end) {
        EntityRepresentation representation = EntityRepresentation.forClass(claz);
        Field f = representation.getField(p);
        if (f != null) {
            Set<String> r = store.rank(claz, f, start, end);
            List<Long> rank = new ArrayList<Long>(r.size());
            for (String item : r) {
                rank.add(Long.parseLong(item));
            }
            return rank;
        } else {
            throw new InvalidTypeException("miss field:" + p + " of " + claz);
        }
    }

    private <T> void processField(T entity, long id, Field field) {
        try {
            if (Collection.class.isAssignableFrom(field.getType())) {
                Collection holder = initCollectionHolder(field);
                store.readValues(entity.getClass(), id, field, holder);
                field.set(entity, holder);
            } else if (Map.class.isAssignableFrom(field.getType())) {
                Map map = initMapHolder(field);
                store.readValues(entity.getClass(), id, field, map);
                field.set(entity, map);
            } else {
                field.set(entity, store.readValue(entity.getClass(), id, field));
            }
        } catch (Exception e) {
            new InvalidTypeException(e);
        }
    }

    private static Collection initCollectionHolder(Field field) {
        if (field.getType() == List.class || field.getType() == Collection.class) {
            return new ArrayList();
        } else if (field.getType() == Set.class) {
            return new HashSet();
        } else {
            throw new InvalidTypeException("unsupported Collection subtype");
        }
    }

    private static Map initMapHolder(Field f) {
        return new HashMap();
    }

}
