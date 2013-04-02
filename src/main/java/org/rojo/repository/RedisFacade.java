package org.rojo.repository;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.rojo.exceptions.RepositoryError;
import org.rojo.repository.converters.Converters;
import org.rojo.repository.utils.IdUtil;
import redis.clients.jedis.Jedis;

public class RedisFacade implements KeyValueStore {

    private Jedis redisClient;
    private Converters converters;

    public RedisFacade(Jedis jrClient, Converters converters) {
        super();
        this.redisClient = jrClient;
        this.converters = converters;
    }

    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#getReferredId(T, long, java.lang.reflect.Field)
     */
    @Override
    public <T> long getReferredId(T type, long id, Field field) {
        return IdUtil.decodeId(redisClient.get(keyForField(type.getClass(), id, field)));
    }

    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#readValue(T, long, java.lang.reflect.Field)
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T readValue(T type, long id, Field field) {
        return (T) converters.getConverterFor(field.getType()).decode(redisClient.get(keyForField(type.getClass(), id, field)));
    }

    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#readValues(T, long, java.lang.reflect.Field, java.util.Collection)
     */
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T> void readValues(T type, long id, Field field, Collection<T> destination) {
        List<String> values = redisClient.lrange(keyForField(type.getClass(), id, field), 0, -1);
        for (String value : values) {
            destination.add((T) converters.getConverterFor((Class)((java.lang.reflect.ParameterizedType)field.getGenericType()).getActualTypeArguments()[0]).decode(value));
        }
    }


    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#getReferredIds(T, long, java.lang.reflect.Field)
     */
    @Override
    public <T> List<Long> getReferredIds(T type, long id, Field field) {
        List<String> refs = redisClient.lrange(keyForField(type.getClass(), id, field), 0, -1);
        List<Long> ids = new ArrayList<Long>(refs.size());
        for (String value : refs) {
            ids.add(IdUtil.decodeId(value));
        }
        return ids;
    }

    private String keyForId(Class<? extends Object> entity, long id) {
        return entity.getCanonicalName().toLowerCase() + ":" + id;
    }

    private String keyForField(Class<? extends Object> entity, long id, Field field) {
        return entity.getCanonicalName().toLowerCase() + ":" + id + ":" + field.getName().toLowerCase();
    }

    private String keyForMetaData(Class<? extends Object> entity, String key) {
        return entity.getCanonicalName().toLowerCase() + ":::" + key;
    }

    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#write(java.lang.Object, long, java.lang.reflect.Field)
     */
    @Override
    public void write(Object entity, long id, Field field) {
        try {
            Object value = field.get(entity);
            redisClient.set(keyForField(entity.getClass(), id, field), converters.getConverterFor(value.getClass()).encode(value));
        } catch (Exception e) {
            throw new RepositoryError("write error " + entity.getClass() + " - " + id + " - " + field.getName(), e);
        }
    }



    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#writeCollection(java.lang.Object, java.util.Collection, long, java.lang.reflect.Field)
     */
    @Override
    public void writeCollection(Object entity,
                                Collection<? extends Object> collection, long id, Field field) {
        for (Object value : collection) {
            redisClient.lpush(keyForField(entity.getClass(), id, field), converters.getConverterFor(value.getClass()).encode(value));
        }
    }

    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#writeReference(java.lang.Object, java.lang.reflect.Field, long, long)
     */
    @Override
    public void writeReference(Object entity, Field field, long id, long referencedId) {
        redisClient.set(keyForField(entity.getClass(), id, field), IdUtil.encodeId(referencedId));
    }

    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#writeReferenceCollection(java.lang.Object, java.lang.reflect.Field, long, java.util.List)
     */
    @Override
    public void writeReferenceCollection(Object entity, Field field, long id,
                                         List<Long> referredIds) {
        for (Long referredId : referredIds) {
            redisClient.lpush(keyForField(entity.getClass(), id, field), IdUtil.encodeId(referredId));
        }
    }

    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#nextId(java.lang.Class)
     */
    @Override
    public long nextId(Class<? extends Object> entity) {
        return redisClient.incr(keyForMetaData(entity, "nextid"));
    }

    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#hasKey(java.lang.Class, long, java.lang.reflect.Field)
     */
    @Override
    public boolean hasKey(Class<? extends Object> entity, long id, Field field) {
        return redisClient.exists(keyForField(entity, id, field));
    }

    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#delete(java.lang.Object, long, java.lang.reflect.Field)
     */
    @Override
    public void delete(Object entity, long id, Field field) {
        redisClient.del(keyForField(entity.getClass(), id, field));
    }

    @Override
    public void writeId(Object entity, long id) {
        redisClient.set(keyForId(entity.getClass(), id), "");
    }

    @Override
    public boolean exists(Object entity, long id) {
        return redisClient.exists(keyForId(entity.getClass(), id));
    }

    @Override
    public void removeId(Object entity, long id) {
        redisClient.del(keyForId(entity.getClass(), id));
    }


}
