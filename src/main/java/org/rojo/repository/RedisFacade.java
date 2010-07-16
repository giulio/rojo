package org.rojo.repository;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jredis.RedisException;
import org.jredis.ri.alphazero.JRedisClient;
import org.rojo.exceptions.RepositoryError;
import org.rojo.repository.converters.Converters;

public class RedisFacade {

    private JRedisClient jrClient;
    private Converters converters;

    public RedisFacade(JRedisClient jrClient, Converters converters) {
        super();
        this.jrClient = jrClient;
        this.converters = converters;
    }

    public <T> long getReferredId(T type, long id, Field field) {
        try {
            return decodeId(jrClient.get(makeLabel(type.getClass(), id, field)));
        } catch (RedisException e) {
            throw new RepositoryError(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readValue(T type, long id, Field field) {
        try {
            return (T) converters.getConverterFor(field.getType()).decode(jrClient.get(makeLabel(type.getClass(), id, field)));
        } catch (RedisException e) {
            throw new RepositoryError(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> void readValues(T type, long id, Field field, Collection<T> destination) {
        try {
            List<byte[]> values = jrClient.lrange(makeLabel(type.getClass(), id, field), 0, -1);
            for (byte[] value : values) {
                destination.add((T) converters.getConverterFor((Class)((java.lang.reflect.ParameterizedType)field.getGenericType()).getActualTypeArguments()[0]).decode(value));
            }
        } catch (RedisException e) {
            throw new RepositoryError(e);
        }
    }


    public <T> List<Long> getReferredIds(T type, long id, Field field) {
        try {
            List<byte[]> refs = jrClient.lrange(makeLabel(type.getClass(), id, field), 0, -1);
            List<Long> ids = new ArrayList<Long>(refs.size());
            for (byte[] value : refs) {
                ids.add(decodeId(value));
            }
            return ids;
        } catch (RedisException e) {
            throw new RepositoryError(e);
        }
    }

    private String makeLabel(Class<? extends Object> entity, long id, Field field) {
        return entity.getCanonicalName().toLowerCase() + ":" + id + ":" + field.getName().toLowerCase();
    }

    private long decodeId(byte[] bytes) {
        return Long.parseLong(new String(bytes));
    }

    public void write(Object entity, long id, Field field) {
        try {
            Object value = field.get(entity);
            jrClient.set(makeLabel(entity.getClass(), id, field), converters.getConverterFor(value.getClass()).encode(value));
        } catch (Exception e) {
            throw new RepositoryError("write error " + entity.getClass() + " - " + id + " - " + field.getName(), e);
        }
    }

    public void writeList(Class<? extends Object> entity,
            Collection<? extends Object> collection, long id, Field field) {
        for (Object value : collection) {
            try {
                jrClient.lpush(makeLabel(entity, id, field) + ":" + id + ":" + field.getName(), converters.getConverterFor(value.getClass()).encode(value));
            } catch (RedisException e) {
                throw new RepositoryError("write error " + entity + " - " + id + " - " + field.getName(), e);
            }
        }
        
    }

}
