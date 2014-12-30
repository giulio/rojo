package org.rojo.repository;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.rojo.annotations.Value;
import org.rojo.exceptions.InvalidTypeException;
import org.rojo.exceptions.RepositoryError;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisFacade {

    private final Jedis je;
    private final Pipeline pipe;

    public RedisFacade(Jedis jrClient) {
        this.je = jrClient;
        pipe = je.pipelined();
    }

    private String keyForId(Class<? extends Object> entity, long id) {
        return entity.getSimpleName() + ":" + id;
    }
    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#readValue(T, long, java.lang.reflect.Field)
     */

    @SuppressWarnings("unchecked")
    public <T> T readValue(Class claz, long id, Field field) {
        String v = je.get(keyForField(claz, id, field));
        Class<?> t = field.getType();
        return decode(t, v);
    }
    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#readValues(T, long, java.lang.reflect.Field, java.util.Collection)
     */

    public <T> void readValues(Class claz, long id, Field field, Collection<T> destination) {
        List<String> values = je.lrange(keyForField(claz, id, field), 0, -1);
        for (String value : values) {
            destination.add((T) decode((Class) ((java.lang.reflect.ParameterizedType) field.getGenericType()).getActualTypeArguments()[0], value));
        }
    }

    <K, V> void readValues(Class claz, long id, Field f, Map<K, V> dest) {
        Map<String, String> values = je.hgetAll(keyForField(claz, id, f));
        for (Map.Entry<String, String> en : values.entrySet()) {
            Type[] ts = ((java.lang.reflect.ParameterizedType) f.getGenericType()).getActualTypeArguments();
            Class keyClaz = (Class) ts[0];
            Class valueClaz = (Class) ts[1];
            dest.put((K) decode(keyClaz, en.getKey()), (V) decode(valueClaz, en.getValue()));
        }
    }

    private String keyForField(Class<? extends Object> entity, long id, Field field) {
        return entity.getSimpleName() + ":" + id + ":" + field.getName();
    }

    private String keyForSorted(Class claz, Field field) {
        return claz.getSimpleName() + ":" + field.getName() + ":sort";
    }

    private String keyForUnique(Class claz, Field field, String v) {
        return claz.getSimpleName() + ":" + field.getName() + ":" + v;
    }
    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#write(java.lang.Object, long, java.lang.reflect.Field)
     */

    boolean write(Object entity, long id, Field field) {
        try {
            final Object value = field.get(entity);
            pipe.set(keyForField(entity.getClass(), id, field), value.toString());
            Value annotation = field.getAnnotation(Value.class);
            if (annotation.sort()) {
                final String key = keyForSorted(entity.getClass(), field);
                pipe.zadd(key, toDouble(value), String.valueOf(id));
                if (annotation.size() > 0) {
                    if (annotation.bigFirst()) {
                        pipe.zremrangeByRank(key, 0, -annotation.size() - 1);
                    } else {
                        pipe.zremrangeByRank(key, annotation.size(), -1);
                    }
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    void write(Class claz, long id, Field field, Object v) {
        try {
            pipe.set(keyForField(claz, id, field), v.toString());
            Value annotation = field.getAnnotation(Value.class);
            if (annotation.sort()) {
                final String key = keyForSorted(claz, field);
                pipe.zadd(key, toDouble(v), String.valueOf(id));
                if (annotation.size() > 0) {
                    if (annotation.bigFirst()) {
                        pipe.zremrangeByRank(key, 0, -annotation.size() - 1);
                    } else {
                        pipe.zremrangeByRank(key, annotation.size(), -1);
                    }
                }
            }
        } catch (Exception e) {
            throw new RepositoryError("write error " + claz + " - " + id + " - " + field.getName(), e);
        }
    }
    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#writeCollection(java.lang.Object, java.util.Collection, long, java.lang.reflect.Field)
     */

    void writeCollection(Class claz, Collection<? extends Object> collection, long id, Field field) {
        for (Object value : collection) {
            pipe.rpush(keyForField(claz, id, field), value.toString());
        }
    }

    void writeMap(Class<? extends Object> claz, Map<Object, Object> map, long id, Field field) {
        for (Map.Entry<Object, Object> en : map.entrySet()) {
            pipe.hset(keyForField(claz, id, field), en.getKey().toString(), en.getValue().toString());
        }
    }
    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#hasKey(java.lang.Class, long, java.lang.reflect.Field)
     */

    public boolean hasKey(Class<? extends Object> entity, long id, Field field) {
        return je.exists(keyForField(entity, id, field));
    }

    /* (non-Javadoc)
     * @see org.rojo.repository.KeyValueStore#delete(java.lang.Object, long, java.lang.reflect.Field)
     */
    public void delete(Class claz, long id, Field field) {
        pipe.del(keyForField(claz, id, field));
        Value annotation = field.getAnnotation(Value.class);
        if (annotation.sort()) {
            pipe.zrem(keyForSorted(claz, field), String.valueOf(id));
        }
    }

    void writeId(Class claz, long id) {
        pipe.set(keyForId(claz, id), "");
    }

    long incr(String s) {
        return je.incr(s);
    }

    public boolean exists(Class claz, long id) {
        return je.exists(keyForId(claz, id));
    }

    boolean uniqueExists(Class claz, Field f, String v) {
        return je.exists(keyForUnique(claz, f, v));
    }

    public void removeId(Class claz, long id) {
        pipe.del(keyForId(claz, id));
    }

    void flush() {
        pipe.sync();
    }

    /**
     * decode
     *
     * @param <T>
     * @param t
     * @param v
     * @return
     */
    private <T> T decode(Class<?> t, String v) {
        if (t == Integer.class || t == int.class) {
            return isEmpty(v) ? (T) (Integer) 0 : (T) (Integer) Integer.parseInt(v);
        }
        if (t == String.class) {
            return (T) v;
        }
        if (t == Float.class || t == float.class) {
            return isEmpty(v) ? (T) (Float) 0f : (T) (Float) Float.parseFloat(v);
        }
        if (t == Double.class || t == double.class) {
            return isEmpty(v) ? (T) (Double) 0.0 : (T) (Double) Double.parseDouble(v);
        }
        if (t == Short.class || t == short.class) {
            return isEmpty(v) ? (T) (Short) (short) 0 : (T) (Short) Short.parseShort(v);
        }
        if (t == Byte.class || t == byte.class) {
            return isEmpty(v) ? (T) (Byte) (byte) 0 : (T) (Byte) Byte.parseByte(v);
        }
        throw new InvalidTypeException("不支持的类型：" + t);
    }

    private double toDouble(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Float) {
            return (Float) value;
        }
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof Short) {
            return (Short) value;
        }
        if (value instanceof Byte) {
            return (Byte) value;
        }
        throw new InvalidTypeException("不支持的类型：" + value.getClass());
    }

    private static boolean isEmpty(String v) {
        return v == null || v.isEmpty();
    }

    /**
     * rank
     *
     * @param claz
     * @param f
     * @param start
     * @param end
     * @return
     */
    Set<String> rank(Class claz, Field f, long start, long end) {
        String key = this.keyForSorted(claz, f);
        Set<String> s = je.zrange(key, start, end);
        return s;
    }

    boolean writeUnique(Object entity, Field unique, long id) {
        Object v;
        try {
            v = unique.get(entity);
            if (v == null) {
                return false;
            }
        } catch (Exception e) {
            return false;
        }
        return je.setnx(keyForUnique(entity.getClass(), unique, v.toString()), String.valueOf(id)) > 0;
    }
}
