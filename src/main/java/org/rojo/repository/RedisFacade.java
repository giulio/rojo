package org.rojo.repository;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.rojo.annotations.Index;
import org.rojo.annotations.Value;
import org.rojo.exceptions.InvalidTypeException;
import org.rojo.exceptions.RepositoryError;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class RedisFacade
{

	private final Jedis je;
	private final Pipeline pipe;

	public RedisFacade(Jedis jrClient)
	{
		this.je = jrClient;
		pipe = je.pipelined();
	}

	private String keyForId(Class<? extends Object> entity, long id)
	{
		return entity.getSimpleName() + ":" + id;
	}

	/* (non-Javadoc)
	 * @see org.rojo.repository.KeyValueStore#readValue(T, long, java.lang.reflect.Field)
	 */
	@SuppressWarnings("unchecked")
	public <T> T readValue(Class claz, long id, Field field)
	{
		String v = je.get(keyForField(claz, id, field));
		Class<?> t = field.getType();
		return decode(t, v);
	}
	/* (non-Javadoc)
	 * @see org.rojo.repository.KeyValueStore#readValues(T, long, java.lang.reflect.Field, java.util.Collection)
	 */

	public <T> void readValues(Class claz, long id, Field field, Collection<T> destination)
	{
		List<String> values = je.lrange(keyForField(claz, id, field), 0, -1);
		for (String value : values)
		{
			destination.add((T) decode((Class) ((java.lang.reflect.ParameterizedType) field.getGenericType()).getActualTypeArguments()[0], value));
		}
	}

	<K, V> void readValues(Class claz, long id, Field f, Map<K, V> dest)
	{
		Map<String, String> values = je.hgetAll(keyForField(claz, id, f));
		for (Map.Entry<String, String> en : values.entrySet())
		{
			Type[] ts = ((java.lang.reflect.ParameterizedType) f.getGenericType()).getActualTypeArguments();
			Class keyClaz = (Class) ts[0];
			Class valueClaz = (Class) ts[1];
			dest.put((K) decode(keyClaz, en.getKey()), (V) decode(valueClaz, en.getValue()));
		}
	}

	/**
	 * all fields
	 *
	 * @param <T>
	 * @param entity
	 * @param id
	 * @param fields
	 */
	<T> void processFields(T entity, long id, Field[] fields) throws Exception
	{
		Response[] rs = new Response[fields.length];
		Class claz = entity.getClass();
		for (int i = 0; i < rs.length; i++)
		{
			rs[i] = readFuture(claz, id, fields[i]);
		}
		sync();
		for (int i = 0; i < rs.length; i++)
		{
			if (Collection.class.isAssignableFrom(fields[i].getType()))
			{
				Collection holder = initCollectionHolder(fields[i]);
				List<String> r = (List<String>) rs[i].get();
				for (String value : r)
				{
					holder.add((T) decode((Class) ((java.lang.reflect.ParameterizedType) fields[i].getGenericType()).getActualTypeArguments()[0], value));
				}
				fields[i].set(entity, holder);
			} else if (Map.class.isAssignableFrom(fields[i].getType()))
			{
				Map map = initMapHolder(fields[i]);
				Map<String, String> values = (Map<String, String>) rs[i].get();
				Type[] ts = ((java.lang.reflect.ParameterizedType) fields[i].getGenericType()).getActualTypeArguments();
				for (Map.Entry<String, String> en : values.entrySet())
				{
					Class keyClaz = (Class) ts[0];
					Class valueClaz = (Class) ts[1];
					map.put(decode(keyClaz, en.getKey()), decode(valueClaz, en.getValue()));
				}
				fields[i].set(entity, map);
			} else
			{
				fields[i].set(entity, decode(fields[i].getType(), (String) rs[i].get()));
			}
		}
	}

	private String keyForField(Class<? extends Object> entity, long id, Field field)
	{
		return entity.getSimpleName() + ":" + id + ":" + field.getName();
	}

	private String keyForSorted(Class claz, Field field)
	{
		return claz.getSimpleName() + ":" + field.getName() + ":sort";
	}

	private String keyForUnique(Class claz, Field field, String v)
	{
		return claz.getSimpleName() + ":" + field.getName() + ":" + v;
	}

	private String keyForIndex(Class claz, Field field, String v)
	{
		return claz.getSimpleName() + ":" + field.getName() + ":" + v;
	}

	/**
	 * write object
	 *
	 * @param entity
	 * @param id
	 * @param field
	 * @return
	 */
	boolean write(Object entity, long id, Field field)
	{
		try
		{
			final Object value = field.get(entity);
			pipe.set(keyForField(entity.getClass(), id, field), value.toString());
			Value annotation = field.getAnnotation(Value.class);
			if (annotation.sort())
			{
				final String key = keyForSorted(entity.getClass(), field);
				pipe.zadd(key, toDouble(value), String.valueOf(id));
				if (annotation.size() > 0)
				{
					if (annotation.bigFirst())
					{
						pipe.zremrangeByRank(key, 0, -annotation.size() - 1);
					} else
					{
						pipe.zremrangeByRank(key, annotation.size(), -1);
					}
				}
			}
			if (!annotation.unique())
			{
				Index index = field.getAnnotation(Index.class);
				if (index != null)//indexing
				{
					pipe.sadd(keyForIndex(entity.getClass(), field, value.toString()), String.valueOf(id));
				}
			}
			return true;
		} catch (Exception e)
		{
			return false;
		}
	}

	/**
	 * no way for unique or index
	 *
	 * @param claz
	 * @param id
	 * @param field
	 * @param v
	 */
	void write(Class claz, long id, Field field, Object v)
	{
		try
		{
			pipe.set(keyForField(claz, id, field), v.toString());
			Value annotation = field.getAnnotation(Value.class);
			if (annotation.sort())
			{
				final String key = keyForSorted(claz, field);
				pipe.zadd(key, toDouble(v), String.valueOf(id));
				if (annotation.size() > 0)
				{
					if (annotation.bigFirst())
					{
						pipe.zremrangeByRank(key, 0, -annotation.size() - 1);
					} else
					{
						pipe.zremrangeByRank(key, annotation.size(), -1);
					}
				}
			}
		} catch (Exception e)
		{
			throw new RepositoryError("write error " + claz + " - " + id + " - " + field.getName(), e);
		}
	}

	/**
	 * collection
	 *
	 * @param claz
	 * @param collection
	 * @param id
	 * @param field
	 */
	void writeCollection(Class claz, Collection<? extends Object> collection, long id, Field field)
	{
		for (Object value : collection)
		{
			pipe.rpush(keyForField(claz, id, field), value.toString());
		}
	}

	/**
	 * map
	 *
	 * @param claz
	 * @param map
	 * @param id
	 * @param field
	 */
	void writeMap(Class<? extends Object> claz, Map<Object, Object> map, long id, Field field)
	{
		for (Map.Entry<Object, Object> en : map.entrySet())
		{
			pipe.hset(keyForField(claz, id, field), en.getKey().toString(), en.getValue().toString());
		}
	}

	/**
	 * delete
	 *
	 * @param claz
	 * @param id
	 * @param field
	 */
	void delete(Class claz, long id, Field field)
	{
		pipe.del(keyForField(claz, id, field));
		Value annotation = field.getAnnotation(Value.class);
		if (annotation.sort())
		{
			pipe.zrem(keyForSorted(claz, field), String.valueOf(id));
		}
	}

	void writeId(Class claz, long id)
	{
		pipe.set(keyForId(claz, id), "");
	}

	long incr(String s)
	{
		return je.incr(s);
	}

	public boolean exists(Class claz, long id)
	{
		return je.exists(keyForId(claz, id));
	}

	boolean uniqueExists(Class claz, Field f, String v)
	{
		return je.exists(keyForUnique(claz, f, v));
	}

	public void removeId(Class claz, long id)
	{
		pipe.del(keyForId(claz, id));
	}

	void flush()
	{
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
	public static <T> T decode(Class<?> t, String v)
	{
		if (t == Integer.class || t == int.class)
		{
			return isEmpty(v) ? (T) (Integer) 0 : (T) (Integer) Integer.parseInt(v);
		}
		if (t == String.class)
		{
			return (T) v;
		}
		if (t == Long.class || t == long.class)
		{
			return isEmpty(v) ? (T) (Long) 0L : (T) (Long) Long.parseLong(v);
		}
		if (t == Float.class || t == float.class)
		{
			return isEmpty(v) ? (T) (Float) 0f : (T) (Float) Float.parseFloat(v);
		}
		if (t == Double.class || t == double.class)
		{
			return isEmpty(v) ? (T) (Double) 0.0 : (T) (Double) Double.parseDouble(v);
		}
		if (t == Short.class || t == short.class)
		{
			return isEmpty(v) ? (T) (Short) (short) 0 : (T) (Short) Short.parseShort(v);
		}
		if (t == Byte.class || t == byte.class)
		{
			return isEmpty(v) ? (T) (Byte) (byte) 0 : (T) (Byte) Byte.parseByte(v);
		}
		throw new InvalidTypeException("不支持的类型：" + t);
	}

	private double toDouble(Object value)
	{
		if (value instanceof Integer)
		{
			return (Integer) value;
		}
		if (value instanceof Long)
		{
			return (Long) value;
		}
		if (value instanceof Float)
		{
			return (Float) value;
		}
		if (value instanceof Double)
		{
			return (Double) value;
		}
		if (value instanceof Short)
		{
			return (Short) value;
		}
		if (value instanceof Byte)
		{
			return (Byte) value;
		}
		throw new InvalidTypeException("不支持的类型：" + value.getClass());
	}

	private static boolean isEmpty(String v)
	{
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
	Set<String> range(Class claz, Field f, long start, long end)
	{
		String key = this.keyForSorted(claz, f);
		Set<String> s = null;
		Value annotation = f.getAnnotation(Value.class);
		if (annotation != null && annotation.sort())
		{
			if (annotation.bigFirst())
			{
				s = je.zrevrange(key, start, end);
			} else
			{
				s = je.zrange(key, start, end);
			}
		}
		return s;
	}

	/**
	 * index of rank
	 *
	 * @param claz
	 * @param f
	 * @param id
	 * @return
	 */
	long rank(Class claz, Field f, long id)
	{
		Long index = null;
		Value annotation = f.getAnnotation(Value.class);
		if (annotation != null && annotation.sort())
		{
			String key = this.keyForSorted(claz, f);
			if (annotation.bigFirst())
			{
				index = je.zrevrank(key, String.valueOf(id));
			} else
			{
				index = je.zrank(key, String.valueOf(id));
			}
		}
		return index == null ? -1L : index;
	}

	boolean writeUnique(Object entity, Field unique, long id)
	{
		Object v;
		try
		{
			v = unique.get(entity);
			if (v == null)
			{
				return false;
			}
		} catch (Exception e)
		{
			return false;
		}
		return je.setnx(keyForUnique(entity.getClass(), unique, v.toString()), String.valueOf(id)) > 0;
	}

	/**
	 * get the future
	 *
	 * @param claz
	 * @param id
	 * @param field
	 * @return
	 */
	Response readFuture(Class claz, long id, Field field)
	{
		if (Collection.class.isAssignableFrom(field.getType()))
		{
			return pipe.lrange(keyForField(claz, id, field), 0, -1);
		} else if (Map.class.isAssignableFrom(field.getType()))
		{
			return pipe.hgetAll(keyForField(claz, id, field));
		} else
		{
			return pipe.get(keyForField(claz, id, field));
		}
	}

	/**
	 * sync the future
	 *
	 */
	void sync()
	{
		this.pipe.sync();
	}

	static Collection initCollectionHolder(Field field)
	{
		if (field.getType() == List.class || field.getType() == Collection.class)
		{
			return new ArrayList();
		} else if (field.getType() == Set.class)
		{
			return new HashSet();
		} else
		{
			throw new InvalidTypeException("unsupported Collection subtype");
		}
	}

	static Map initMapHolder(Field f)
	{
		return new HashMap();
	}

	/**
	 * remove unique field
	 *
	 * @param entity
	 * @param unique
	 */
	void removeUnique(Object entity, Field unique)
	{
		try
		{
			pipe.del(keyForUnique(entity.getClass(), unique, unique.get(entity).toString()));
		} catch (Exception e)
		{
		}
	}

	/**
	 * delete index
	 *
	 * @param claz
	 * @param field
	 * @param v
	 * @param id
	 */
	void deleteIndex(Class claz, Field field, String v, long id)
	{
		pipe.srem(keyForIndex(claz, field, v), String.valueOf(id));
	}

	/**
	 * indexing ids
	 *
	 * @param claz
	 * @param p
	 * @param v
	 * @return
	 */
	Set<Long> index(Class claz, String p, Object v)
	{
		String key = claz.getSimpleName() + ":" + p + ":" + v;
		Set<String> set = je.smembers(key);
		Set<Long> r = new HashSet<Long>();
		for (String item : set)
		{
			r.add(Long.parseLong(item));
		}
		return r;
	}

}
