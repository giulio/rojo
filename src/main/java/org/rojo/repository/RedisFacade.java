package org.rojo.repository;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.rojo.annotations.Index;
import org.rojo.annotations.Value;
import org.rojo.exceptions.InvalidTypeException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class RedisFacade
{

  private final Jedis je;
  private final Pipeline pipe;
  private static final String FOR_ALLFIELD_KEY = "001all_propertiesHashMap_key";
  private static final String FOR_ALL_IDS_SET_KEY = "002all_ids_key";
  private static final String FOR_SORTED_KEY = "003sorted_key";
  private static final String FOR_UNIQUE_KEY = "004unique_key";
  private static final String FOR_INDEX_KEY = "005indexing_key";
  private static final String NULL = "#*%$NULL@&%$#*{)}}(";//null

  public RedisFacade(Jedis jrClient)
  {
    this.je = jrClient;
    pipe = je.pipelined();
  }

  /**
   * read a proterty
   *
   * @param <T>
   * @param table
   * @param id
   * @param column
   * @param field
   * @return
   * @throws java.io.UnsupportedEncodingException
   */
  @SuppressWarnings("unchecked")
  public <T> T readValue(String table, String id, String column, Field field) throws UnsupportedEncodingException
  {
    byte[] v = je.hget(keyForAllField(table, id).getBytes("UTF-8"), column.getBytes("UTF-8"));
    Class<?> t = field.getType();
    return decode(t, v);
  }

  /**
   * read collection
   *
   * @param <T>
   * @param table
   * @param id
   * @param column
   * @param field
   * @param destination
   */
  public <T> void readValues(String table, String id, String column, Field field, Collection<T> destination)
  {
    List<String> values = je.lrange(keyForField(table, id, column), 0, -1);
    for (String value : values)
    {
      destination.add((T) decode((Class) ((java.lang.reflect.ParameterizedType) field.getGenericType()).getActualTypeArguments()[0], value));
    }
  }

  <K, V> void readValues(String table, String id, String column, Field f, Map<K, V> dest)
  {
    Map<String, String> values = je.hgetAll(keyForField(table, id, column));
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
  <T> void processFields(T entity, EntityRepresentation representation, String id) throws Exception
  {
    Field[] fields = representation.getFields();
    String[] columns = representation.getColumns();
    String table = representation.getTable();
    Response[] rs = new Response[fields.length];
    for (int i = 0; i < rs.length; i++)
    {
      rs[i] = readFuture(table, id, columns[i], fields[i]);
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
          holder.add(decode((Class) ((java.lang.reflect.ParameterizedType) fields[i].getGenericType()).getActualTypeArguments()[0], value));
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
          Object k = decode(keyClaz, en.getKey());
          if (k != null)
          {
            map.put(k, decode(valueClaz, en.getValue()));
          }
        }
        fields[i].set(entity, map);
      } else
      {
        Object temp = decode(fields[i].getType(), rs[i].get());
        if (temp != null)
        {
          fields[i].set(entity, temp);
        }
      }
    }
  }

  private String keyForAll(String table)
  {
    return table + ":" + FOR_ALL_IDS_SET_KEY;
  }

  private String keyForField(String table, String id, String column)
  {
    return table + ":" + id + ":" + column;
  }

  private String keyForAllField(String table, String id)
  {
    return table + ":" + id + ":" + FOR_ALLFIELD_KEY;
  }

  private String keyForSorted(String table, String column)
  {
    return table + ":" + column + ":" + FOR_SORTED_KEY;
  }

  private String keyForUnique(String table, String column, String v)
  {
    return table + ":" + column + ":" + v + ":" + FOR_UNIQUE_KEY;
  }

  private String keyForIndex(String table, String column, String v)
  {
    return table + ":" + column + ":" + v + ":" + FOR_INDEX_KEY;
  }

  /**
   * write object
   *
   * @param entity
   * @param id
   * @param field
   * @return
   */
  boolean write(String table, String id, String column, Object v, Field field, boolean withIndexing)
  {
    try
    {
      pipe.hset(keyForAllField(table, id), column, v.toString());
      Value annotation = field.getAnnotation(Value.class);
      if (annotation.sort())
      {
        final String key = keyForSorted(table, column);
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
      if (withIndexing)
      {
        if (!annotation.unique())
        {
          Index index = field.getAnnotation(Index.class);
          if (index != null)//indexing
          {
            pipe.zadd(keyForIndex(table, column, v.toString()), System.currentTimeMillis(), String.valueOf(id));
          }
        }
      }
      return true;
    } catch (Exception e)
    {
      return false;
    }
  }

  /**
   * blob type
   *
   * @param table
   * @param id
   * @param column
   * @param v
   * @param field
   * @return
   */
  boolean writeBlob(String table, String id, String column, byte[] v, Field field)
  {
    try
    {
      if (v != null)
      {
        pipe.hset(keyForAllField(table, id).getBytes("UTF-8"), column.getBytes("UTF-8"), v);
      } else
      {
        pipe.hdel(keyForAllField(table, id).getBytes("UTF-8"), column.getBytes("UTF-8"));
      }
      return true;
    } catch (Exception e)
    {
      return false;
    }
  }

  boolean update(String table, String id, String column, Object v, Field field)
  {
    try
    {
      Value annotation = field.getAnnotation(Value.class);
      if (annotation.unique() || field.isAnnotationPresent(Index.class))
      {
        return false;
      }
      if (v != null)
      {
        pipe.hset(keyForAllField(table, id), column, v.toString());
        if (annotation.sort())
        {
          final String key = keyForSorted(table, column);
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
      } else
      {
        pipe.hdel(keyForAllField(table, id), column);
      }
      return true;
    } catch (Exception e)
    {
      return false;
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
  void writeCollection(String table, Collection<? extends Object> collection, String id, String column)
  {
    String key = keyForField(table, id, column);
    pipe.del(key);
    if (collection != null)
    {
      String[] vs = new String[collection.size()];
      int i = 0;
      for (Object value : collection)
      {
        vs[i++] = value == null ? NULL : value.toString();
      }
      pipe.rpush(key, vs);
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
  void writeMap(String table, Map<Object, Object> map, String id, String column)
  {
    String key = keyForField(table, id, column);
    pipe.del(key);
    if (map != null)
    {
      Map<String, String> m = new HashMap();
      for (Map.Entry<Object, Object> en : map.entrySet())
      {
        Object k = en.getKey();
        if (k != null)
        {
          m.put(k.toString(), en.getValue() == null ? NULL : en.getValue().toString());
        }
      }
      pipe.hmset(key, m);
    }
  }

  /**
   * delete
   *
   * @param claz
   * @param id
   * @param field
   */
  void delete(String table, String id, String column, Field field)
  {
    if (Collection.class.isAssignableFrom(field.getType()) || Map.class.isAssignableFrom(field.getType()))
    {
      pipe.del(keyForField(table, id, column));
    }
    Value annotation = field.getAnnotation(Value.class);
    if (annotation != null && annotation.sort())
    {
      pipe.zrem(keyForSorted(table, column), String.valueOf(id));
    }
  }

  void delete(String table, String id)
  {
    pipe.del(keyForAllField(table, id));
  }

  long incr(String s)
  {
    return je.incr(s);
  }

  boolean exists(String table, String id)
  {
    return je.exists(keyForAllField(table, id));
  }

  boolean uniqueExists(String table, String column, String v)
  {
    return je.exists(keyForUnique(table, column, v));
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
   * @param o
   * @return
   */
  public static <T> T decode(Class<?> t, Object o)
  {
    if (t == byte[].class)
    {
      return isNull(o) ? null : (T) o;
    }
    String v = (String) o;
    if (t == Integer.class)
    {
      return isNull(v) ? null : (T) (Integer) Integer.parseInt(v);
    }
    if (t == int.class)
    {
      return isNull(v) ? (T) new Integer(0) : (T) (Integer) Integer.parseInt(v);
    }
    if (t == String.class)
    {
      return isNull(v) ? null : (T) v;
    }
    if (t == Long.class)
    {
      return isNull(v) ? null : (T) (Long) Long.parseLong(v);
    }
    if (t == long.class)
    {
      return isNull(v) ? (T) new Long(0) : (T) (Long) Long.parseLong(v);
    }
    if (t == Float.class)
    {
      return isNull(v) ? null : (T) (Float) Float.parseFloat(v);
    }
    if (t == float.class)
    {
      return isNull(v) ? (T) new Float(0) : (T) (Float) Float.parseFloat(v);
    }
    if (t == Double.class)
    {
      return isNull(v) ? null : (T) (Double) Double.parseDouble(v);
    }
    if (t == double.class)
    {
      return isNull(v) ? (T) new Double(0) : (T) (Double) Double.parseDouble(v);
    }
    if (t == Short.class)
    {
      return isNull(v) ? null : (T) (Short) Short.parseShort(v);
    }
    if (t == short.class)
    {
      return isNull(v) ? (T) new Short((short) 0) : (T) (Short) Short.parseShort(v);
    }
    if (t == Byte.class)
    {
      return isNull(v) ? null : (T) (Byte) Byte.parseByte(v);
    }
    if (t == byte.class)
    {
      return isNull(v) ? (T) new Byte((byte) 0) : (T) (Byte) Byte.parseByte(v);
    }
    if (t == Boolean.class)
    {
      return isNull(v) ? null : (T) (Boolean) Boolean.parseBoolean(v);
    }
    if (t == boolean.class)
    {
      return isNull(v) ? (T) Boolean.FALSE : (T) (Boolean) Boolean.parseBoolean(v);
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

  private static boolean isNull(Object v)
  {
    boolean r = (v == null || v.equals(NULL));
    return r;
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
  Set<String> range(String table, String column, Field f, long start, long end)
  {
    String key = this.keyForSorted(table, column);
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
   * range by score
   *
   * @param table
   * @param column
   * @param f
   * @param start
   * @param end
   * @return
   */
  Set<String> scoreRange(String table, String column, Field f, double start, double end)
  {
    String key = this.keyForSorted(table, column);
    Set<String> s = null;
    Value annotation = f.getAnnotation(Value.class);
    if (annotation != null && annotation.sort())
    {
      if (annotation.bigFirst())
      {
        s = je.zrevrangeByScore(key, start, end);
      } else
      {
        s = je.zrangeByScore(key, start, end);
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
  long rank(String table, String column, Field f, String id)
  {
    Long index = null;
    Value annotation = f.getAnnotation(Value.class);
    if (annotation != null && annotation.sort())
    {
      String key = this.keyForSorted(table, column);
      if (annotation.bigFirst())
      {
        index = je.zrevrank(key, id);
      } else
      {
        index = je.zrank(key, id);
      }
    }
    return index == null ? -1L : index;
  }

  boolean writeUnique(Object entity, Field unique, String table, String column, String id)
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
    long r = je.setnx(keyForUnique(table, column, v.toString()), id);
    return r == 1;
  }

  /**
   * get the future
   *
   * @param claz
   * @param id
   * @param field
   * @return
   */
  Response readFuture(String table, String id, String column, Field field) throws UnsupportedEncodingException
  {
    if (Collection.class.isAssignableFrom(field.getType()))
    {
      return pipe.lrange(keyForField(table, id, column), 0, -1);
    } else if (Map.class.isAssignableFrom(field.getType()))
    {
      return pipe.hgetAll(keyForField(table, id, column));
    } else if (field.getType() == byte[].class)
    {
      return pipe.hget(keyForAllField(table, id).getBytes("UTF-8"), column.getBytes("UTF-8"));
    } else
    {
      return pipe.hget(keyForAllField(table, id), column);
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
      return new LinkedHashSet();
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
  void removeUnique(Object entity, Field unique, String table, String column)
  {
    try
    {
      pipe.del(keyForUnique(table, column, unique.get(entity).toString()));
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
  void deleteIndex(String table, String column, String v, String id)
  {
    pipe.zrem(keyForIndex(table, column, v), String.valueOf(id));
  }

  /**
   * index size
   *
   * @param table
   * @param column
   * @param v
   * @return
   */
  long indexSize(String table, String column, Object v)
  {
    String key = keyForIndex(table, column, v.toString());
    return je.zcard(key);
  }

  /**
   * indexing ids
   *
   * @param claz
   * @param p
   * @param v
   * @return
   */
  Set<String> index(String table, String column, Object v, long start, long end)
  {
    String key = keyForIndex(table, column, v.toString());
    Set<String> set = je.zrange(key, start, end);
    return set;
  }

  String unique(String table, String column, String v)
  {
    String key = this.keyForUnique(table, column, v);
    return je.get(key);
  }

  /**
   * all ids for t
   *
   * @param t
   * @return
   */
  Set<String> all(String table, long start, long end)
  {
    String key = keyForAll(table);
    return je.zrange(key, start, end);
  }

  /**
   * the collection of entities create between start and end
   *
   * @param c
   * @param start
   * @param end
   * @return
   */
  Set<String> all(String table, Date start, Date end)
  {
    String key = keyForAll(table);
    return je.zrangeByScore(key, start == null ? 0 : start.getTime(), end == null ? System.currentTimeMillis() : end.getTime());
  }

  /**
   * all size
   *
   * @param table
   * @return
   */
  long allSize(String table)
  {
    String key = keyForAll(table);
    return je.zcard(key);
  }

  void addId(String table, String id)
  {
    String key = keyForAll(table);
    pipe.zadd(key, System.currentTimeMillis(), id);
  }

  void deleteId(String table, String id)
  {
    String key = keyForAll(table);
    pipe.zrem(key, id);
  }

  Date createTime(String table, String id)
  {
    String key = keyForAll(table);
    Double d = je.zscore(key, id);
    if (d != null)
    {
      return new Date((long) (double) d);
    }
    return null;
  }

  Jedis getJedis()
  {
    return je;
  }

}
