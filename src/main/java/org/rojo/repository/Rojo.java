package org.rojo.repository;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import org.rojo.util.CacheoutListerner;
import org.rojo.util.SoftCache;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.rojo.annotations.Index;
import org.rojo.exceptions.RepositoryError;
import org.rojo.util.Cache;
import redis.clients.jedis.Jedis;

public class Rojo
{

  private static final Logger LOG = Logger.getLogger(Rojo.class.getName());
  private final RedisFacade store;
  private static Cache cache;
  private static int TIMES_CACHE_CLEAR;
  private static volatile long read;
  private static volatile long write;
  private static volatile boolean cacheable;//cache?

  static
  {
    /**
     * defaultGenerator init*
     */
    new DefaultGenerator().configue("defaultGenerator");
    try
    {
      TIMES_CACHE_CLEAR = Integer.parseInt(System.getProperty("rojo.times.cache.clear", "15000"));
      cacheable = Boolean.parseBoolean(System.getProperty("rojo.cacheable", "false"));
      String impl = System.getProperty("rojo.cache.impl", "org.rojo.util.LruCache");
      int size = Integer.parseInt(System.getProperty("rojo.cache.size", "150000"));
      if (cacheable)
      {
        Class c = Class.forName(impl);
        Constructor con = c.getConstructor(int.class);
        cache = (Cache) con.newInstance(size);
        if (cache instanceof SoftCache)
        {
          ((SoftCache) cache).setTIMES_CACHE_CLEAR(TIMES_CACHE_CLEAR);
        }
      }
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "some property are missing,default values will be applied");
    }
  }

  public Rojo(Jedis je)
  {
    store = new RedisFacade(je);
  }

  public Jedis getJedis()
  {
    return store.getJedis();
  }

  public static void setCache(Cache cache)
  {
    Rojo.cache = cache;
  }

  public static Cache getCache()
  {
    return cache;
  }

  /**
   * all entities of ranking from start to end .(order by create time asc)
   *
   * @param <T>
   * @param c
   * @param start index of zset
   * @param end index of zset
   * @return
   */
  public <T> Set<T> all(Class<T> c, long start, long end)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(c);
      String table = representation.getTable();
      Set<String> s = store.all(table, start, end);
      read++;
      Set<T> r = new LinkedHashSet<T>(s.size());
      for (String item : s)
      {
        r.add(this.get(c, item));
      }
      return r;
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
      return null;
    }
  }

  /**
   * all class size
   *
   * @param c
   * @param start
   * @param end
   * @return
   */
  public long allSize(Class c, Date start, Date end)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(c);
      String table = representation.getTable();
      Set<String> s = store.all(table, start, end);
      read++;
      return s.size();
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
      return -1;
    }
  }

  /**
   * all size
   *
   * @param c
   * @return
   */
  public long allSize(Class c)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(c);
      String table = representation.getTable();
      long l = store.allSize(table);
      read++;
      return l;
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
      return -1;
    }
  }

  /**
   * the collection of entities create between start and end
   *
   * @param <T>
   * @param c
   * @param start time of created
   * @param end time of created
   * @return
   */
  public <T> Set<T> all(Class<T> c, Date start, Date end)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(c);
      String table = representation.getTable();
      Set<String> s = store.all(table, start, end);
      read++;
      Set<T> r = new LinkedHashSet<T>(s.size());
      for (String item : s)
      {
        r.add(this.get(c, item));
      }
      return r;
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
      return null;
    }
  }

  /**
   * the create time of entity
   *
   * @param c
   * @param id
   * @return
   */
  public Date createTime(Class c, String id)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(c);
      String table = representation.getTable();
      read++;
      return store.createTime(table, id);
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
      return null;
    }
  }

  public void flush()
  {
    store.flush();
    write++;
  }

  public String saveAndFlush(Object entity)
  {
    String id;
    if ((id = save(entity)) != null)
    {
      store.flush();
      write++;
    }
    return id;
  }

  /**
   * write an entity
   *
   * @param entity
   * @return
   */
  private String save(Object entity)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(entity.getClass());
      String id = representation.getId(entity);
      String table = representation.getTable();
      boolean auto = representation.isAutoId();
      boolean idCache = representation.isIdCache();
      if (auto)
      {
        if (isEmpty(id))
        {
          id = representation.getIdGenerator().id(entity.getClass(), table, store.getJedis());
          write++;
        }
      }
      if (isEmpty(id))//null id
      {
        return null;
      }
      Field unique = representation.getUnique();
      if (unique != null)
      {
        boolean result = store.writeUnique(entity, unique, table, representation.getColumn(unique.getName()), id);
        if (!result)
        {
          return null;
        }
      }
      Field[] fs = representation.getFields();
      for (int i = 0; i < fs.length; i++)
      {
        Field field = fs[i];
        if (field.get(entity) != null)
        {
          if (Collection.class.isAssignableFrom(field.getType()))
          {
            Collection<? extends Object> collection = (Collection<? extends Object>) field.get(entity);
            store.writeCollection(table, collection, id, representation.getColumns()[i]);
          } else if (Map.class.isAssignableFrom(field.getType()))
          {
            Map map = (Map) field.get(entity);
            store.writeMap(table, map, id, representation.getColumns()[i]);
          } else
          {
            Object v = field.get(entity);
            if (v instanceof byte[])//blob
            {
              store.writeBlob(table, id, representation.getColumns()[i], (byte[]) v, field);
            } else
            {
              store.write(table, id, representation.getColumns()[i], v, field, true);
            }
          }
        }
      }
      representation.setId(entity, id);
      if (idCache)
      {
        store.addId(table, id);
      }
      return id;
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
      return null;
    }
  }

  /**
   * write properties
   *
   * @param entity
   * @param ps
   * @return
   */
  public boolean update(Object entity, String... ps)
  {
    try
    {
      Class claz = entity.getClass();
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      String table = representation.getTable();
      String id = representation.getId(entity);
      if (id != null)
      {
        for (String p : ps)
        {
          Field f = representation.getField(p);
          String column = representation.getColumn(p);
          Object v = representation.readProperty(entity, f);
          if (Collection.class.isAssignableFrom(f.getType()))
          {
            store.writeCollection(table, (Collection) v, id, column);
          } else if (Map.class.isAssignableFrom(f.getType()))
          {
            store.writeMap(table, (Map) v, id, column);
          } else if (f.getType() == byte[].class)//blob
          {
            store.writeBlob(table, id, column, (byte[]) v, f);
          } else
          {
            store.update(table, id, column, v, f);
          }
        }
        return true;
      }
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
    }
    return false;
  }

  /**
   * update all properties exclude unique
   *
   * @param entity
   * @return
   */
  public boolean update(Object entity)
  {
    try
    {
      Class claz = entity.getClass();
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      String table = representation.getTable();
      String id = representation.getId(entity);
      if (id != null)
      {
        for (String p : representation.getColumns())
        {
          Field f = representation.getField(p);
          String column = representation.getColumn(p);
          Object v = representation.readProperty(entity, f);
          if (Collection.class.isAssignableFrom(f.getType()))
          {
            store.writeCollection(table, (Collection) v, id, column);
          } else if (Map.class.isAssignableFrom(f.getType()))
          {
            store.writeMap(table, (Map) v, id, column);
          } else if (f.getType() == byte[].class)//blob
          {
            store.writeBlob(table, id, column, (byte[]) v, f);
          } else
          {
            store.update(table, id, column, v, f);
          }
        }
        return true;
      }
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
    }
    return false;
  }

  /**
   * write and flush
   *
   * @param entity
   * @param ps
   * @return
   */
  public boolean updateAndFlush(Object entity, String... ps)
  {
    if (this.update(entity, ps))
    {
      flush();
      write++;
      return true;
    }
    return false;
  }

  /**
   * update all properties of entity
   *
   * @param entity
   * @return
   */
  public boolean updateAndFlush(Object entity)
  {
    if (this.update(entity))
    {
      flush();
      write++;
      return true;
    }
    return false;
  }

  /**
   * read an entity
   *
   * @param <T>
   * @param claz
   * @param id
   * @return
   */
  public <T> T get(Class<T> claz, String id)
  {
    T entity = getFromCache(claz, id);
    if (entity != null)
    {
      return entity;
    }
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      String table = representation.getTable();
      read++;
      if (!store.exists(table, id))
      {
        return null;
      }
      entity = claz.newInstance();
      representation.setId(entity, id);
      store.processFields(entity, representation, id);
      read++;
      if (Rojo.cacheable && representation.isCacheable())
      {
        cache(entity, id);
      }
      return entity;
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
      throw new RepositoryError(e);
    }
  }

  /**
   * exist
   *
   * @param claz
   * @param id
   * @return
   */
  public boolean exist(Class claz, String id)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      String table = representation.getTable();
      read++;
      return store.exists(table, id);
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
    }
    return false;
  }

  /**
   * get a property
   *
   * @param <T>
   * @param claz
   * @param id
   * @param p
   * @return
   */
  public <T> T get(Class<T> claz, String id, String p)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      Field f = representation.getField(p);
      String table = representation.getTable();
      String column = representation.getColumn(p);
      if (Rojo.cacheable && representation.isCacheable())
      {
        Object entity = getFromCache(claz, id);
        if (entity != null)
        {
          return (T) f.get(entity);
        }
      }
      read++;
      if (Collection.class.isAssignableFrom(f.getType()))
      {
        Collection holder = RedisFacade.initCollectionHolder(f);
        store.readValues(table, id, column, f, holder);
        return (T) holder;
      } else if (Map.class.isAssignableFrom(f.getType()))
      {
        Map map = RedisFacade.initMapHolder(f);
        store.readValues(table, id, column, f, map);
        return (T) map;
      } else
      {
        return store.readValue(table, id, column, f);
      }
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
    }
    return null;
  }

  /**
   * delete and flush
   *
   * @param entity
   */
  public void deleteAndFlush(Object entity)
  {
    this.delete(entity);
    write++;
    flush();
  }

  /**
   * delete by id
   *
   * @param claz
   * @param id
   */
  public void deleteAndFlush(Class claz, String id)
  {
    Object temp = this.get(claz, id);
    if (temp != null)
    {
      this.deleteAndFlush(temp);
    }
  }

  /**
   * delete by class
   *
   * @param claz
   */
  public void deleteAndFlush(Class claz)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      String table = representation.getTable();
      store.delKeys(table);
      read++;
      write++;
      if (Rojo.cacheable && representation.isCacheable())
      {
        cache.evict(claz);
      }
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
    }
  }

  /**
   * delete entity
   *
   * @param entity
   */
  public void delete(Object entity)
  {
    if (entity == null)
    {
      return;
    }
    Class claz = entity.getClass();
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      String table = representation.getTable();
      String id = representation.getId(entity);
      if (id == null)
      {
        return;
      }
      Field unique = representation.getUnique();
      if (unique != null)
      {
        store.removeUnique(entity, unique, table, representation.getColumn(unique.getName()));
      }
      Field[] fs = representation.getFields();
      String[] columns = representation.getColumns();
      for (int i = 0; i < fs.length; i++)
      {
        Field field = fs[i];
        store.delete(table, id, columns[i], field);//collection or map
        Index index = field.getAnnotation(Index.class);
        if (index != null)
        {
          try
          {
            Object v = field.get(entity);
            if (v != null)
            {
              store.deleteIndex(table, columns[i], v.toString(), id);
            }
          } catch (Exception e)
          {
            LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
          }
        }
      }
      store.delete(table, id);//all simple properties
      store.deleteId(table, id);//id
      if (Rojo.cacheable && representation.isCacheable())
      {
        evict(claz, id);
      }
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
    }
  }

  /**
   * the range
   *
   * @param <T>
   * @param claz
   * @param p
   * @param start
   * @param end
   * @return
   */
  public <T> Set<T> range(Class<T> claz, String p, long start, long end)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      Field f = representation.getField(p);
      String table = representation.getTable();
      String column = representation.getColumn(f.getName());
      Set<String> s = store.range(table, column, f, start, end);
      read++;
      Set<T> r = new LinkedHashSet<T>(s.size());
      for (String item : s)
      {
        r.add(this.get(claz, item));
      }
      return r;
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", e.getMessage());
    }
    return null;
  }

  /**
   * range by score
   *
   * @param <T>
   * @param claz
   * @param p
   * @param start
   * @param end
   * @return
   */
  public <T> Set<T> scoreRange(Class<T> claz, String p, double start, double end)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      Field f = representation.getField(p);
      String table = representation.getTable();
      String column = representation.getColumn(f.getName());
      Set<String> s = store.scoreRange(table, column, f, start, end);
      read++;
      Set<T> r = new LinkedHashSet<T>(s.size());
      for (String item : s)
      {
        r.add(this.get(claz, item));
      }
      return r;
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", e.getMessage());
    }
    return null;
  }

  /**
   * index of rank
   *
   * @param claz
   * @param id
   * @param p
   * @return
   */
  public long rank(Class claz, String id, String p)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      Field f = representation.getField(p);
      String table = representation.getTable();
      String column = representation.getColumn(p);
      read++;
      return store.rank(table, column, f, id);
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
      return -1;
    }
  }

  /**
   * index size
   *
   * @param claz
   * @param p
   * @param v
   * @return
   */
  public long indexSize(Class claz, String p, Object v)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      String table = representation.getTable();
      String column = representation.getColumn(p);
      long s = store.indexSize(table, column, v);
      read++;
      return s;
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
    }
    return -1;
  }

  /**
   * the indexings
   *
   * @param <T>
   * @param claz
   * @param p
   * @param v
   * @return
   */
  public <T> Set<T> index(Class<T> claz, String p, Object v)
  {
    return this.index(claz, p, v, 0, -1);
  }

  /**
   * indexing with range(index sorted by createTime asc)
   *
   * @param <T>
   * @param claz
   * @param p
   * @param v
   * @param start
   * @param end
   * @return
   */
  public <T> Set<T> index(Class<T> claz, String p, Object v, long start, long end)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      String table = representation.getTable();
      String column = representation.getColumn(p);
      Set<String> s = store.index(table, column, v, start, end);
      read++;
      Set<T> r = new LinkedHashSet<T>(s.size());
      for (String item : s)
      {
        r.add(this.get(claz, item));
      }
      return r;
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
    }
    return null;
  }

  /**
   * unique object
   *
   * @param <T>
   * @param claz
   * @param v
   * @return
   */
  public <T> T unique(Class<T> claz, Object v)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      Field unique = representation.getUnique();
      String table = representation.getTable();
      String column = representation.getColumn(unique.getName());
      String id = store.unique(table, column, v.toString());
      read++;
      return id == null ? null : this.get(claz, id);
    } catch (Exception e)
    {
      store.reset();
      LOG.log(Level.SEVERE, "rojo error :{0}", stackTrace(e));
    }
    return null;
  }

  /**
   * cache
   *
   * @param entity
   */
  private void cache(Object entity, String id)
  {
    if (cache != null)
    {
      cache.cache(entity, id);
    }
  }

  /**
   *
   * @param <T>
   * @param claz
   * @param id
   * @return
   */
  private <T> T getFromCache(Class<T> claz, String id)
  {
    return cache == null ? null : cache.get(claz, id);
  }

  /**
   * evict an object
   *
   * @param claz
   * @param id
   */
  public static void evict(Class claz, String id)
  {
    if (cache != null)
    {
      cache.evict(claz, id);
    }
  }

  /**
   * evict entites of claz
   *
   * @param claz
   */
  public static void clearCache(Class claz)
  {
    if (cache != null)
    {
      if (cache instanceof SoftCache)
      {
        ((SoftCache) cache).clear(claz);
      }
    }
  }

  /**
   * clear cache
   */
  public static void clearCache()
  {
    if (cache != null)
    {
      cache.clear();
    }
  }

  public static void setCacheoutListerner(CacheoutListerner cacheoutListerner)
  {
    if (cache != null)
    {
      cache.setCacheoutListerner(cacheoutListerner);
    }
  }

  public static CacheoutListerner getCacheoutListerner()
  {
    return cache == null ? null : cache.getCacheoutListerner();
  }

  private static boolean isEmpty(String id)
  {
    return id == null || id.isEmpty();
  }

  public static long read()
  {
    return read;
  }

  public static long write()
  {
    return write;
  }

  public static boolean isCacheable()
  {
    return cacheable;
  }

  public static void setCacheable(boolean cacheable)
  {
    Rojo.cacheable = cacheable;
    if (!cacheable)
    {
      Rojo.clearCache();
    } else if (cache == null)
    {
      try
      {
        TIMES_CACHE_CLEAR = Integer.parseInt(System.getProperty("rojo.times.cache.clear", "15000"));
        String impl = System.getProperty("rojo.cache.impl", "org.rojo.util.LruCache");
        int size = Integer.parseInt(System.getProperty("rojo.cache.size", "150000"));
        Class c = Class.forName(impl);
        Constructor con = c.getConstructor(Integer.class);
        cache = (Cache) con.newInstance(size);
        if (cache instanceof SoftCache)
        {
          ((SoftCache) cache).setTIMES_CACHE_CLEAR(TIMES_CACHE_CLEAR);
        }
      } catch (Exception e)
      {
        LOG.log(Level.SEVERE, "some property are missing,default values will be applied");
      }
    }
  }

  /**
   * exception trace
   *
   * @param e
   * @return
   */
  public static String stackTrace(Exception e)
  {
    try
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos, true, "utf-8");
      e.printStackTrace(ps);
      return baos.toString();
    } catch (Exception ex)
    {
      LOG.log(Level.SEVERE, "stackTrace error " + ex.getMessage());
      return null;
    }
  }
}
