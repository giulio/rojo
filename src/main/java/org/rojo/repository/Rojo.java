package org.rojo.repository;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.rojo.annotations.Index;
import org.rojo.exceptions.RepositoryError;
import redis.clients.jedis.Jedis;

public class Rojo
{

  private static final Logger LOG = Logger.getLogger(Rojo.class.getName());
  private final RedisFacade store;
  private static final Map<Class, Map<String, SoftObjectReference<Object>>> cache = new HashMap<Class, Map<String, SoftObjectReference<Object>>>();
  private static final ReferenceQueue<SoftObjectReference<Object>> rq = new ReferenceQueue<SoftObjectReference<Object>>();
  private static int TIMES_CACHE_CLEAR;
  private static volatile int timeCache;
  private static final ReadWriteLock lock = new ReentrantReadWriteLock();
  private static final Lock readLock = lock.readLock();
  private static final Lock writeLock = lock.writeLock();
  private static volatile int cachedObjectCounter;
  private CacheoutListerner cacheoutListerner;
  private static volatile long hit;
  private static volatile int miss;

  static
  {
    /**
     * defaultGenerator init*
     */
    new DefaultGenerator().configue("defaultGenerator");
    try
    {
      TIMES_CACHE_CLEAR = Integer.parseInt(System.getProperty("rojo.times.cache.clear", "15000"));
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "property rojo.times.cache.clear is missing,15000 will be applied");
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
      Set<T> r = new LinkedHashSet<T>(s.size());
      for (String item : s)
      {
        r.add(this.get(c, item));
      }
      return r;
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
      return null;
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
      Set<T> r = new LinkedHashSet<T>(s.size());
      for (String item : s)
      {
        r.add(this.get(c, item));
      }
      return r;
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
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
      return store.createTime(table, id);
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
      return null;
    }
  }

  public void flush()
  {
    store.flush();
  }

  public String saveAndFlush(Object entity)
  {
    String id;
    if ((id = save(entity)) != null)
    {
      store.flush();
    }
    return id;
  }

  /**
   * write an entity
   *
   * @param entity
   * @return
   */
  public String save(Object entity)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(entity.getClass());
      String id = representation.getId(entity);
      String table = representation.getTable();
      id = isEmpty(id) ? representation.getIdGenerator().id(entity.getClass(), table, store.getJedis()) : id;
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
            store.write(table, id, representation.getColumns()[i], field.get(entity), field, true);
          }
        }
      }
      representation.setId(entity, id);
      store.addId(table, id);
      if (representation.isCache())
      {
        cache(entity, id);
      }
      return id;
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
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
          if (Collection.class.isAssignableFrom(f.getType()) && Collection.class.isAssignableFrom(v.getClass()))
          {
            store.writeCollection(table, (Collection) v, id, column);
          } else if (Map.class.isAssignableFrom(f.getType()) && Map.class.isAssignableFrom(v.getClass()))
          {
            store.writeMap(table, (Map) v, id, column);
          } else
          {
            store.update(table, id, column, v, f);
          }
        }
        if (representation.isCache())
        {
          Object c = getFromCache(claz, id);
          if (entity != c && c != null)//cache invalid
          {
            this.evict(claz, id);//evict invalid entity
          }
        }
        return true;
      }
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
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
    T entity = (T) getFromCache(claz, id);
    if (entity != null)
    {
      return entity;
    }
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      String table = representation.getTable();
      if (!store.exists(table, id))
      {
        return null;
      }
      entity = claz.newInstance();
      representation.setId(entity, id);
      store.processFields(entity, representation, id);
      if (representation.isCache())
      {
        cache(entity, id);
      }
      return entity;
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
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
      return store.exists(table, id);
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
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
      if (representation.isCache())
      {
        Object entity = getFromCache(claz, id);
        if (entity != null)
        {
          return (T) f.get(entity);
        }
      }
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
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
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
    flush();
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
      if (representation.isCache())
      {
        evict(claz, id);
      }
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
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
      Set<T> r = new LinkedHashSet<T>(s.size());
      for (String item : s)
      {
        r.add(this.get(claz, item));
      }
      return r;
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
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
      return store.rank(table, column, f, id);
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
      return -1;
    }
  }

  /**
   * the indexing ids
   *
   * @param <T>
   * @param claz
   * @param p
   * @param v
   * @return
   */
  public <T> Set<T> index(Class<T> claz, String p, Object v)
  {
    try
    {
      EntityRepresentation representation = EntityRepresentation.forClass(claz);
      String table = representation.getTable();
      String column = representation.getColumn(p);
      Set<String> s = store.index(table, column, v);
      Set<T> r = new LinkedHashSet<T>(s.size());
      for (String item : s)
      {
        r.add(this.get(claz, item));
      }
      return r;
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
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
      return id == null ? null : this.get(claz, id);
    } catch (Exception e)
    {
      LOG.log(Level.WARNING, "rojo error :{0}", e.getMessage());
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
    writeLock.lock();
    try
    {
      Map<String, SoftObjectReference<Object>> c = cache.get(entity.getClass());
      if (c == null)
      {
        c = new HashMap<String, SoftObjectReference<Object>>();
        cache.put(entity.getClass(), c);
      }
      c.put(id, new SoftObjectReference<Object>(entity, rq, id));
      cachedObjectCounter++;
      timeCache++;
      if (timeCache >= TIMES_CACHE_CLEAR)
      {
        SoftObjectReference sr;
        while ((sr = (SoftObjectReference) rq.poll()) != null)
        {
          c = cache.get(sr.claz);
          c.remove(sr.id);
          cachedObjectCounter--;
          onCacheout(sr.claz, sr.id);
        }
        timeCache = 0;
      }
    } finally
    {
      writeLock.unlock();
    }
  }

  /**
   *
   * @param <T>
   * @param claz
   * @param id
   * @return
   */
  private Object getFromCache(Class claz, String id)
  {
    readLock.lock();
    try
    {
      Map<String, SoftObjectReference<Object>> c = cache.get(claz);
      if (c != null)
      {
        SoftReference<Object> sr = c.get(id);
        if (sr != null)
        {
          Object r = sr.get();
          if (r != null)
          {
            hit++;
          }
          return r;
        }
      }
      miss++;
      return null;
    } finally
    {
      readLock.unlock();
    }
  }

  /**
   * evict an object
   *
   * @param claz
   * @param id
   */
  public void evict(Class claz, String id)
  {
    readLock.lock();
    try
    {
      Map<String, SoftObjectReference<Object>> c = cache.get(claz);
      if (c != null)
      {
        c.remove(id);
      }
    } finally
    {
      readLock.unlock();
    }
  }

  /**
   * evict entites of claz
   *
   * @param claz
   */
  public void clearCache(Class claz)
  {
    readLock.lock();
    try
    {
      Map<String, SoftObjectReference<Object>> c = cache.get(claz);
      if (c != null)
      {
        c.clear();
      }
    } finally
    {
      readLock.unlock();
    }
  }

  /**
   * clear cache
   */
  @SuppressWarnings("empty-statement")
  public void clearCache()
  {
    writeLock.lock();
    try
    {
      for (Map<String, SoftObjectReference<Object>> m : cache.values())
      {
        m.clear();
      }
      SoftObjectReference sr;
      while ((sr = (SoftObjectReference) rq.poll()) != null)
      {
        onCacheout(sr.claz, sr.id);
      }
      cachedObjectCounter = 0;
    } finally
    {
      writeLock.unlock();
    }
  }

  /**
   * cache size
   *
   * @return
   */
  public static int getCachedObjectCounter()
  {
    return cachedObjectCounter;
  }

  public void setCacheoutListerner(CacheoutListerner cacheoutListerner)
  {
    this.cacheoutListerner = cacheoutListerner;
  }

  public CacheoutListerner getCacheoutListerner()
  {
    return cacheoutListerner;
  }

  /**
   * cacheout
   *
   * @param claz
   * @param id
   */
  private void onCacheout(Class claz, String id)
  {
    if (this.cacheoutListerner != null)
    {
      this.cacheoutListerner.onCacheout(claz, id);
    }
  }

  private boolean isEmpty(String id)
  {
    return id == null || id.isEmpty();
  }

  private class SoftObjectReference<Object> extends SoftReference<Object>
  {

    public final String id;
    public final Class claz;

    public SoftObjectReference(Object r, ReferenceQueue q, String id)
    {
      super(r, q);
      this.id = id;
      this.claz = r.getClass();
    }
  }

  public static long hit()
  {
    return hit;
  }

  public static long miss()
  {
    return miss;
  }
}
