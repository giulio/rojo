/**
 * cache
 */
package org.rojo.repository;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * @author beykery
 */
public class SoftCache
{

  private volatile long cachedObjectCounter;
  private volatile long cacheoutCounter;
  private CacheoutListerner cacheoutListerner;
  private volatile long hit;
  private volatile long miss;
  private int TIMES_CACHE_CLEAR = 15000;
  private volatile int timeCache;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock readLock = lock.readLock();
  private final Lock writeLock = lock.writeLock();
  private final Map<Class, Map<String, SoftObjectReference<Object>>> cache = new HashMap<Class, Map<String, SoftObjectReference<Object>>>();
  private final ReferenceQueue<SoftObjectReference<Object>> rq = new ReferenceQueue<SoftObjectReference<Object>>();

  public SoftCache()
  {
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

  /**
   * cache
   *
   * @param entity
   * @param id
   */
  public void cache(Object entity, String id)
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
  public <T> T get(Class<T> claz, String id)
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
          return (T) r;
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
   * @return
   */
  public boolean evict(Class claz, String id)
  {
    writeLock.lock();
    try
    {
      Map<String, SoftObjectReference<Object>> c = cache.get(claz);
      if (c != null)
      {
        return c.remove(id) != null;
      }
      return false;
    } finally
    {
      writeLock.unlock();
    }
  }

  /**
   * evict entites of claz
   *
   * @param claz
   */
  public void clear(Class claz)
  {
    writeLock.lock();
    try
    {
      Map<String, SoftObjectReference<Object>> c = cache.get(claz);
      if (c != null)
      {
        cachedObjectCounter -= c.size();
        c.clear();
      }
    } finally
    {
      writeLock.unlock();
    }
  }

  /**
   * clear cache
   */
  @SuppressWarnings("empty-statement")
  public void clear()
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
   * cacheout
   *
   * @param claz
   * @param id
   */
  private void onCacheout(Class claz, String id)
  {
    cacheoutCounter++;
    if (cacheoutListerner != null)
    {
      cacheoutListerner.onCacheout(claz, id);
    }
  }

  public long cached()
  {
    return cachedObjectCounter;
  }

  public long cacheout()
  {
    return cacheoutCounter;
  }

  public long hit()
  {
    return hit;
  }

  public long miss()
  {
    return miss;
  }

  public CacheoutListerner getCacheoutListerner()
  {
    return cacheoutListerner;
  }

  public void setCacheoutListerner(CacheoutListerner cacheoutListerner)
  {
    this.cacheoutListerner = cacheoutListerner;
  }

  public int getTIMES_CACHE_CLEAR()
  {
    return TIMES_CACHE_CLEAR;
  }

  public void setTIMES_CACHE_CLEAR(int TIMES_CACHE_CLEAR)
  {
    this.TIMES_CACHE_CLEAR = TIMES_CACHE_CLEAR;
  }

}
