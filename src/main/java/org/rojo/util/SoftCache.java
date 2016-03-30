/**
 * cache
 */
package org.rojo.util;

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
public class SoftCache implements Cache
{

  private CacheoutListerner cacheoutListerner;
  private int TIMES_CACHE_CLEAR = 15000;
  private volatile int timeCache;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock readLock = lock.readLock();
  private final Lock writeLock = lock.writeLock();
  private final Map<Class, Map<String, SoftObjectReference<Object>>> cache = new HashMap<Class, Map<String, SoftObjectReference<Object>>>();
  private final ReferenceQueue<SoftObjectReference<Object>> rq = new ReferenceQueue<SoftObjectReference<Object>>();
  private final Stats stats = new Stats();

  public SoftCache(int size)
  {
  }

  @Override
  public Stats stats()
  {
    return stats;
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
  @Override
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
      Object old = c.put(id, new SoftObjectReference<Object>(entity, rq, id));
      int currentSize;
      if (old == null)
      {
        currentSize = stats.size.incrementAndGet();
      } else
      {
        currentSize = stats.size.get();
      }
      stats.putCounter.incrementAndGet();
      timeCache++;
      if (timeCache >= TIMES_CACHE_CLEAR)
      {
        SoftObjectReference sr;
        while ((sr = (SoftObjectReference) rq.poll()) != null)
        {
          c = cache.get(sr.claz);
          Object en = c.remove(sr.id);
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
  @Override
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
            stats.accessCounter.incrementAndGet();
          }
          return (T) r;
        }
      }
      stats.missCounter.incrementAndGet();
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
  @Override
  public void evict(Class claz, String id)
  {
    writeLock.lock();
    try
    {
      Map<String, SoftObjectReference<Object>> c = cache.get(claz);
      if (c != null)
      {
        c.remove(id);
      }
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
  @Override
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
    stats.size.decrementAndGet();
    if (cacheoutListerner != null)
    {
      cacheoutListerner.onCacheout(claz, id);
    }
  }

  @Override
  public CacheoutListerner getCacheoutListerner()
  {
    return cacheoutListerner;
  }

  @Override
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
