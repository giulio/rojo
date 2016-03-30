/**
 * cache
 */
package org.rojo.util;

/**
 *
 * @author beykery
 */
public interface Cache
{

  /**
   * cache
   *
   * @param entity
   * @param id
   */
  public void cache(Object entity, String id);

  /**
   * get entity
   *
   * @param <T>
   * @param claz
   * @param id
   * @return
   */
  public <T> T get(Class<T> claz, String id);

  /**
   * evict an object
   *
   * @param claz
   * @param id
   */
  public void evict(Class claz, String id);

  /**
   * clean the cache
   *
   */
  public void clear();

  /**
   * listerner
   *
   * @param cacheoutListerner
   */
  public void setCacheoutListerner(CacheoutListerner cacheoutListerner);

  /**
   * the listerner
   *
   * @return
   */
  public CacheoutListerner getCacheoutListerner();

  /**
   * the stats
   *
   * @return
   */
  public Stats stats();
}
