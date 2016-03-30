/**
 * cache out listerner
 */
package org.rojo.util;

/**
 *
 * @author beykery
 */
public abstract class CacheoutListerner
{

  /**
   *
   * @param claz
   * @param id
   */
  public abstract void onCacheout(Class claz, String id);

}
