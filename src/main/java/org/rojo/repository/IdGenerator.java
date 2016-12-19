/**
 * id generator
 */
package org.rojo.repository;

import java.util.HashMap;
import java.util.Map;
import redis.clients.jedis.Jedis;

/**
 *
 * @author beykery
 */
public abstract class IdGenerator
{

  private static final Map<String, IdGenerator> gs = new HashMap<String, IdGenerator>();

  /**
   * configue
   *
   * @param name
   */
  public void configue(String name)
  {
    gs.put(name, this);
  }

  /**
   * get generator
   *
   * @param name
   * @return
   */
  static IdGenerator getGenerator(String name)
  {
    return gs.get(name);
  }

  /**
   * generate id
   *
   * @param claz
   * @param table
   * @param je
   * @return
   */
  public abstract String id(Class claz, String table, Jedis je);

}
