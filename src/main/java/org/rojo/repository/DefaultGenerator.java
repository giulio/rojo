/**
 * default generator
 */
package org.rojo.repository;

import redis.clients.jedis.Jedis;

/**
 *
 * @author beykery
 */
public class DefaultGenerator extends IdGenerator
{

  public DefaultGenerator()
  {
  }

  @Override
  public String id(Class claz, String table, Jedis je)
  {
    return je.incr(table + ":::id").toString();
  }

}
