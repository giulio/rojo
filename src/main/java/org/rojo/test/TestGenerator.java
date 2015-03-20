/**
 * test id generator
 */
package org.rojo.test;

import org.rojo.repository.IdGenerator;
import redis.clients.jedis.Jedis;

/**
 *
 * @author beykery
 */
public class TestGenerator extends IdGenerator
{

  private final Jedis je;
  private final String name;

  public TestGenerator(Jedis je, String name)
  {
    this.je = je;
    this.name = name;
  }

  @Override
  public String id(Class claz, String table, Jedis je)
  {
    return je.incr(name) + "fkey";
  }

}
