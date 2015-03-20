/**
 * 测试
 */
package org.rojo.test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.rojo.repository.Rojo;
import redis.clients.jedis.Jedis;

/**
 *
 * @author beykery
 */
public class Test
{

  public static void main(String[] args) throws InterruptedException
  {
    int times = 10;
    long start = System.currentTimeMillis();
    long os = start;
    Jedis je = new Jedis("localhost", 6379);
    je.auth("ssm123");
    je.ping();
    Rojo re = new Rojo(je);
    je.flushAll();
    // new TestGenerator(je, "common:id").configue("common:id");//configue id generator
    for (int i = 1; i <= times; i++)
    {
      TestEntity ss = new TestEntity();
      ss.setName("test" + i);
      ss.setAge(i);
      List<String> l = new ArrayList<String>();
      l.add("fd");
      l.add("akjl;sfd");
      ss.setList(l);
      Map<String, Integer> map = new HashMap<String, Integer>();
      ss.setMap(map);
      map.put("a", 1);
      map.put("b", 2);
      Set<Float> set = new HashSet<Float>();
      ss.setSet(set);
      set.add(1.2f);
      set.add(3.14159f);
      ss.setSex(i % 2 == 1 ? 1 : 0);
      re.saveAndFlush(ss);
      ss.setT1(41);
      ss.setT2(20);
      re.updateAndFlush(ss, "t1", "t2");
    }

    System.out.println(System.currentTimeMillis() - start);
    re.clearCache();
    start = System.currentTimeMillis();
    for (int i = 1; i <= times; i++)
    {
      TestEntity te = re.get(TestEntity.class, i + "");
      System.out.println(te);
    }
    System.out.println(System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    for (int i = 1; i <= times; i++)
    {
      TestEntity te = re.get(TestEntity.class, (i) + "");
      System.out.println(te);
    }
    System.out.println(System.currentTimeMillis() - start);

    re.clearCache();
    Set<TestEntity> l = re.index(TestEntity.class, "sex", 0);
    System.out.println(l);

    TestEntity te = re.unique(TestEntity.class, "test6");
    System.out.println("unique:" + te);

    Set<TestEntity> r = re.range(TestEntity.class, "age", -5, -1);
    System.out.println(r);

    long rank = re.rank(TestEntity.class, "10", "age");
    System.out.println(rank);
    Set<TestEntity> tes = re.all(TestEntity.class, 0, -1);
    System.out.println(tes);
    re.deleteAndFlush(te);
    tes = re.all(TestEntity.class, 0, -1);
    System.out.println(tes);
    Date ctime = re.createTime(TestEntity.class, "8");
    System.out.println(ctime);
    tes = re.all(TestEntity.class, new Date(os), ctime);
    System.out.println(tes);
  }

}
