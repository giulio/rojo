/**
 * 测试
 */
package org.rojo.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.rojo.repository.Repository;
import redis.clients.jedis.Jedis;

/**
 *
 * @author beykery
 */
public class Test {

    public static void main(String[] args) throws InterruptedException {
        Jedis je = new Jedis("localhost", 6379);
        je.auth("4swardsman");
        je.ping();
        je.flushAll();
        Repository re = new Repository(je);
        int times = 100000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
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
            re.writeAndFlush(ss);
        }
        System.out.println(System.currentTimeMillis() - start);
        
        Repository.getCache().clear();
        start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            TestEntity te = re.get(TestEntity.class, i + 1);
            //System.out.println(te);
        }
        System.out.println(System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            TestEntity te = re.get(TestEntity.class, i + 1);
            //System.out.println(te);
        }
        System.out.println(System.currentTimeMillis() - start);
    }

}
