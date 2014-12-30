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

    public static void main(String[] args) {
        Jedis je = new Jedis("localhost", 6379);
        je.auth("4swardsman");
        je.ping();
        je.flushAll();
        Repository re = new Repository(je);
        int times = 10000;
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
            ss = re.get(TestEntity.class, ss.getId());
            ss.getId();
        }
        System.out.println(System.currentTimeMillis() - start);
    }

}
