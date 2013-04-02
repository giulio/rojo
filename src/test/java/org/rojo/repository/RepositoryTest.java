package org.rojo.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.rojo.domain.Person;
import org.rojo.domain.SomeStrings;
import org.rojo.exceptions.InvalidTypeException;
import org.rojo.repository.converters.IntegerConverter;
import org.rojo.repository.utils.IdUtil;
import org.rojo.test.Util;
import redis.clients.jedis.Jedis;

public class RepositoryTest {

    private Repository target;
    private Jedis jrClient;
    
    @Before
    public void init() {
        jrClient= mock(Jedis.class);
        target = new Repository(new RedisFacade(jrClient, Util.initConverters()), new AnnotationValidator());
    }

    @Test(expected = InvalidTypeException.class)
    public void invalidEntityRequest() {
        target.get(new FooTestClass(),  2);
    }
    
    @Test
    public void validEntityRequest() {
        
        when(jrClient.exists("org.rojo.domain.person:2")).thenReturn(true);

        when(jrClient.exists("org.rojo.domain.person:2:name")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.person:2:name")).thenReturn(new String("mikael foobar"));

        when(jrClient.exists("org.rojo.domain.person:2:age")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.person:2:age")).thenReturn(new IntegerConverter().encode(33));
        
        when(jrClient.exists("org.rojo.domain.person:2:address")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.person:2:address")).thenReturn(IdUtil.encodeId(6));
        
        
        when(jrClient.exists("org.rojo.domain.address:6")).thenReturn(true);

        when(jrClient.exists("org.rojo.domain.address:6:town")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.address:6:town")).thenReturn(new String("Stockholm"));
        
        when(jrClient.exists("org.rojo.domain.address:6:street")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.address:6:street")).thenReturn(new String("Lundagatan"));
        
        
        Person person = target.get(new Person(), 2);

        assertEquals("mikael foobar", person.getName());
        assertEquals(33, person.getAge());
        assertEquals(2, person.getId());
        
        assertEquals("Stockholm", person.getAddress().getTown());
        assertEquals("Lundagatan", person.getAddress().getStreet());
        assertEquals(6, person.getAddress().getId());
     
        
    }
    
    @Test 
    public void valuesInCollections() {
        
        List<String> listOfString = new ArrayList<String>(3);
        listOfString.add(new String("one"));
        listOfString.add(new String("dos"));
        listOfString.add(new String("tre"));

        when(jrClient.exists("org.rojo.domain.somestrings:3")).thenReturn(true);
        
        when(jrClient.exists("org.rojo.domain.somestrings:3:strings")).thenReturn(true);
        when(jrClient.lrange("org.rojo.domain.somestrings:3:strings", 0, -1)).thenReturn(listOfString);
        
        
        SomeStrings someStrings = target.get(new SomeStrings(), 3);
        
        assertTrue(someStrings.getStrings().contains("one"));
        assertTrue(someStrings.getStrings().contains("dos"));
        assertTrue(someStrings.getStrings().contains("tre"));
        
    }
    
    @Test
    public void referencesInCollections() {

        when(jrClient.exists("org.rojo.domain.person:1")).thenReturn(true);
        
        when(jrClient.exists("org.rojo.domain.person:1:name")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.person:1:name")).thenReturn(new String("jennifer boyle"));

        when(jrClient.exists("org.rojo.domain.person:1:age")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.person:1:age")).thenReturn(new IntegerConverter().encode(29));

        when(jrClient.exists("org.rojo.domain.person:1:address")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.person:1:address")).thenReturn(IdUtil.encodeId(6));
      
        when(jrClient.exists("org.rojo.domain.person:1:friends")).thenReturn(true);
        when(jrClient.lrange("org.rojo.domain.person:1:friends",0,-1)).thenReturn(Collections.singletonList(IdUtil.encodeId(2)));
        

        when(jrClient.exists("org.rojo.domain.person:2")).thenReturn(true);
        
        when(jrClient.exists("org.rojo.domain.person:2:name")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.person:2:name")).thenReturn(new String("mikael foobar"));
        
        when(jrClient.exists("org.rojo.domain.person:2:age")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.person:2:age")).thenReturn(new IntegerConverter().encode(33));
        
        when(jrClient.exists("org.rojo.domain.person:2:address")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.person:2:address")).thenReturn(IdUtil.encodeId(6));

        when(jrClient.exists("org.rojo.domain.address:6")).thenReturn(true);
        
        when(jrClient.exists("org.rojo.domain.address:6:town")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.address:6:town")).thenReturn(new String("Stockholm"));

        when(jrClient.exists("org.rojo.domain.address:6:street")).thenReturn(true);
        when(jrClient.get("org.rojo.domain.address:6:street")).thenReturn(new String("Lundagatan"));
        
        Person mate = target.get(new Person(), 1);
        
        assertEquals(1, mate.getFriends().size());
        
        assertEquals("mikael foobar", mate.getFriends().get(0).getName());
        
    }
    
    public class FooTestClass {
    }
    
}
