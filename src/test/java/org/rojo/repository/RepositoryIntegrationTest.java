package org.rojo.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jredis.RedisException;
import org.jredis.ri.alphazero.JRedisClient;
import org.jredis.ri.alphazero.support.DefaultCodec;
import org.junit.Before;
import org.junit.Test;
import org.rojo.domain.Person;
import org.rojo.domain.SomeStrings;
import org.rojo.exceptions.InvalidTypeException;
import org.rojo.repository.converters.Converters;
import org.rojo.repository.converters.IntegerConverter;
import org.rojo.repository.converters.StringConverter;
import org.rojo.test.TestUtils;

public class RepositoryIntegrationTest {

    private Repository target;
    private JRedisClient jrClient;
    
    @Before
    public void init() throws RedisException  {
        jrClient= mock(JRedisClient.class);
        target = new Repository(new RedisFacade(jrClient, TestUtils.initConverters()), new AnnotationValidator());
    }
    
  

    @Test(expected = InvalidTypeException.class)
    public void invalidEntityRequest() {
        target.get(new FooTestClass(),  2);
    }
    
    @Test
    public void validEntityRequest() throws RedisException {
        
        when(jrClient.get("org.rojo.domain.person:2:name")).thenReturn(new String("mikael foobar").getBytes());
        when(jrClient.get("org.rojo.domain.person:2:age")).thenReturn(DefaultCodec.<Integer>encode(33));
        when(jrClient.get("org.rojo.domain.person:2:address")).thenReturn(new Long(6).toString().getBytes());
        
        
        when(jrClient.get("org.rojo.domain.address:6:town")).thenReturn(new String("Stockholm").getBytes());
        when(jrClient.get("org.rojo.domain.address:6:street")).thenReturn(new String("Lundagatan").getBytes());
        
        
        Person person = target.get(new Person(), 2);

        assertEquals("mikael foobar", person.getName());
        assertEquals(33, person.getAge());
        assertEquals(2, person.getId());
        
        assertEquals("Stockholm", person.getAddress().getTown());
        assertEquals("Lundagatan", person.getAddress().getStreet());
        assertEquals(6, person.getAddress().getId());
     
        
    }
    
    @Test 
    public void valuesInCollections() throws RedisException {
        
        List<byte[]> listOfString = new ArrayList<byte[]>(3);
        listOfString.add(new String("one").getBytes());
        listOfString.add(new String("dos").getBytes());
        listOfString.add(new String("tre").getBytes());
        
        when(jrClient.lrange("org.rojo.domain.somestrings:3:strings", 0, -1)).thenReturn(listOfString);
        
        
        SomeStrings someStrings = target.get(new SomeStrings(), 3);
        
        assertTrue(someStrings.getStrings().contains("one"));
        assertTrue(someStrings.getStrings().contains("dos"));
        assertTrue(someStrings.getStrings().contains("tre"));
        
    }
    
    @Test
    public void referencesInCollections() throws RedisException  {
        
        when(jrClient.get("org.rojo.domain.person:1:name")).thenReturn(new String("jennifer boyle").getBytes());
        when(jrClient.get("org.rojo.domain.person:1:age")).thenReturn(DefaultCodec.<Integer>encode(29));
        when(jrClient.get("org.rojo.domain.person:1:address")).thenReturn(new Long(6).toString().getBytes());
      
        when(jrClient.lrange("org.rojo.domain.person:1:friends",0,-1)).thenReturn(Collections.singletonList(new Long(2).toString().getBytes()));
        
        
        when(jrClient.get("org.rojo.domain.person:2:name")).thenReturn(new String("mikael foobar").getBytes());
        when(jrClient.get("org.rojo.domain.person:2:age")).thenReturn(DefaultCodec.<Integer>encode(33));
        when(jrClient.get("org.rojo.domain.person:2:address")).thenReturn(new Long(6).toString().getBytes());
        
        
        when(jrClient.get("org.rojo.domain.address:6:town")).thenReturn(new String("Stockholm").getBytes());
        when(jrClient.get("org.rojo.domain.address:6:street")).thenReturn(new String("Lundagatan").getBytes());
        
        
        Person mate = target.get(new Person(), 1);
        
        assertEquals(1, mate.getFriends().size());
        
        assertEquals("mikael foobar", mate.getFriends().get(0).getName());
        
    }
    
    public class FooTestClass {
    }
    
}
