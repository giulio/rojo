package org.rojo.repository;

import static org.junit.Assert.assertEquals;

import org.jredis.RedisException;
import org.jredis.ri.alphazero.JRedisClient;
import org.junit.Before;
import org.junit.Test;
import org.rojo.domain.Address;
import org.rojo.domain.Person;
import org.rojo.test.Util;

public class ReadWriteTestInt {
    
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    
    private Repository repository;
    
    @Before
    public void init() throws RedisException {
        JRedisClient jrClient = new JRedisClient(HOST, PORT);
        jrClient.ping();
        repository = new Repository(new RedisFacade(jrClient, Util.initConverters()), new AnnotationValidator());
    }
    
    @Test
    public void testWritesAndReadAreConsistent() {
        
        Person person = new Person();
        person.setName("Jeff KassKat");
        person.setAge(22);
        
        Address address = new Address();
        address.setStreet("somewhere");
        address.setTown("Stockholm");
        person.setAddress(address);
        
        //-- write
        repository.write(person);
        
        //-- read
        Person samePerson = repository.get(new Person(), person.getId());
        
        //-- verify
        assertEquals(person.getAge(), samePerson.getAge());
        assertEquals(person.getName(), samePerson.getName());

        assertEquals(address.getStreet(), samePerson.getAddress().getStreet());
        assertEquals(address.getTown(), samePerson.getAddress().getTown());
        
    }
    
    

}
