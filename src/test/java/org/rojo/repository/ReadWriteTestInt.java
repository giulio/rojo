package org.rojo.repository;

import org.junit.Before;
import org.junit.Test;
import org.rojo.domain.Address;
import org.rojo.domain.Person;
import org.rojo.exceptions.MissingEntity;
import org.rojo.test.Util;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.assertEquals;

public class ReadWriteTestInt {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;

    private Repository repository;

    @Before
    public void init()  {
        Jedis jrClient = new Jedis(HOST, PORT);
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

    @Test(expected = MissingEntity.class)
    public void testWriteAndDelete() {

        Person person = new Person();
        person.setName("Martin Lol");
        person.setAge(44);

        Address address = new Address();
        address.setStreet("somewhere");
        address.setTown("Stockholm");
        person.setAddress(address);


        //-- write
        repository.write(person);

        //-- delete
        repository.delete(person);

        //-- read should throw exception
        repository.get(new Person(), person.getId());

    }


}
