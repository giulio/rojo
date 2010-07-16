package org.rojo.repository;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.jredis.RedisException;
import org.jredis.ri.alphazero.JRedisClient;
import org.junit.Before;
import org.junit.Test;
import org.rojo.annotations.Entity;
import org.rojo.annotations.Id;
import org.rojo.annotations.Value;
import org.rojo.repository.converters.IntegerConverter;
import org.rojo.test.TestUtils;

public class EntityWriterTest {
    
    private EntityWriter target;
    private JRedisClient jrClient;

    
    @Before
    public void init() {
        jrClient= mock(JRedisClient.class);
        target = new Repository(new RedisFacade(jrClient, TestUtils.initConverters()), new AnnotationValidator());
    }
    
    @Test
    public void WriteSimpleValueEntity() throws RedisException {
        SimpleValueEntity entity = new SimpleValueEntity();
        entity.id = 2;
        entity.value = 999;
        target.write(entity);
        verify(jrClient).set("org.rojo.repository.entitywritertest.simplevalueentity:2:value", new IntegerConverter().encode(entity.value));
    }
    
    
    @Entity
    public class SimpleValueEntity {        
        @Id
        public long id;
        
        @Value
        public int value;      
    }
    
    

}
