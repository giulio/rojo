package org.rojo.repository;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jredis.RedisException;
import org.jredis.ri.alphazero.JRedisClient;
import org.junit.Before;
import org.junit.Test;
import org.rojo.annotations.Entity;
import org.rojo.annotations.Id;
import org.rojo.annotations.Reference;
import org.rojo.annotations.Value;
import org.rojo.repository.converters.IntegerConverter;
import org.rojo.repository.utils.IdUtil;
import org.rojo.test.Util;

public class EntityWriterTest {
    
    private EntityWriter target;
    private JRedisClient jrClient;
    
    @Before
    public void init() {
        jrClient= mock(JRedisClient.class);
        target = new Repository(new RedisFacade(jrClient, Util.initConverters()), new AnnotationValidator());
    }
    
    @Test
    public void writeSimpleValueEntity() throws RedisException {
        SimpleValueEntity entity = new SimpleValueEntity();
        entity.id = 2;
        entity.value = 999;
        target.write(entity);
        verify(jrClient).set("org.rojo.repository.entitywritertest.simplevalueentity:2:value", new IntegerConverter().encode(entity.value));
    }
    
    @Test
    public void writeCollectionOfValuesEntity() throws RedisException {
        
        CollectionOfValuesEntity entity = new CollectionOfValuesEntity();
        entity.id = 2;
        entity.values = Arrays.asList(new Integer[]{1, 2 ,3, 4, 5});
        
        target.write(entity);
        
        verify(jrClient).lpush("org.rojo.repository.entitywritertest.collectionofvaluesentity:2:values", new IntegerConverter().encode(1));
        verify(jrClient).lpush("org.rojo.repository.entitywritertest.collectionofvaluesentity:2:values", new IntegerConverter().encode(2));
        verify(jrClient).lpush("org.rojo.repository.entitywritertest.collectionofvaluesentity:2:values", new IntegerConverter().encode(3));
        verify(jrClient).lpush("org.rojo.repository.entitywritertest.collectionofvaluesentity:2:values", new IntegerConverter().encode(4));
        verify(jrClient).lpush("org.rojo.repository.entitywritertest.collectionofvaluesentity:2:values", new IntegerConverter().encode(5));

    }
    
    
    @Test
    public void writeReference() throws RedisException {
        
        EntityWithReference entity = new EntityWithReference();
        entity.id = 6;
        entity.simpleValueEntity = new SimpleValueEntity();
        entity.simpleValueEntity.id = 2;
        entity.simpleValueEntity.value = 12;
        
        target.write(entity);

        verify(jrClient).set("org.rojo.repository.entitywritertest.simplevalueentity:2:value", new IntegerConverter().encode(entity.simpleValueEntity.value));
        verify(jrClient).set("org.rojo.repository.entitywritertest.entitywithreference:6:simplevalueentity", IdUtil.encodeId(entity.simpleValueEntity.id));
     
    }
    
    @Test 
    public void writeCollectionOfReferences() throws RedisException {
       
        CollectionOfReferences entity = new CollectionOfReferences();
        entity.id = 6;
        entity.values = new ArrayList<SimpleValueEntity>();
        
        SimpleValueEntity refOne = new SimpleValueEntity();
        refOne.id = 20;
        refOne.value = 5;
        entity.values.add(refOne);
        
        SimpleValueEntity refTwo = new SimpleValueEntity();
        refTwo.id = 30;
        refTwo.value = 6;
        entity.values.add(refTwo);
        
        target.write(entity);
        
        verify(jrClient).set("org.rojo.repository.entitywritertest.simplevalueentity:20:value", new IntegerConverter().encode(5));
        verify(jrClient).set("org.rojo.repository.entitywritertest.simplevalueentity:30:value", new IntegerConverter().encode(6));

        verify(jrClient).lpush("org.rojo.repository.entitywritertest.collectionofreferences:6:values", IdUtil.encodeId(20));
        verify(jrClient).lpush("org.rojo.repository.entitywritertest.collectionofreferences:6:values", IdUtil.encodeId(30));
      
    }
    
    @Entity public class SimpleValueEntity {        
        @Id public long id;
        @Value public int value;      
    }
    
    @Entity public class CollectionOfValuesEntity {
        @Id public long id;
        @Value public List<Integer> values;
    }
    
    @Entity public class EntityWithReference {
        @Id public long id;
        @Reference public SimpleValueEntity simpleValueEntity;
    }
    
    @Entity public class CollectionOfReferences {
        @Id public long id;
        @Reference public List<SimpleValueEntity> values;
    }

}
