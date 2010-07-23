# ROJO - Redis pOJO - Java ORM for Redis key/value store  

## About

This project is in, and most probably will never leave, an embryonal stage. 

The goal is to build a Java annotatin driven ORM for Redis (http://github.com/antirez/redis) key-value store. 


## Getting started

### Getting around the code

Unit tests / integration tests can be a good starting point to get a feeling of how things work.

The basic idea is that you annotate your POJO as an *@Entity* and annotate the fields that should be persisted as either *@Value* or *@Reference*; the only constrain is that entities must have an id field (long) ant this filed must be annoteted as '@Id'.

Read/write/delete operation are provided by EntityReader and EntityWriter. Ant that's pretty much it.

### Install 

    mvn install

### Testing 

Integration test can be run using the *integration-tests* maven profile: 

    mvn -Pintegration-tests test



