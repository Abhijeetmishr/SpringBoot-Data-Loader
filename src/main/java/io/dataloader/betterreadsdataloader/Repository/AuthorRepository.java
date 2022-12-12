package io.dataloader.betterreadsdataloader.Repository;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

import io.dataloader.betterreadsdataloader.entity.Author;


@Repository
public interface AuthorRepository extends CassandraRepository<Author, String>{
    
}
