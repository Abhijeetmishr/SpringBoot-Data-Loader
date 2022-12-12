package io.dataloader.betterreadsdataloader.Repository;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

import io.dataloader.betterreadsdataloader.entity.Book;
@Repository
public interface BookRepository extends CassandraRepository<Book, String>{
    
}
