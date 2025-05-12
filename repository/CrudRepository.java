package com.db.kurs.orm.repository;

import java.util.List;

public interface CrudRepository<T, ID> {
    T findById(ID id);
    List<T> findAll();
    void create(T entity);
    void update(T entity);
    void delete(ID id);
    Long countAll();
}