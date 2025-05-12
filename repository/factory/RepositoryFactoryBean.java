package com.db.kurs.orm.repository.factory;

import com.db.kurs.orm.mapper.EntityMapper;
import com.db.kurs.orm.repository.CrudRepository;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.reflect.Proxy;

public class RepositoryFactoryBean<T> implements FactoryBean<T> {

    private final Class<T> repositoryInterface;
    private final JdbcTemplate jdbcTemplate;
    private final EntityMapper entityMapper;

    public RepositoryFactoryBean(Class<T> repositoryInterface,
                                 JdbcTemplate jdbcTemplate,
                                 EntityMapper entityMapper) {
        if (repositoryInterface == null) {
            throw new BeanCreationException("Repository interface must not be null");
        }
        if (repositoryInterface.equals(CrudRepository.class)) {
            throw new BeanCreationException("CrudRepository must be extended");
        }
        this.repositoryInterface = repositoryInterface;
        this.jdbcTemplate = jdbcTemplate;
        this.entityMapper = entityMapper;
    }

    @Override
    public T getObject() {
        // Передаём конкретный интерфейс репозитория в RepositoryInvocationHandler
        RepositoryInvocationHandler handler = new RepositoryInvocationHandler(jdbcTemplate, entityMapper, repositoryInterface);
        return (T) Proxy.newProxyInstance(
                repositoryInterface.getClassLoader(),
                new Class[]{repositoryInterface},
                handler
        );
    }

    @Override
    public Class<?> getObjectType() {
        return repositoryInterface;
    }
}
