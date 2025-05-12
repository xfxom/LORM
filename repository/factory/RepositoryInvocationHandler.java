package com.db.kurs.orm.repository.factory;

import com.db.kurs.exception.RepositoryException;
import com.db.kurs.orm.annotation.Param;
import com.db.kurs.orm.annotation.Query;
import com.db.kurs.orm.mapper.EntityMapper;
import com.db.kurs.orm.mapper.QueryExecutor;
import com.db.kurs.orm.repository.CrudRepository;
import com.db.kurs.orm.repository.factory.executor.InsertExecutor;
import com.db.kurs.orm.repository.factory.executor.PreparedQueryExecutor;
import com.db.kurs.orm.repository.factory.executor.RelationQueryExecutor;
import com.db.kurs.orm.repository.factory.executor.UpdateExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
public class RepositoryInvocationHandler implements InvocationHandler {

    private final JdbcTemplate jdbcTemplate;
    private final EntityMapper entityMapper;
    private final Class<?> repositoryInterface;
    private final Map<Method, QueryExecutor> executors = new ConcurrentHashMap<>();
    private final Map<Class<?>, CrudMetadata> metadataCache = new HashMap<>();

    public RepositoryInvocationHandler(JdbcTemplate jdbcTemplate,
                                       EntityMapper entityMapper,
                                       Class<?> repositoryInterface) {
        this.jdbcTemplate = jdbcTemplate;
        this.entityMapper = entityMapper;
        this.repositoryInterface = repositoryInterface;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
        return executors.computeIfAbsent(method, this::createExecutor).execute(args);
    }

    private QueryExecutor createExecutor(Method method) {
        Query q = method.getAnnotation(Query.class);
        if (q != null) {
            // 1) определяем тип возвращаемого элемента и isList
            Class<?> elementType = method.getReturnType();
            boolean isList = false;
            if (List.class.isAssignableFrom(elementType)) {
                ParameterizedType pt = (ParameterizedType) method.getGenericReturnType();
                elementType = (Class<?>) pt.getActualTypeArguments()[0];
                isList = true;
            }

            // 2) строим paramNames по @Param или имени аргумента
            Parameter[] params = method.getParameters();
            String[] paramNames = new String[params.length];
            for (int i = 0; i < params.length; i++) {
                Param p = params[i].getAnnotation(Param.class);
                paramNames[i] = (p != null && !p.value().isEmpty())
                        ? p.value()
                        : params[i].getName();
            }

            // 3) создаём RelationQueryExecutor
            return new RelationQueryExecutor(
                    jdbcTemplate,
                    entityMapper,
                    q.value(),
                    paramNames,
                    elementType,
                    isList
            );
        }

        return createCrudExecutor(method);
    }

    private QueryExecutor createCrudExecutor(Method method) {
        Class<?> entityType = getEntityType(repositoryInterface);
        CrudMetadata md    = getCrudMetadata(entityType);
        return switch (method.getName()) {
            case "findById" -> new RelationQueryExecutor(
                    jdbcTemplate, entityMapper,
                    String.format("SELECT * FROM %s WHERE %s = ?", md.getTableName(), md.getIdColumn()),
                    new String[] {"id"}, // единственный параметр
                    entityType, false
            );
            case "findAll" -> new RelationQueryExecutor(
                    jdbcTemplate, entityMapper,
                    String.format("SELECT * FROM %s", md.getTableName()),
                    new String[0], entityType, true
            );
            case "countAll" -> new PreparedQueryExecutor(
                    jdbcTemplate, entityMapper,
                    String.format("SELECT COUNT(*) FROM %s", md.getTableName()),
                    Long.class, false
            );
            case "create" -> new InsertExecutor(jdbcTemplate, entityMapper, md.getTableName(), entityType);
            case "update" -> new UpdateExecutor(jdbcTemplate, entityMapper,
                    md.getTableName(), entityType,
                    md.getIdFieldName(), md.getIdColumn());
            case "delete" -> new PreparedQueryExecutor(
                    jdbcTemplate, entityMapper,
                    String.format("DELETE FROM %s WHERE %s = ?", md.getTableName(), md.getIdColumn()),
                    void.class, false
            );
            default -> throw new RepositoryException("Unsupported CRUD method: " + method.getName());
        };
    }

    private Class<?> getEntityType(Class<?> repoInterface) {
        for (Type type : repoInterface.getGenericInterfaces()) {
            if (type instanceof ParameterizedType pt &&
                    pt.getRawType().equals(CrudRepository.class)) {
                return (Class<?>) pt.getActualTypeArguments()[0];
            }
        }
        for (Type type : repoInterface.getGenericInterfaces()) {
            if (type instanceof Class<?>) {
                return getEntityType((Class<?>) type);
            }
        }
        throw new RepositoryException("Entity type not found for " + repoInterface.getName());
    }

    private CrudMetadata getCrudMetadata(Class<?> entityType) {
        return metadataCache.computeIfAbsent(entityType, CrudMetadata::from);
    }


}
