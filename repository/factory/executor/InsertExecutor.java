package com.db.kurs.orm.repository.factory.executor;

import com.db.kurs.exception.RepositoryException;
import com.db.kurs.orm.annotation.link.JoinColumn;
import com.db.kurs.orm.mapper.EntityMapper;
import com.db.kurs.orm.mapper.QueryExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.*;

@Slf4j
public class InsertExecutor implements QueryExecutor {
    private final JdbcTemplate jdbc;
    private final PreparedStatementCreatorFactory pscFactory;
    private final List<Field> insertFields;     // простые @Column
    private final List<Field> relationFields;   // @ManyToOne, owner @OneToOne

    /**
     * @param jdbc         JdbcTemplate
     * @param entityMapper EntityMapper (для getFields)
     * @param tableName    имя таблицы из @Table
     * @param entityType   класс-сущность
     */
    public InsertExecutor(JdbcTemplate jdbc,
                          EntityMapper entityMapper,
                          String tableName,
                          Class<?> entityType) {
        this.jdbc = jdbc;

        // простые поля @Column (без @Id и без связей)
        Map<String, Field> simple = entityMapper.getFields(entityType);
        this.insertFields = new ArrayList<>(simple.values());

        // поля-связи @ManyToOne и owner @OneToOne
        this.relationFields = new ArrayList<>();
        for (Field f : entityType.getDeclaredFields()) {
            if (f.isAnnotationPresent(com.db.kurs.orm.annotation.link.ManyToOne.class) ||
                    (f.isAnnotationPresent(com.db.kurs.orm.annotation.link.OneToOne.class) &&
                            f.getAnnotation(com.db.kurs.orm.annotation.link.OneToOne.class).mappedBy().isEmpty())) {
                f.setAccessible(true);
                relationFields.add(f);
            }
        }

        List<String> columnNames = new ArrayList<>();
        List<Integer> sqlTypes    = new ArrayList<>();

        // простые
        for (var e : simple.entrySet()) {
            columnNames.add(e.getKey());
            sqlTypes.add(mapJavaTypeToSqlType(e.getValue().getType()));
        }
        // связи: берем имя @JoinColumn
        for (Field rf : relationFields) {
            var jc = rf.getAnnotation(JoinColumn.class);
            if (jc == null) throw new RepositoryException("Relation missing @JoinColumn on " + rf.getName());
            columnNames.add(jc.name());
            sqlTypes.add(Types.OTHER);
        }

        String cols = String.join(", ", columnNames);
        String vals = String.join(", ", Collections.nCopies(columnNames.size(), "?"));
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, cols, vals);
        log.debug("Prepared INSERT SQL: {}", sql);

        this.pscFactory = new PreparedStatementCreatorFactory(
                sql,
                sqlTypes.stream().mapToInt(i->i).toArray()
        );
    }

    @Override
    public Object execute(Object[] args) {
        Object entity = args[0];
        try {
            List<Object> params = new ArrayList<>();
            // простые
            for (Field fld : insertFields) {
                params.add(fld.get(entity));
            }
            // связи: stub.getId()
            for (Field rf : relationFields) {
                Object related = rf.get(entity);
                if (related == null) {
                    params.add(null);
                } else {
                    Field pk = new EntityMapper().getFieldWithIdAnnotation(related.getClass());
                    pk.setAccessible(true);
                    params.add(pk.get(related));
                }
            }

            log.debug("Executing INSERT with params: {}", params);
            jdbc.update(pscFactory.newPreparedStatementCreator(params));
            return entity;
        } catch (Exception ex) {
            throw new RepositoryException("Failed to execute INSERT");
        }
    }

    private int mapJavaTypeToSqlType(Class<?> cls) {
        if (cls == String.class)          return Types.VARCHAR;
        if (cls == Integer.class|| cls==int.class)    return Types.INTEGER;
        if (cls == Long.class   || cls==long.class)   return Types.BIGINT;
        if (cls == Boolean.class|| cls==boolean.class)return Types.BOOLEAN;
        if (cls == Double.class || cls==double.class)  return Types.DOUBLE;
        if (cls == Float.class  || cls==float.class)   return Types.FLOAT;
        if (cls == java.util.Date.class
                || cls == java.sql.Timestamp.class)            return Types.TIMESTAMP;
        if (cls == byte[].class)           return Types.BLOB;
        return Types.OTHER;
    }
}
