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
public class UpdateExecutor implements QueryExecutor {
    private final JdbcTemplate jdbc;
    private final PreparedStatementCreatorFactory pscFactory;
    private final Field idField;
    private final List<Field> updateFields;      // поля-колонки
    private final List<Field> relationFields;    // поля-связи

    /**
     * @param jdbc           JdbcTemplate
     * @param entityMapper   EntityMapper (для getFields)
     * @param tableName      имя таблицы из @Table
     * @param entityType     класс-сущность
     * @param idFieldName    имя поля в entity, помеченного @Id
     * @param idColumn       имя колонки в БД для этого @Id
     */
    public UpdateExecutor(JdbcTemplate jdbc,
                          EntityMapper entityMapper,
                          String tableName,
                          Class<?> entityType,
                          String idFieldName,
                          String idColumn) {
        this.jdbc = jdbc;

        try {
            this.idField = entityType.getDeclaredField(idFieldName);
            idField.setAccessible(true);
        } catch (NoSuchFieldException ex) {
            throw new RepositoryException("No @Id field '" + idFieldName + "' in " + entityType.getName());
        }

        // простые поля @Column (без @Id и без связей)
        Map<String, Field> simple = entityMapper.getFields(entityType);
        this.updateFields = new ArrayList<>(simple.values());

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

        // Собираем имя всех колонок (простых + joinColumn.name()) и SQL-типы
        List<String> columnNames = new ArrayList<>();
        List<Integer> sqlTypes    = new ArrayList<>();

        // простые
        for (var entry : simple.entrySet()) {
            columnNames.add(entry.getKey());
            sqlTypes.add(mapJavaTypeToSqlType(entry.getValue().getType()));
        }

        // связи: для каждой ManyToOne / OneToOne-owner берём @JoinColumn.name()
        for (Field rf : relationFields) {
            JoinColumn jc = rf.getAnnotation(JoinColumn.class);
            if (jc == null) {
                throw new RepositoryException("Relation field " + rf.getName() + " missing @JoinColumn");
            }
            columnNames.add(jc.name());
            // тип — тот же, что и у PK целевой сущности; но JDBC примитивно: OTHER
            sqlTypes.add(Types.OTHER);
        }

        // добавляем тип для id в WHERE
        sqlTypes.add(mapJavaTypeToSqlType(idField.getType()));

        // 4) строим SET-часть
        String setClause = String.join(", ",
                columnNames.stream().map(c -> c + " = ?").toArray(String[]::new)
        );

        String sql = String.format("UPDATE %s SET %s WHERE %s = ?", tableName, setClause, idColumn);
        log.debug("Prepared UPDATE SQL: {}", sql);
        System.out.println(sql);

        this.pscFactory = new PreparedStatementCreatorFactory(
                sql,
                sqlTypes.stream().mapToInt(i -> i).toArray()
        );
    }

    @Override
    public Object execute(Object[] args) {
        Object entity = args[0];
        try {
            List<Object> params = new ArrayList<>();
            for (Field fld : updateFields) {
                params.add(fld.get(entity));
            }
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
            params.add(idField.get(entity));

            log.debug("Executing UPDATE with params: {}", params);
            jdbc.update(pscFactory.newPreparedStatementCreator(params));
            return entity;
        } catch (Exception ex) {
            throw new RepositoryException("Failed to execute UPDATE");
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
