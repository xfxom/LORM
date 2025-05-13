package com.db.kurs.orm.repository.factory.executor;

import com.db.kurs.orm.annotation.Table;
import com.db.kurs.orm.mapper.EntityMapper;
import com.db.kurs.orm.mapper.QueryExecutor;
import com.db.kurs.orm.metadata.EntityMetadata;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PreparedQueryExecutor implements QueryExecutor {

    private final JdbcTemplate jdbcTemplate;
    private final EntityMapper entityMapper;
    private final String parsedSql;
    private final int[] paramOrder;
    private final Class<?> elementType;
    private final boolean isList;
    private final boolean isEntity;

    public PreparedQueryExecutor(JdbcTemplate jdbcTemplate,
                                 EntityMapper entityMapper,
                                 String sql,
                                 Class<?> elementType,
                                 boolean isList) {
        this.jdbcTemplate = jdbcTemplate;
        this.entityMapper = entityMapper;
        this.elementType = elementType;
        this.isList = isList;
        // считаем, что «сущность» — это класс с @Table и хотя бы одним @Id
        this.isEntity = elementType.isAnnotationPresent(Table.class)
                && !new EntityMetadata(elementType).idFields.isEmpty();

        // разбираем ?1,?2 → ? и собираем paramOrder (ваш уже готовый код)
        Pattern p = Pattern.compile("\\?(\\d+)");
        Matcher m = p.matcher(sql);
        StringBuffer sb = new StringBuffer();
        List<Integer> order = new ArrayList<>();
        while (m.find()) {
            order.add(Integer.parseInt(m.group(1)) - 1);
            m.appendReplacement(sb, "?");
        }
        m.appendTail(sb);
        this.parsedSql = sb.toString();
        this.paramOrder = order.isEmpty() ? null : order.stream().mapToInt(i -> i).toArray();
    }

    @Override
    public Object execute(Object[] args) {
        // перестановка по paramOrder
        Object[] finalArgs = args;
        if (paramOrder != null) {
            finalArgs = new Object[paramOrder.length];
            for (int i = 0; i < paramOrder.length; i++) {
                finalArgs[i] = args[paramOrder[i]];
            }
        }

        if (void.class.equals(elementType)) {
            // DML
            if (finalArgs != null && finalArgs.length > 0) {
                jdbcTemplate.update(parsedSql, finalArgs);
            } else {
                jdbcTemplate.update(parsedSql);
            }
            return null;
        }

        if (!isEntity) {
            // СКАЛЯРЫ / DTO: просто вытаскиваем из первой колонки
            if (isList) {
                return jdbcTemplate.queryForList(parsedSql, elementType, finalArgs);
            } else {
                return jdbcTemplate.queryForObject(parsedSql, elementType, finalArgs);
            }
        }

        // ЕЩЁ ЗДЕСЬ — сущности, мапим через EntityMapper
        RowMapper<?> rm = (rs, rowNum) -> entityMapper.map(rs, elementType);
        List<?> result = (finalArgs != null && finalArgs.length > 0)
                ? jdbcTemplate.query(parsedSql, finalArgs, rm)
                : jdbcTemplate.query(parsedSql, rm);

        return isList ? result : (result.isEmpty() ? null : result.get(0));
    }
    /**
     *  на основе типа Java-объекта возвращает java.sql.Types
     */
    private int[] resolveTypes(Object[] args) {
        int[] types = new int[args.length];
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg == null) {
                types[i] = Types.NULL;
            } else if (arg instanceof String) {
                types[i] = Types.VARCHAR;
            } else if (arg instanceof Integer) {
                types[i] = Types.INTEGER;
            } else if (arg instanceof Long) {
                types[i] = Types.BIGINT;
            } else if (arg instanceof Boolean) {
                types[i] = Types.BOOLEAN;
            } else if (arg instanceof Double) {
                types[i] = Types.DOUBLE;
            } else if (arg instanceof Float) {
                types[i] = Types.FLOAT;
            } else if (arg instanceof Short) {
                types[i] = Types.SMALLINT;
            } else if (arg instanceof java.math.BigDecimal) {
                types[i] = Types.NUMERIC;
            } else if (arg instanceof java.sql.Date) {
                types[i] = Types.DATE;
            } else if (arg instanceof java.sql.Time) {
                types[i] = Types.TIME;
            } else if (arg instanceof java.sql.Timestamp) {
                types[i] = Types.TIMESTAMP;
            } else {
                types[i] = Types.JAVA_OBJECT;
            }
        }
        return types;
    }
}
