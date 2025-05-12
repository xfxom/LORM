package com.db.kurs.orm.repository.factory.executor;

import com.db.kurs.orm.annotation.link.FetchType;
import com.db.kurs.orm.annotation.link.JoinColumn;
import com.db.kurs.orm.mapper.EntityMapper;
import com.db.kurs.orm.mapper.QueryExecutor;
import com.db.kurs.orm.metadata.EntityMetadata;
import com.db.kurs.orm.metadata.RelationshipMetadata;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.sql.ResultSet;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class RelationQueryExecutor implements QueryExecutor {
    private final JdbcTemplate jdbc;
    private final EntityMapper mapper;
    private final String parsedSql;
    private final int[] paramOrder;
    private final Class<?> rootType;
    private final boolean isList;
    private final EntityMetadata rootMeta;

    public RelationQueryExecutor(JdbcTemplate jdbc,
                                 EntityMapper mapper,
                                 String sql,
                                 String[] paramNames,
                                 Class<?> rootType,
                                 boolean isList) {
        this.jdbc     = jdbc;
        this.mapper   = mapper;
        this.rootType = rootType;
        this.isList   = isList;
        this.rootMeta = new EntityMetadata(rootType);

        List<Integer> order = new ArrayList<>();
        Pattern named = Pattern.compile(":(\\w+)");
        Matcher mn = named.matcher(sql);
        StringBuffer sb1 = new StringBuffer();
        while (mn.find()) {
            String name = mn.group(1);
            int idx = Arrays.asList(paramNames).indexOf(name);
            if (idx < 0) {
                throw new IllegalArgumentException("Unknown parameter name in query: " + name);
            }
            order.add(idx);
            mn.appendReplacement(sb1, "?");
        }
        mn.appendTail(sb1);
        String afterNamed = sb1.toString();

        // обрабатываем ?1,?2… позиционные
        Pattern positional = Pattern.compile("\\?(\\d+)");
        Matcher mp = positional.matcher(afterNamed);
        StringBuffer sb2 = new StringBuffer();
        while (mp.find()) {
            int pos = Integer.parseInt(mp.group(1)) - 1;
            order.add(pos);
            mp.appendReplacement(sb2, "?");
        }
        mp.appendTail(sb2);
        this.parsedSql = sb2.toString();

        this.paramOrder = order.isEmpty()
                ? null
                : order.stream().mapToInt(i -> i).toArray();
    }

    @Override
    public Object execute(Object[] args) {
        // Переставляем args в соответствии с paramOrder, если нужно
        Object[] finalArgs = args;
        if (paramOrder != null) {
            finalArgs = new Object[paramOrder.length];
            for (int i = 0; i < paramOrder.length; i++) {
                finalArgs[i] = args[paramOrder[i]];
            }
        }

        log.debug("Executing base query: {} | params: {}", parsedSql, Arrays.toString(finalArgs));
        List<Object> roots = jdbc.query(
                parsedSql,
                (ResultSet rs, int rn) -> mapper.map(rs, rootType),
                finalArgs
        );
        if (roots.isEmpty()) return isList ? roots : null;

        // Batch-загрузка связей, как раньше
        Map<Class<?>, List<Object>> toProcess = new LinkedHashMap<>();
        toProcess.put(rootType, roots);
        Set<Class<?>> visited = new HashSet<>();

        while (!toProcess.isEmpty()) {
            Map<Class<?>, List<Object>> next = new LinkedHashMap<>();
            for (var entry : toProcess.entrySet()) {
                Class<?> cls = entry.getKey();
                if (!visited.add(cls)) continue;
                EntityMetadata meta = new EntityMetadata(cls);
                for (RelationshipMetadata rel : meta.relations) {
                    if (rel.fetch == FetchType.LAZY) continue;
                    switch (rel.type) {
                        case MANY_TO_ONE  -> batchManyToOne  (entry.getValue(), rel, next);
                        case ONE_TO_ONE   -> batchOneToOne   (entry.getValue(), meta, rel, next);
                        case ONE_TO_MANY  -> batchOneToMany  (entry.getValue(), meta, rel, next);
                        case MANY_TO_MANY -> batchManyToMany (entry.getValue(), meta, rel, next);
                    }
                }
            }
            toProcess = next;
        }

        return isList ? roots : roots.get(0);
    }

    private void batchManyToOne(List<Object> parents,
                                RelationshipMetadata rel,
                                Map<Class<?>, List<Object>> next) {
        Class<?> targetType = rel.field.getType();
        EntityMetadata tm = new EntityMetadata(targetType);

        // собрать (parent → FK) и уникальные FK
        Map<Object, List<Object>> fkToParents = new HashMap<>();
        for (Object p : parents) {
            try {
                Object stub = rel.field.get(p);
                if (stub == null) continue;
                Field idF = tm.idFields.get(0);
                idF.setAccessible(true);
                Object id = idF.get(stub);
                if (id != null) {
                    fkToParents.computeIfAbsent(id, k->new ArrayList<>()).add(p);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        if (fkToParents.isEmpty()) return;

        List<Object> ids = new ArrayList<>(fkToParents.keySet());
        String in  = ids.stream().map(i->"?").collect(Collectors.joining(","));
        String qs  = String.format("SELECT * FROM %s WHERE %s IN (%s)",
                tm.tableName, tm.idColumns.get(0), in);
        log.debug("Batch MANY_TO_ONE [{}] SQL: {} | params: {}",
                targetType.getSimpleName(), qs, ids);

        // получить всех детей
        List<Object> children = jdbc.query(qs,
                (rs,rn)->mapper.map(rs, targetType),
                ids.toArray()
        );
        // сгруппировать child by id
        Map<Object,Object> idToChild = new HashMap<>();
        for (Object c : children) {
            try {
                Field idF = tm.idFields.get(0);
                idF.setAccessible(true);
                idToChild.put(idF.get(c), c);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        // записать обратно в родителей
        fkToParents.forEach((id, ps) -> {
            Object child = idToChild.get(id);
            for (Object p : ps) {
                try { rel.field.set(p, child); }
                catch (Exception ex) { throw new RuntimeException(ex); }
            }
        });

        // добавляем детей в очередь на следующий виток
        next.merge(targetType, children, (old, more)->{ old.addAll(more); return old; });
    }

    private void batchOneToOne(List<Object> parents,
                               EntityMetadata parentMeta,
                               RelationshipMetadata rel,
                               Map<Class<?>, List<Object>> next) {
        if (Objects.requireNonNull(rel.mappedBy).isEmpty()) {
            // владеющая сторона – копируем логику MANY_TO_ONE
            batchManyToOne(parents, rel, next);
        } else {
            // inverse: FK лежит в target-таблице как столбец mappedBy
            Class<?> targetType = rel.field.getType();
            EntityMetadata tm = new EntityMetadata(targetType);
            String fk   = rel.mappedBy;

            // собрать parent IDs
            List<Object> pids = parents.stream().map(p->{
                try {
                    Field idF = parentMeta.idFields.get(0);
                    idF.setAccessible(true);
                    return idF.get(p);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }).collect(Collectors.toList());
            if (pids.isEmpty()) return;
            String in  = pids.stream().map(i->"?").collect(Collectors.joining(","));
            String qs  = String.format("SELECT * FROM %s WHERE %s IN (%s)",
                    tm.tableName, fk, in);
            log.debug("Batch ONE_TO_ONE [{}] SQL: {} | params: {}",
                    targetType.getSimpleName(), qs, pids);

            List<Object> children = jdbc.query(qs,
                    (rs,rn)->mapper.map(rs, targetType),
                    pids.toArray()
            );
            // сгруппировать по FK (значение в поле mappedBy)
            Map<Object,Object> ownerToChild = new HashMap<>();
            for (Object c : children) {
                try {
                    Field fkField = c.getClass().getDeclaredField(fk);
                    fkField.setAccessible(true);
                    ownerToChild.put(fkField.get(c), c);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            // записать обратно
            for (int i = 0; i < parents.size(); i++) {
                Object p = parents.get(i);
                Object pid = pids.get(i);
                try { rel.field.set(p, ownerToChild.get(pid)); }
                catch (Exception ex) { throw new RuntimeException(ex); }
            }

            next.merge(targetType, children, (o,n)->{ o.addAll(n); return o; });
        }
    }

    private void batchOneToMany(List<Object> parents,
                                EntityMetadata parentMeta,
                                RelationshipMetadata rel,
                                Map<Class<?>, List<Object>> next) {
        // child side
        ParameterizedType pt = (ParameterizedType) rel.field.getGenericType();
        Class<?> childType = (Class<?>) pt.getActualTypeArguments()[0];
        EntityMetadata cm = new EntityMetadata(childType);

        Field childFld;
        try {
            childFld = childType.getDeclaredField(Objects.requireNonNull(rel.mappedBy));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        String fkCol = childFld.getAnnotation(JoinColumn.class).name();

        // собрать parent IDs
        List<Object> pids = parents.stream().map(p->{
            try {
                Field idF = parentMeta.idFields.get(0);
                idF.setAccessible(true);
                return idF.get(p);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }).collect(Collectors.toList());
        if (pids.isEmpty()) return;
        String in  = pids.stream().map(i->"?").collect(Collectors.joining(","));
        String qs  = String.format("SELECT * FROM %s WHERE %s IN (%s)",
                cm.tableName, fkCol, in);
        log.debug("Batch ONE_TO_MANY [{}] SQL: {} | params: {}",
                childType.getSimpleName(), qs, pids);

        List<Object> children = jdbc.query(qs,
                (rs,rn)->mapper.map(rs, childType),
                pids.toArray()
        );
        // сгруппировать по parent FK
        Map<Object,List<Object>> grouping = new HashMap<>();
        for (Object c: children) {
            try {
                Field fkField = c.getClass().getDeclaredField(rel.mappedBy);
                fkField.setAccessible(true);
                Object ownerId = fkField.get(c);
                grouping.computeIfAbsent(ownerId, k->new ArrayList<>()).add(c);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        // записать обратно
        for (int i=0;i<parents.size();i++) {
            Object p = parents.get(i);
            Object pid = pids.get(i);
            List<Object> list = grouping.getOrDefault(pid, Collections.emptyList());
            try { rel.field.set(p, list); }
            catch (Exception ex) { throw new RuntimeException(ex); }
        }

        next.merge(childType, children, (o,n)->{ o.addAll(n); return o; });
    }

    private void batchManyToMany(List<Object> parents,
                                 EntityMetadata parentMeta,
                                 RelationshipMetadata rel,
                                 Map<Class<?>, List<Object>> next) {
        // целевой тип
        ParameterizedType pt = (ParameterizedType) rel.field.getGenericType();
        Class<?> targetType = (Class<?>) pt.getActualTypeArguments()[0];
        EntityMetadata tm = new EntityMetadata(targetType);

        String joinTbl = rel.joinTable;
        // owner FK и target FK
        JoinColumn ownerJ = rel.joinColumns.stream()
                .filter(j->j.referencedColumnName().equalsIgnoreCase(parentMeta.idColumns.get(0)))
                .findFirst().orElseThrow();
        JoinColumn targetJ= rel.joinColumns.stream()
                .filter(j->j.referencedColumnName().equalsIgnoreCase(tm.idColumns.get(0)))
                .findFirst().orElseThrow();
        String ownerCol = ownerJ.name(), targetCol = targetJ.name();

        // 1) собрать parent IDs
        List<Object> pids = parents.stream().map(p->{
            try {
                Field idF = parentMeta.idFields.get(0);
                idF.setAccessible(true);
                return idF.get(p);
            } catch (Exception ex) { throw new RuntimeException(ex); }
        }).collect(Collectors.toList());
        if (pids.isEmpty()) return;
        String in1 = pids.stream().map(i->"?").collect(Collectors.joining(","));

        // 2) из joinTable вытянуть пары owner→target
        String jql = String.format(
                "SELECT %s AS owner_id, %s AS target_id FROM %s WHERE %s IN (%s)",
                ownerCol, targetCol, joinTbl, ownerCol, in1
        );
        log.debug("Batch MANY_TO_MANY map SQL: {} | params: {}", jql, pids);
        List<Map<String,Object>> rows = jdbc.query(jql,
                (rs,rn)->{
                    Map<String,Object> m = new HashMap<>();
                    m.put("o", rs.getObject(1));
                    m.put("t", rs.getObject(2));
                    return m;
                },
                pids.toArray()
        );
        // группировать owner→List<targetId>
        Map<Object,List<Object>> map = new HashMap<>();
        rows.forEach(r-> map.computeIfAbsent(r.get("o"), k->new ArrayList<>()).add(r.get("t")));
        if (map.isEmpty()) return;

        // 3) вытянуть всех target
        Set<Object> allT = map.values().stream().flatMap(List::stream).collect(Collectors.toSet());
        String in2 = allT.stream().map(i->"?").collect(Collectors.joining(","));
        String sql2= String.format("SELECT * FROM %s WHERE %s IN (%s)",
                tm.tableName, tm.idColumns.get(0), in2);
        log.debug("Batch MANY_TO_MANY [{}] SQL: {} | params: {}",
                targetType.getSimpleName(), sql2, allT);
        List<Object> targets = jdbc.query(sql2,
                (rs,rn)->mapper.map(rs, targetType),
                allT.toArray()
        );
        // id→object
        Map<Object,Object> id2obj = new HashMap<>();
        for (Object t : targets) {
            try {
                Field idF = tm.idFields.get(0);
                idF.setAccessible(true);
                id2obj.put(idF.get(t), t);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        // 4) записать обратно в родителей
        for (Object p: parents) {
            try {
                Field idF = parentMeta.idFields.get(0);
                idF.setAccessible(true);
                Object pid = idF.get(p);
                List<Object> tids = map.getOrDefault(pid, Collections.emptyList());
                List<Object> objs = tids.stream().map(id2obj::get).collect(Collectors.toList());
                rel.field.set(p, objs);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        next.merge(targetType, targets, (o,n)->{ o.addAll(n); return o; });
    }
}
