package cn.aethli.common.utils;

import cn.aethli.common.converter.BooleanConverter;
import cn.aethli.common.converter.Converter;
import cn.aethli.common.converter.LocalDateTimeConverter;
import cn.aethli.common.converter.StringConverter;
import cn.aethli.common.model.EntityMapper;
import cn.aethli.common.model.TableColumn;
import cn.aethli.datasource.ExpansionAbleConnectionPool;
import cn.aethli.entity.BaseEntity;
import cn.aethli.exception.DataRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * utils for database
 *
 * @author selcaNyan
 */
public class DataUtils {
    private static final Map<String, Converter<?>> CONVERTER_MAP = new HashMap<>();
    private static final Logger LOGGER = LogManager.getLogger(DataUtils.class);

    static {
        CONVERTER_MAP.put(Boolean.class.getTypeName(), new BooleanConverter());
        CONVERTER_MAP.put(String.class.getTypeName(), new StringConverter());
        CONVERTER_MAP.put(LocalDateTime.class.getTypeName(), new LocalDateTimeConverter());
    }


    /**
     * initialInternalDatabase
     *
     * @param resource h2 database resource path
     * @throws IOException if file copy fail
     */
    public static void initialInternalDatabase(String resource) throws IOException {

    }


    public static <T extends BaseEntity> boolean updateById(T entity) {
        String id = entity.getId();
        if (StringUtils.isEmpty(id)) {
            throw new DataRuntimeException("no id found in parameter entity");
        }
        // avoid to build sql within 'id=xxxx'
        entity.setId(null);
        String updateStatement = buildAssignmentStatement(entity);
        ExpansionAbleConnectionPool instance = ExpansionAbleConnectionPool.getInstance();
        Connection connection;
        try {
            EntityMapper entityMapper =
                    MetadataUtils.getEntityMapperByTypeName(entity.getClass().getTypeName());
            if (entityMapper == null) {
                throw new DataRuntimeException(
                        "Class metadata has not been initialized: " + entity.getClass().getTypeName());
            }
            String tableName = entityMapper.getTableName();

            String sql = "update `" + tableName + "` " + updateStatement;
            connection = instance.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int i = preparedStatement.executeUpdate();
            close(null, preparedStatement, connection);
            if (i == 1) {
                return true;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return false;
    }

    public static <T extends BaseEntity> T selectOne(T entity) {
        Class<? extends BaseEntity> aClass = entity.getClass();
        String whereStatement = buildWhereStatement(entity);
        ExpansionAbleConnectionPool instance = ExpansionAbleConnectionPool.getInstance();
        Connection connection;
        try {
            EntityMapper entityMapper =
                    MetadataUtils.getEntityMapperByTypeName(entity.getClass().getTypeName());
            if (entityMapper == null) {
                throw new DataRuntimeException(
                        "Class metadata has not been initialized: " + entity.getClass().getTypeName());
            }
            String tableName = entityMapper.getTableName();

            String sql = "select * from `" + tableName + "` where " + whereStatement + " LIMIT 1";
            connection = instance.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            Map<String, String> fieldMap = new HashMap<>();
            if (resultSet.next()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int count = metaData.getColumnCount();
                for (int i = 1; i <= count; i++) {
                    fieldMap.put(metaData.getColumnLabel(i), resultSet.getString(i));
                }
                close(resultSet, preparedStatement, connection);
                return (T) mapperBy(aClass, fieldMap);
            } else {
                close(resultSet, preparedStatement, connection);
                return null;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }

    public static <T extends BaseEntity> boolean insertOne(T entity) {
        entity.setId(UUID.randomUUID().toString());
        String insertStatement = buildAssignmentStatement(entity);
        ExpansionAbleConnectionPool instance = ExpansionAbleConnectionPool.getInstance();
        Connection connection;
        try {
            EntityMapper entityMapper =
                    MetadataUtils.getEntityMapperByTypeName(entity.getClass().getTypeName());
            if (entityMapper == null) {
                throw new DataRuntimeException(
                        "Class metadata has not been initialized: " + entity.getClass().getTypeName());
            }
            String tableName = entityMapper.getTableName();

            String sql = "insert into `" + tableName + "` " + insertStatement;
            connection = instance.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int i = preparedStatement.executeUpdate();
            close(null, preparedStatement, connection);
            if (i == 1) {
                return true;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return false;
    }

    public static <T extends BaseEntity> String buildWhereStatement(T entity) {
        List<String> wheres = getStatement(entity);
        return String.join(" AND ", wheres);
    }

    public static <T extends BaseEntity> String buildAssignmentStatement(T entity) {
        List<String> assignments = getStatement(entity);
        return "SET " + String.join(",", assignments);
    }

    private static <T extends BaseEntity> List<String> getStatement(T entity) {
        List<String> statementList = new ArrayList<>();
        String typeName = entity.getClass().getTypeName();
        EntityMapper entityMapperByTypeName = MetadataUtils.getEntityMapperByTypeName(typeName);
        if (entityMapperByTypeName == null || entityMapperByTypeName.getFields() == null) {
            throw new DataRuntimeException(
                    "Class metadata has not been initialized: " + entity.getClass().getTypeName());
        }
        Set<Field> fields = entityMapperByTypeName.getFields();
        Set<TableColumn> tableColumns = entityMapperByTypeName.getTableColumns();
        Map<String, TableColumn> tableColumnMap =
                tableColumns.parallelStream()
                        .collect(Collectors.toMap(TableColumn::getAlias, tableColumn -> tableColumn));
        fields.forEach(
                field -> {
                    String name = field.getName();
                    try {
                        field.setAccessible(true);
                        Object o = field.get(entity);
                        if (o != null) {
                            statementList.add(
                                    String.format(
                                            "`%s`='%s'",
                                            tableColumnMap.get(name).getColumnName(),
                                            CONVERTER_MAP.get(field.getType().getTypeName()).parse(o)));
                        }
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                });
        return statementList;
    }

    public static <T extends BaseEntity> T mapperBy(Class<T> tClass, Map<String, String> fieldMap) {
        try {
            T t = tClass.newInstance();
            EntityMapper entityMapperByTypeName =
                    MetadataUtils.getEntityMapperByTypeName(tClass.getTypeName());
            if (entityMapperByTypeName == null || entityMapperByTypeName.getFields() == null) {
                return null;
            }
            Set<Field> fieldsByTypeName = entityMapperByTypeName.getFields();
            Map<String, String> aliasColumnMap =
                    entityMapperByTypeName.getTableColumns().parallelStream()
                            .collect(Collectors.toMap(TableColumn::getAlias, TableColumn::getColumnName));
            for (Field field : fieldsByTypeName) {
                Converter<?> converter = CONVERTER_MAP.get(field.getType().getTypeName());
                if (converter != null) {
                    field.set(t, converter.valueOf(fieldMap.get(aliasColumnMap.get(field.getName()))));
                }
            }
            return t;
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void close(
            ResultSet resultSet, PreparedStatement preparedStatement, Connection connection) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
