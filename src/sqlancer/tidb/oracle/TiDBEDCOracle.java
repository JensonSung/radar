package sqlancer.tidb.oracle;

import com.beust.jcommander.Strings;
import sqlancer.Randomly;
import sqlancer.common.oracle.EDCBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.tidb.TiDBEDC;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider;
import sqlancer.tidb.TiDBSchema;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.visitor.TiDBVisitor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TiDBEDCOracle extends EDCBase<TiDBProvider.TiDBGlobalState> implements TestOracle<TiDBProvider.TiDBGlobalState> {
    public TiDBEDCOracle(TiDBProvider.TiDBGlobalState originalState) {
        super(originalState);
        TiDBErrors.addExpressionErrors(errors);
        errors.add("Empty pattern is invalid in regexp");
        errors.add("Invalid regexp pattern");
    }

    @Override
    public Map<String, Map<String, List<String>>> obtainTableSchemas(TiDBProvider.TiDBGlobalState state) throws SQLException {
        Map<String, Map<String, List<String>>> tableSchema = new HashMap<>();
        List<String> foreignKeyList = new ArrayList<>();
        Pattern patternForColumnSet = Pattern.compile("\\((.*?)\\)");
        Pattern patternForColumn = Pattern.compile("`([^`]*?)`");
        boolean needComma;

        for (TiDBSchema.TiDBTable table : state.getSchema().getDatabaseTablesWithoutViews()) {
            String tableName = table.getName();
            try (Statement statement = state.getConnection().createStatement()) {
                ResultSet resultSet = statement.executeQuery("SHOW CREATE TABLE " + tableName);
                String createTable = null;
                if (resultSet.next()) {
                    createTable = resultSet.getString("Create Table");
                }
                if (createTable != null) {
                    tableSchema.put(tableName, new HashMap<>());
                    String[] createTableLines = createTable.split("\n");
                    // handle column-related metadata
                    Map<String, String> generatedColumns = new HashMap<>();
                    String columnName = table.getColumns().get(0).getName();
                    needComma = createTableLines[1].startsWith("  `" + columnName + "`"); // column name is not continuous
                    for (int i = 0; i < table.getColumns().size(); i++) {
                        columnName = table.getColumns().get(i).getName();
                        String columnRef = "`" + columnName + "`";
                        int offset = 5;
                        if (!needComma) {
                            columnRef = columnName;
                            offset = 3;
                        }
                        int start = createTableLines[i + 1].indexOf(columnRef) + offset;
                        String metadataString = createTableLines[i + 1].substring(start, createTableLines[i + 1].length() - 1);
                        if (metadataString.contains("GENERATED")) {
                            generatedColumns.put(columnRef, metadataString);
                        } else {
                            tableSchema.get(tableName).put(columnRef, List.of(metadataString));
                        }
                    }

                    // handle generated column
                    for (String generatedColumn : generatedColumns.keySet()) {
                        String expression = generatedColumns.get(generatedColumn);
                        Matcher matcher = patternForColumn.matcher(expression);
                        List<String> metaElements = new ArrayList<>();
                        while (matcher.find()) {
                            String columnRef = matcher.group(1);
                            if (needComma) {
                                columnRef = "`" + columnRef + "`";
                            }
                            if (tableSchema.get(tableName).containsKey(columnRef)) { // handle constant generate
                                metaElements.add(Strings.join(" ", tableSchema.get(tableName).get(columnRef)));
                            }
                        }
                        Collections.sort(metaElements);
                        metaElements.add(0, "GENERATED");
                        tableSchema.get(tableName).put(generatedColumn, metaElements);
                    }

                    // handle table-related metadata
                    for (int i = table.getColumns().size() + 1; i < createTableLines.length - 1; i++) {
                        if (createTableLines[i].contains("ENGINE")) { // to the end of create table
                            tableSchema.get(tableName).put("CONFIGURATION", List.of(createTableLines[i].substring(1)));
                            break;
                        }
                        Matcher matcherColumnSet = patternForColumnSet.matcher(createTableLines[i]);
                        List<String> compositeColumns = new ArrayList<>();
                        if (matcherColumnSet.find()) {
                            String columns = matcherColumnSet.group(1);
                            if (columns.contains(",")) {
                                Matcher matcherColumn = patternForColumn.matcher(columns);
                                while (matcherColumn.find()) {
                                    String columnRef = matcherColumn.group(1);
                                    if (needComma) {
                                        columnRef = "`" + columnRef + "`";
                                    }
                                    compositeColumns.add(columnRef);
                                }
                            } else {
                                compositeColumns.add(matcherColumnSet.group(1));
                            }
                        }
                        String metadataType = "";
                        if (createTableLines[i].contains("UNIQUE")) {
                            metadataType = "UNIQUE";
                        } else if (createTableLines[i].contains("PRIMARY")) {
                            metadataType = "PRIMARY";
                        } else if (createTableLines[i].contains("FOREIGN")) {
                            // handle foreign key until finishing all single-table metadata
                            foreignKeyList.add(tableName);
                            foreignKeyList.add(compositeColumns.get(0));
                            String references = createTableLines[i].substring(createTableLines[i].indexOf("REFERENCES") + 11);
                            Matcher matcher = patternForColumn.matcher(references); // `test`.`t1` (`c0`)
                            if (matcher.find()) {
                                // ignored the database name
                            }
                            if (matcher.find()) {
                                foreignKeyList.add(matcher.group(1));
                            }
                            if (matcher.find()) {
                                if (needComma) {
                                    foreignKeyList.add("`" + matcher.group(1) + "`");
                                } else {
                                    foreignKeyList.add(matcher.group(1));
                                }
                            }
                            continue;
                        } else if (createTableLines[i].contains("CHECK")) {
                            metadataType = "CHECK"; // TiDB parses but ignores CHECK constraints, https://docs.pingcap.com/tidb/stable/constraints
                            int start = createTableLines[i].indexOf(metadataType) + 5;
                            String metadataString = createTableLines[i].substring(start, createTableLines[i].length() - 1);
                            Matcher matcher = patternForColumn.matcher(metadataString);
                            List<String> metaElements = new ArrayList<>();
                            while (matcher.find()) {
                                String columnRef = matcher.group(1);
                                if (needComma) {
                                    columnRef = "`" + columnRef + "`";
                                }
                                if (tableSchema.get(tableName).containsKey(columnRef)) { // handle constant generate
                                    metaElements.add(Strings.join(" ", tableSchema.get(tableName).get(columnRef)));
                                }
                            }
                            Collections.sort(metaElements);
                            metaElements.add(0, metadataType);
                            tableSchema.get(tableName).put(metadataType + i, metaElements);
                            continue;
                        } else if (createTableLines[i].contains("KEY")) {
                            metadataType = "KEY";
                        }
                        List<String> columnRelatedMetadata = new ArrayList<>();
                        for (String columnRef : compositeColumns) {
                            if (tableSchema.get(tableName).containsKey(columnRef)) { // avoid constant expression
                                columnRelatedMetadata.addAll(tableSchema.get(tableName).get(columnRef));
                            }
                        }
                        Collections.sort(columnRelatedMetadata);
                        columnRelatedMetadata.add(0, metadataType);
                        tableSchema.get(tableName).put(metadataType + i, columnRelatedMetadata);
                    }
                }
            }
        }

        if (!foreignKeyList.isEmpty()) {
            for (int i = 0; i < foreignKeyList.size() / 4; i++) {
                List<String> foreignExpression = new ArrayList<>();
                foreignExpression.add("FOREIGN KEY");

                String source = Strings.join(" ", tableSchema.get(foreignKeyList.get(i)).get(foreignKeyList.get(i + 1)));
                String target = Strings.join(" ", tableSchema.get(foreignKeyList.get(i + 2)).get(foreignKeyList.get(i + 3)));
                foreignExpression.add(source);
                foreignExpression.add(target);

                tableSchema.get(foreignKeyList.get(i)).put("FOREIGN KEY" + i, foreignExpression);
            }
        }
        return tableSchema;
    }

    @Override
    public TiDBProvider.TiDBGlobalState constructEquivalentState(TiDBProvider.TiDBGlobalState state) {
        try {
            TiDBEDC edc = new TiDBEDC(state);
            return edc.createRawDB();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String generateQueryString(TiDBProvider.TiDBGlobalState state) {
        TiDBSchema.TiDBTables randomTables = state.getSchema().getRandomTableNonEmptyTables();
        List<TiDBSchema.TiDBColumn> columns = randomTables.getColumns();
        TiDBExpressionGenerator expressionGenerator = new TiDBExpressionGenerator(state).setColumns(columns);
        TiDBExpression randomWhereCondition = expressionGenerator.generateExpression();
        List<TiDBExpression> tableRefs = randomTables.getTables().stream().map(TiDBTableReference::new).collect(Collectors.toList());
        TiDBSelect select = new TiDBSelect();
        select.setFetchColumns((Randomly.nonEmptySubset(randomTables.getColumns()).stream().map(TiDBColumnReference::new).collect(Collectors.toList())));
        select.setFromList(tableRefs);
        select.setWhereClause(randomWhereCondition);
        // no group by in tidb, too many errors
//        if (Randomly.getBooleanWithSmallProbability()) {
//            select.setGroupByExpressions(expressionGenerator.generateExpressions(Randomly.smallNumber() + 1));
//            if (Randomly.getBooleanWithSmallProbability()) {
//                select.setHavingClause(expressionGenerator.generateHavingClause());
//            }
//        }
//        if (Randomly.getBooleanWithSmallProbability()) {
//            select.setOrderByExpressions(expressionGenerator.generateOrderBys());
//        }

        return TiDBVisitor.asString(select);
    }

}
