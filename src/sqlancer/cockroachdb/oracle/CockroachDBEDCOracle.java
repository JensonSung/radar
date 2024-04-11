package sqlancer.cockroachdb.oracle;

import com.beust.jcommander.Strings;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBCommon;
import sqlancer.cockroachdb.CockroachDBEDC;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.common.oracle.EDCBase;
import sqlancer.common.oracle.TestOracle;

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

import static sqlancer.cockroachdb.oracle.CockroachDBNoRECOracle.getJoins;

public class CockroachDBEDCOracle extends EDCBase<CockroachDBProvider.CockroachDBGlobalState> implements TestOracle<CockroachDBProvider.CockroachDBGlobalState> {

    public CockroachDBEDCOracle(CockroachDBProvider.CockroachDBGlobalState state) {
        super(state);
        CockroachDBErrors.addExpressionErrors(errors);
        CockroachDBErrors.addTransactionErrors(errors);
        errors.add("unable to vectorize execution plan"); // SET vectorize=experimental_always;
        errors.add("mismatched physical types at index"); // SET vectorize=experimental_always;
    }

    @Override
    public Map<String, Map<String, List<String>>> obtainTableSchemas(CockroachDBProvider.CockroachDBGlobalState state) throws SQLException {
        Map<String, Map<String, List<String>>> tableSchema = new HashMap<>();
        List<String> foreignKeyList = new ArrayList<>();
        Pattern patternForColumn = Pattern.compile("c\\d+");
        for (CockroachDBSchema.CockroachDBTable table : state.getSchema().getDatabaseTablesWithoutViews()) {
            String tableName = table.getName();
            try (Statement statement = state.getConnection().createStatement()) {
                tableSchema.put(tableName, new HashMap<>());
                Map<String, String> generatedColumns = new HashMap<>();
                String getColumnInfo = String.format("SHOW COLUMNS FROM %s", tableName);
                ResultSet resultSet = statement.executeQuery(getColumnInfo);
                while (resultSet.next()) {
                    boolean isHidden = resultSet.getBoolean("is_hidden");
                    if (isHidden) {
                        continue;
                    }
                    List<String> metaElements = new ArrayList<>();
                    String dataType = resultSet.getString("data_type");
                    String columnName = resultSet.getString("column_name");
                    boolean isNullable = resultSet.getBoolean("is_nullable");
                    String hasDefault = resultSet.getString("column_default");
                    String isGenerated = resultSet.getString("generation_expression");
                    metaElements.add(dataType);
                    if (!isNullable) {
                        metaElements.add("NOT NULL");
                    }
                    if (hasDefault != null) {
                        metaElements.add("DEFAULT " + hasDefault);
                    }
                    if (!isGenerated.equals("")) {
                        generatedColumns.put(columnName, isGenerated);
                    }
                    tableSchema.get(tableName).put(columnName, metaElements);
                }

                for (String generatedColumn : generatedColumns.keySet()) {
                    String expression = generatedColumns.get(generatedColumn);
                    Matcher matcher = patternForColumn.matcher(expression);
                    List<String> metaElements = new ArrayList<>();
                    while (matcher.find()) {
                        String columnRef = matcher.group();
                        if (tableSchema.get(tableName).containsKey(columnRef)) { // to avoid constant
                            metaElements.add(Strings.join(" ", tableSchema.get(tableName).get(columnRef)));
                        }
                    }
                    Collections.sort(metaElements);
                    metaElements.add(0, "GENERATED");
                    tableSchema.get(tableName).get(generatedColumn).addAll(metaElements);
                }

                String getConstraints = String.format("SHOW CONSTRAINTS FROM %s", tableName);
                resultSet = statement.executeQuery(getConstraints);
                while (resultSet.next()) {
                    String constraintName = resultSet.getString("constraint_name");
                    String constraintType = resultSet.getString("constraint_type");
                    String expression = resultSet.getString("details");
                    if (constraintType.equals("FOREIGN KEY")) {
                        foreignKeyList.add(tableName);
                        foreignKeyList.add(constraintName);
                        Pattern patternForTable = Pattern.compile("t\\d+");
                        Matcher matchTable = patternForTable.matcher(expression);
                        if (matchTable.find()) {
                            foreignKeyList.add(matchTable.group());
                        }
                        Matcher matchColumn = patternForColumn.matcher(expression);
                        List<String> metaElements = new ArrayList<>();
                        if (matchColumn.find()) {
                            String columnRef = matchColumn.group();
                            if (tableSchema.get(tableName).containsKey(columnRef)) { // to avoid constant
                                metaElements.add(Strings.join(" ", tableSchema.get(tableName).get(columnRef)));
                            }
                        }
                        tableSchema.get(tableName).put(constraintName, metaElements);
                        if (matchColumn.find()) {
                            foreignKeyList.add(matchColumn.group());
                        }
                    } else {
                        Matcher matcher = patternForColumn.matcher(expression);
                        List<String> metaElements = new ArrayList<>();
                        if (!matcher.find()) { // skip rowid
                            continue;
                        } else {
                            String columnRef = matcher.group();
                            if (tableSchema.get(tableName).containsKey(columnRef)) { // to avoid constant
                                metaElements.add(Strings.join(" ", tableSchema.get(tableName).get(columnRef)));
                            }
                        }
                        while (matcher.find()) {
                            String columnRef = matcher.group();
                            if (tableSchema.get(tableName).containsKey(columnRef)) { // to avoid constant
                                metaElements.add(Strings.join(" ", tableSchema.get(tableName).get(columnRef)));
                            }
                        }
                        Collections.sort(metaElements);
                        metaElements.add(0, constraintType);
                        tableSchema.get(tableName).put(constraintName, metaElements);
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

                tableSchema.get(foreignKeyList.get(i)).replace(foreignKeyList.get(i + 1), foreignExpression);
            }
        }
        return tableSchema;
    }

    @Override
    public CockroachDBProvider.CockroachDBGlobalState constructEquivalentState(CockroachDBProvider.CockroachDBGlobalState state) {
        try {
            CockroachDBEDC edc = new CockroachDBEDC(state);
            return edc.createRawDB();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String generateQueryString(CockroachDBProvider.CockroachDBGlobalState state) {
        CockroachDBSchema.CockroachDBTables tables = state.getSchema().getRandomTableNonEmptyTables();
        List<CockroachDBTableReference> tableL = tables.getTables().stream().map(CockroachDBTableReference::new).collect(Collectors.toList());
        List<CockroachDBExpression> tableList = CockroachDBCommon.getTableReferences(tableL);
        List<CockroachDBSchema.CockroachDBColumn> candidateColumns = new ArrayList<>();
        for (CockroachDBSchema.CockroachDBColumn column : tables.getColumns()) {
            if (!column.getName().equals("rowid")) { // edc does not support the hidden column rowid, which value is randomly assigned by CockroachDB
                candidateColumns.add(column);
            }
        }
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(state).setColumns(candidateColumns);
        CockroachDBExpression whereCondition = gen.generateExpression(CockroachDBSchema.CockroachDBDataType.BOOL.get());
        CockroachDBSelect select = new CockroachDBSelect();
        select.setFetchColumns((Randomly.nonEmptySubset(candidateColumns).stream().map(CockroachDBColumnReference::new).collect(Collectors.toList())));
        select.setFromList(tableList);
        select.setWhereClause(whereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            List<CockroachDBExpression> joinExpressions = getJoins(tableList, state);
            select.setJoinList(joinExpressions);
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.getOrderingTerms());
        }

        String query = CockroachDBVisitor.asString(select);
        query = query.replaceAll("@\\{FORCE_INDEX=[^\\s]*\\}", ""); // remove index reference @{FORCE_INDEX=%s}
        query = query.replaceAll("HASH", "").replaceAll("MERGE", "").replaceAll("LOOKUP", ""); //remove join hint
        return query;
    }

}
