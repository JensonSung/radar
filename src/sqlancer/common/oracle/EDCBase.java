package sqlancer.common.oracle;

import com.beust.jcommander.Strings;
import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.StateLogger;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class EDCBase<S extends SQLGlobalState<?, ?>> implements TestOracle<S> {

    protected final S originalState;
    protected S equivalentState;
    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;
    protected String queryString;

    public EDCBase(S originalState) {
        this.originalState = originalState;
        this.con = originalState.getConnection();
        this.logger = originalState.getLogger();
        this.options = originalState.getOptions();
    }

    public void check() throws Exception {
        queryString = generateQueryString(originalState);
        logger.writeCurrent(queryString);
        List<String> optimizedResult = getOptimizedResult(originalState);
        List<String> nonOptimizedResult = getNonOptimizedResult(equivalentState);
        ComparatorHelper.assumeResultSetsAreEqual(optimizedResult, nonOptimizedResult, queryString, List.of(equivalentState.getDatabaseName()), originalState);
    }

    public abstract Map<String, Map<String, List<String>>> obtainTableSchemas(S state) throws SQLException;

    public void constructEquivalentState() {
        try {
            originalState.updateSchema();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.equivalentState = constructEquivalentState(originalState);
    }

    public abstract S constructEquivalentState(S state);

    public abstract String generateQueryString(S state);

    public List<String> getOptimizedResult(S state) throws SQLException {
        List<String> resultSet = new ArrayList<>();
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        SQLancerResultSet result = null;
        try {
            result = q.executeAndGet(state);
            if (result == null) {
                throw new IgnoreMeException(); // avoid too many false positives
            }
            ResultSetMetaData metaData = result.getRs().getMetaData();
            int columns = metaData.getColumnCount();
            while (result.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columns; i++) {
                    String resultTemp = result.getString(i);
                    if (resultTemp != null) {
                        resultTemp = resultTemp.replaceAll("[\\.]0+$", ""); // Remove the trailing zeros as many DBMS treat it as non-bugs
                    }
                    row.append(resultTemp).append(",");
                }
                resultSet.add(row.toString());
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw e;
            }
            if (e.getMessage() == null) {
                throw new AssertionError(queryString, e);
            }
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(queryString, e);
        } finally {
            if (result != null && !result.isClosed()) {
                result.close();
            }
        }

        return resultSet;
    }

    public List<String> getNonOptimizedResult(S state) throws SQLException {
        return getOptimizedResult(state);
    }

    public void closeEquStates() throws SQLException {
        if (equivalentState != null) {
            equivalentState.getConnection().close();
        }
    }

    public boolean containsNewDatabaseStructure(Set<Integer> databaseStructureSet) throws SQLException {
        Map<String, Map<String, List<String>>> tableSchemas = obtainTableSchemas(originalState);
        List<TableStructure> tableStructures = new ArrayList<>();
        boolean isRawDb = true;
        for (String tableName : tableSchemas.keySet()) {
            List<String> columns = new ArrayList<>();
            List<String> tables = new ArrayList<>();
            for (String columnRef : tableSchemas.get(tableName).keySet()) {
                List<String> metaElements = tableSchemas.get(tableName).get(columnRef);
                String metadata = Strings.join(" ", metaElements).toUpperCase();
                if (isRawDb) {
                    if (metadata.contains("NOT NULL") || metadata.contains("GENERATED") || metadata.contains("UNIQUE")
                            || metadata.contains("FOREIGN KEY") || metadata.contains("PRIMARY") ||
                            metadata.contains("CHECK") || metadata.contains("KEY") || metadata.contains("INDEX")) {
                        isRawDb = false;
                    }
                }
                if (columnRef.matches("c\\d+") || columnRef.matches("`c\\d+`")) { // column reference, like c1 or `c1`
                    columns.add(metadata);
                } else {
                    tables.add(metadata);
                }
            }
            tableStructures.add(new TableStructure(columns, tables));
        }

        if (isRawDb) {
            throw new IgnoreMeException(); // do not test raw db
        }
        DatabaseStructure databaseStructure = new DatabaseStructure(tableStructures);
        int hashcode = databaseStructure.hasCode;
        if (databaseStructureSet.contains(hashcode)) {
            return false;
        } else {
            databaseStructureSet.add(hashcode);
            return true;
        }
    }

    public static class TableStructure implements Comparable<TableStructure> {
        private List<String> columns;
        private List<String> tables;
        private int hasCode;

        public TableStructure(List<String> columns, List<String> tables) {
            this.columns = new ArrayList<>(columns);
            this.tables = new ArrayList<>(tables);
            this.hasCode = Objects.hash(this.columns, this.tables);
        }

        public int compareTo(TableStructure that) {
            if (this.hashCode() > that.hashCode()) {
                return 1;
            } else if (this.hashCode() == that.hashCode()) {
                return 0;
            } else {
                return -1;
            }
        }

        @Override
        public boolean equals(Object that) {
            return this.hashCode() == that.hashCode();
        }

        @Override
        public int hashCode() {
            return hasCode;
        }
    }

    public class DatabaseStructure {

        private List<TableStructure> tableStructures;
        private int hasCode;

        public DatabaseStructure(List<TableStructure> tableStructures) {
            this.tableStructures = new ArrayList<>(tableStructures);
            this.hasCode = Objects.hash(this.tableStructures);
        }

        @Override
        public boolean equals(Object that) {
            return this.hashCode() == that.hashCode();
        }

        @Override
        public int hashCode() {
            return hasCode;
        }
    }

}
