package sqlancer.mariadb.oracle;

import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.ast.MariaDBColumnName;
import sqlancer.mariadb.ast.MariaDBExpression;
import sqlancer.mariadb.ast.MariaDBSelectStatement;
import sqlancer.mariadb.gen.MariaDBExpressionGenerator;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public abstract class MariaDBTLPBase
        extends TernaryLogicPartitioningOracleBase<MariaDBExpression, MariaDBGlobalState>
        implements TestOracle<MariaDBGlobalState> {

    MariaDBExpressionGenerator gen;
    MariaDBSchema s;
    MariaDBSchema.MariaDBTables targetTables;
    MariaDBSelectStatement select;

    protected MariaDBTLPBase(MariaDBGlobalState state) {
        super(state);
    }

    @Override
    protected ExpressionGenerator<MariaDBExpression> getGen() {
        return gen;
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new MariaDBExpressionGenerator(state.getRandomly()).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new MariaDBSelectStatement();
        select.setFetchColumns(Arrays.asList(new MariaDBColumnName(targetTables.getColumns().get(0))));
        List<MariaDBTable> tables = targetTables.getTables();
        select.setFromTables(tables);
        select.setWhereClause(null);
    }
}
