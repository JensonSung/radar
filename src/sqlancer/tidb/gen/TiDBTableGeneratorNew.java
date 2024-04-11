package sqlancer.tidb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBOptions;
import sqlancer.tidb.TiDBProvider;
import sqlancer.tidb.TiDBSchema;
import sqlancer.tidb.visitor.TiDBVisitor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TiDBTableGeneratorNew {

    private final List<TiDBSchema.TiDBColumn> columns = new ArrayList<>();
    private final ExpectedErrors errors = new ExpectedErrors();

    public SQLQueryAdapter getQuery(TiDBProvider.TiDBGlobalState globalState) throws SQLException {
        errors.add("Information schema is changed during the execution of the statement");
        String tableName = globalState.getSchema().getFreeTableName();
        int nrColumns = Randomly.fromOptions(1, 2, 3);
        for (int i = 0; i < nrColumns; i++) {
            TiDBSchema.TiDBColumn fakeColumn = new TiDBSchema.TiDBColumn("c" + i, null, false, false);
            columns.add(fakeColumn);
        }
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(globalState).setColumns(columns);

        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");
        // generated data type and column constraints
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            TiDBSchema.TiDBCompositeDataType type = TiDBSchema.TiDBCompositeDataType.getRandom(); // TODO: replaced by getBias()
            TiDBTableGenerator.appendType(sb, type);
            sb.append(" ");
            boolean isGeneratedColumn = Randomly.getBooleanWithRatherLowProbability();
            boolean isNotNUllColumn = Randomly.getBooleanWithRatherLowProbability();
            boolean isDefaultColumn = Randomly.getBoolean() && type.getPrimitiveDataType().canHaveDefault() && !isGeneratedColumn && globalState.getDbmsSpecificOptions().getTestOracleFactory().get(0) != TiDBOptions.TiDBOracleFactory.EDC;
            if (isGeneratedColumn) {
                sb.append(" AS (");
                sb.append(TiDBVisitor.asString(gen.generateExpression()));
                sb.append(") ");
                sb.append(Randomly.fromOptions("STORED", "VIRTUAL"));
                sb.append(" ");
                errors.add("Generated column can refer only to generated columns defined prior to it");
                errors.add(
                        "'Defining a virtual generated column as primary key' is not supported for generated columns.");
                errors.add("contains a disallowed function.");
                errors.add("cannot refer to auto-increment column");
            }
            if (isNotNUllColumn) {
                sb.append("NOT NULL ");
            }
            if (isDefaultColumn) {
                sb.append("DEFAULT ");
                sb.append(TiDBVisitor.asString(gen.generateConstant(type.getPrimitiveDataType())));
                sb.append(" ");
                errors.add("Invalid default value");
                errors.add("All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead");
            }
        }
        // generate table constraints
        boolean isCheckTable = Randomly.getBooleanWithRatherLowProbability(); // only syntax compatible
        boolean isPrimaryTable = Randomly.getBooleanWithRatherLowProbability();
        boolean isUniqueTable = Randomly.getBooleanWithRatherLowProbability();
        if (isCheckTable) {
            sb.append(", CHECK (");
            sb.append(TiDBVisitor.asString(gen.generateExpression()));
            sb.append(") ");
        }
        if (isPrimaryTable) {
            List<TiDBSchema.TiDBColumn> candidates = new ArrayList<>();
            for (TiDBSchema.TiDBColumn element : Randomly.nonEmptySubset(columns)) {
                if (TiDBTableGenerator.canUseAsUnique(element.getType())) {
                    candidates.add(element);
                }
            }
            if (!candidates.isEmpty()) {
                sb.append(", PRIMARY KEY(");
                sb.append(candidates.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
                sb.append(")");
            }
        }
        if (isUniqueTable) {
            List<TiDBSchema.TiDBColumn> candidates = new ArrayList<>();
            for (TiDBSchema.TiDBColumn element : Randomly.nonEmptySubset(columns)) {
                if (TiDBTableGenerator.canUseAsUnique(element.getType())) {
                    candidates.add(element);
                }
            }
            if (!candidates.isEmpty()) {
                sb.append(", UNIQUE(");
                sb.append(candidates.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
                sb.append(")");
            }
        }
        sb.append(")");

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
