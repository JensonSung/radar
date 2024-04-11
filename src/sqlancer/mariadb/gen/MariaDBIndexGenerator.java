package sqlancer.mariadb.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBOptions;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;

import java.util.List;

public final class MariaDBIndexGenerator {

    private MariaDBIndexGenerator() {
    }

    public static SQLQueryAdapter generate(MariaDBProvider.MariaDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("CREATE ");
        errors.add("Key/Index cannot be defined on a virtual generated column");
        errors.add("Specified key was too long");
        if (Randomly.getBoolean()) {
            errors.add("Duplicate entry");
            errors.add("Key/Index cannot be defined on a virtual generated column");
            sb.append("UNIQUE ");
        }
        errors.add("Duplicate key name");
        sb.append("INDEX ");
        sb.append("i");
        sb.append(DBMSCommon.createColumnName(Randomly.smallNumber()));
        if (Randomly.getBoolean()) {
            sb.append(" USING ");
            sb.append(Randomly.fromOptions("BTREE", "HASH")); // , "RTREE")
        }

        sb.append(" ON ");
        MariaDBTable randomTable = globalState.getSchema().getRandomTable();
        sb.append(randomTable.getName());
        sb.append("(");
        List<MariaDBColumn> columns = Randomly.nonEmptySubset(randomTable.getColumns());
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().getTestOracleFactory().get(0) != MariaDBOptions.MariaDBOracleFactory.EDC) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
        }
        sb.append(")");
        // if (Randomly.getBoolean()) {
        // sb.append(" ALGORITHM=");
        // sb.append(Randomly.fromOptions("DEFAULT", "INPLACE", "COPY", "NOCOPY", "INSTANT"));
        // errors.add("is not supported for this operation");
        // }

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
