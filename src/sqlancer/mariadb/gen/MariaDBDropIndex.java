package sqlancer.mariadb.gen;

import sqlancer.IgnoreMeException;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.mariadb.MariaDBSchema;


public final class MariaDBDropIndex {
    private MariaDBDropIndex() {
    }

    public static SQLQueryAdapter generate(MariaDBProvider.MariaDBGlobalState globalState) {
        MariaDBSchema.MariaDBTable table = globalState.getSchema().getRandomTable();
        if (!table.hasIndexes()) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("DROP INDEX ");
        sb.append(table.getRandomIndex().getIndexName());
        sb.append(" ON ");
        sb.append(table.getName());
        return new SQLQueryAdapter(sb.toString(), new ExpectedErrors());
    }
}
