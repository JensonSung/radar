
package sqlancer.mysql;

import sqlancer.SQLGlobalState;

import java.sql.SQLException;

public class MySQLGlobalState extends SQLGlobalState<MySQLOptions, MySQLSchema> {

    @Override
    protected MySQLSchema readSchema() throws SQLException {
        return MySQLSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return false;
    }

}
