package sqlancer.tidb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.gen.TiDBAlterTableGenerator;
import sqlancer.tidb.gen.TiDBAnalyzeTableGenerator;
import sqlancer.tidb.gen.TiDBDeleteGenerator;
import sqlancer.tidb.gen.TiDBIndexGenerator;
import sqlancer.tidb.gen.TiDBInsertGenerator;
import sqlancer.tidb.gen.TiDBRandomQuerySynthesizer;
import sqlancer.tidb.gen.TiDBSetGenerator;
import sqlancer.tidb.gen.TiDBTableGenerator;
import sqlancer.tidb.gen.TiDBTableGeneratorNew;
import sqlancer.tidb.gen.TiDBUpdateGenerator;
import sqlancer.tidb.gen.TiDBViewGenerator;

@AutoService(DatabaseProvider.class)
public class TiDBProvider extends SQLProviderAdapter<TiDBGlobalState, TiDBOptions> {

    public TiDBProvider() {
        super(TiDBGlobalState.class, TiDBOptions.class);
    }

    public enum Action implements AbstractAction<TiDBGlobalState> {
        INSERT(TiDBInsertGenerator::getQuery), //
        ANALYZE_TABLE(TiDBAnalyzeTableGenerator::getQuery), //
        TRUNCATE((g) -> new SQLQueryAdapter("TRUNCATE " + g.getSchema().getRandomTable(t -> !t.isView()).getName())), //
        CREATE_INDEX(TiDBIndexGenerator::getQuery), //
        DELETE(TiDBDeleteGenerator::getQuery), //
        SET(TiDBSetGenerator::getQuery), // TODO: eliminate error report
        UPDATE(TiDBUpdateGenerator::getQuery), //
        ADMIN_CHECKSUM_TABLE(
                (g) -> new SQLQueryAdapter("ADMIN CHECKSUM TABLE " + g.getSchema().getRandomTable().getName())), // TODO: eliminate error report
        VIEW_GENERATOR(TiDBViewGenerator::getQuery), //
        ALTER_TABLE(TiDBAlterTableGenerator::getQuery), //
        EXPLAIN((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            TiDBErrors.addExpressionErrors(errors);
            TiDBErrors.addExpressionHavingErrors(errors);
            return new SQLQueryAdapter(
                    "EXPLAIN " + TiDBRandomQuerySynthesizer.generate(g, Randomly.smallNumber() + 1).getQueryString(),
                    errors);
        });

        private final SQLQueryProvider<TiDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<TiDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(TiDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    public static class TiDBGlobalState extends SQLGlobalState<TiDBOptions, TiDBSchema> {

        @Override
        protected TiDBSchema readSchema() throws SQLException {
            return TiDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

        public SQLConnection createConnection() throws SQLException {
            String host = getOptions().getHost();
            int port = getOptions().getPort();
            if (host == null) {
                host = TiDBOptions.DEFAULT_HOST;
            }
            if (port == MainOptions.NO_SET_PORT) {
                port = TiDBOptions.DEFAULT_PORT;
            }
            String databaseName = getDatabaseName();
            String url = String.format("jdbc:mysql://%s:%d/%s", host, port, databaseName);
            Connection con = DriverManager.getConnection(url, getOptions().getUserName(),
                    getOptions().getPassword());
            return new SQLConnection(con);
        }

        public boolean usesTxInfer() {
            return getDbmsSpecificOptions().oracle.stream().anyMatch(o -> o == TiDBOptions.TiDBOracleFactory.TX_INFER);
        }

    }

    private static int mapActions(TiDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case ANALYZE_TABLE:
        case CREATE_INDEX:
            return r.getInteger(0, 2);
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case EXPLAIN:
            return 0;
        case TRUNCATE:
        case DELETE:
        case ADMIN_CHECKSUM_TABLE:   //  TODO: eliminate error report
            return 0;
        case SET:   //  TODO: eliminate error report
        case UPDATE:
            return r.getInteger(0, 5);
        case VIEW_GENERATOR:
            // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/8
            return r.getInteger(0, 0);
        case ALTER_TABLE:
            return r.getInteger(0, 10); // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/10
        default:
            throw new AssertionError(a);
        }

    }

    @Override
    public void generateDatabase(TiDBGlobalState globalState) throws Exception {
        int tableNum = Randomly.fromOptions(1, 2, 3);
        if (globalState.usesTxInfer()) {
            tableNum = 1;
        }
        for (int i = 0; i < tableNum; i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new TiDBTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }

        StatementExecutor<TiDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                TiDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        try {
            se.executeStatements();
        } catch (SQLException e) {
            if (e.getMessage().contains(
                    "references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")) {
                throw new IgnoreMeException(); // TODO: drop view instead
            } else {
                throw new AssertionError(e);
            }
        }
    }

    @Override
    public SQLConnection createDatabase(TiDBGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = TiDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = TiDBOptions.DEFAULT_PORT;
        }

        String databaseName = globalState.getDatabaseName();
        String url = String.format("jdbc:mysql://%s:%d/", host, port);
        Connection con = null;
        try {
            con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                    globalState.getOptions().getPassword());
        } catch (SQLException e) {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Cannot build a connection, try to restart the database.");
            System.out.println("Press Y to continue connection.");
            String input = scanner.nextLine();
            if (input.equals("Y")) {
                con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                        globalState.getOptions().getPassword());
            }
            if (con != null) {
                System.out.println("Good. Here we go.");
            }
        }

        globalState.getState().logStatement("USE test");
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        String createDatabaseCommand = "CREATE DATABASE " + databaseName;
        globalState.getState().logStatement(createDatabaseCommand);
        globalState.getState().logStatement("USE " + databaseName);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute(createDatabaseCommand);
        }
        con.close();
        con = DriverManager.getConnection(url + databaseName, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "tidb";
    }
}
