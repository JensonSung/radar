package sqlancer.tidb;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.tidb.TiDBOptions.TiDBOracleFactory;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.oracle.TiDBEDCOracle;
import sqlancer.tidb.oracle.transaction.TiDBTxInferOracle;

@Parameters(separators = "=", commandDescription = "TiDB (default port: " + TiDBOptions.DEFAULT_PORT
        + ", default host: " + TiDBOptions.DEFAULT_HOST + ")")
public class TiDBOptions implements DBMSSpecificOptions<TiDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 4000;

    @Parameter(names = "--oracle")
    public List<TiDBOracleFactory> oracle = Arrays.asList(TiDBOracleFactory.EDC);

    @Parameter(names = "--use-fixed-num-transaction", description = "Specifies whether the fixed number of transactions is generated", arity = 1)
    private boolean useFixedNumTransaction = false;

    @Parameter(names = {"--num-transaction"}, description = "Specifies the number of transactions to be generated for a database")
    private int nrTransactions = 2;

    @Parameter(names = {"--num-schedule"}, description = "Specifies the number of schedules to be generated for a group of transactions")
    private int nrSchedules = 10;

    public enum TiDBOracleFactory implements OracleFactory<TiDBGlobalState> {
        EDC {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBEDCOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        },
        TX_INFER {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBTxInferOracle(globalState);
            }
        }

    }

    @Override
    public List<TiDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }

    public boolean useFixedNumTransaction() {
        return useFixedNumTransaction;
    }

    public int getNrTransactions() {
        return nrTransactions;
    }

    public int getNrSchedules() {
        return nrSchedules;
    }
}
