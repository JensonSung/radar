package sqlancer;

import sqlancer.StateToReproduce.OracleRunReproductionState;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.EDCBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.schema.AbstractSchema;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class ProviderAdapter<
        G extends GlobalState<O, ? extends AbstractSchema<G, ?>, C>,
        O extends DBMSSpecificOptions<? extends OracleFactory<G>>, C extends SQLancerDBConnection>
        implements DatabaseProvider<G, O, C> {

    private final Class<G> globalClass;
    private final Class<O> optionClass;

    public ProviderAdapter(Class<G> globalClass, Class<O> optionClass) {
        this.globalClass = globalClass;
        this.optionClass = optionClass;
    }

    @Override
    public StateToReproduce getStateToReproduce(String databaseName) {
        return new StateToReproduce(databaseName, this);
    }

    @Override
    public Class<G> getGlobalStateClass() {
        return globalClass;
    }

    @Override
    public Class<O> getOptionClass() {
        return optionClass;
    }

    @Override
    public Reproducer<G> generateAndTestDatabase(G globalState, Set<Integer> databaseStructureSet) throws Exception {
        TestOracle<G> oracle = null;
        boolean isEDCOracle = false;
        try {
            generateDatabase(globalState);
            checkViewsAreValid(globalState);
            globalState.getManager().incrementCreateDatabase();

            oracle = getTestOracle(globalState);
            int nrQueries = globalState.getOptions().getNrQueries();
            if (oracle instanceof EDCBase) {
                isEDCOracle = true;
                boolean findNew = ((EDCBase) oracle).containsNewDatabaseStructure(databaseStructureSet);
                if (!findNew) {
                    if (globalState.getOptions().isAdaptive()) {
                        return null;
                    }
                }
                ((EDCBase) oracle).constructEquivalentState();
            }
            for (int i = 0; i < nrQueries; i++) {
                try (OracleRunReproductionState localState = globalState.getState().createLocalState()) {
                    assert localState != null;
                    try {
                        oracle.check();
                        globalState.getManager().incrementSelectQueryCount();
                    } catch (IgnoreMeException e) {

                    } catch (AssertionError e) {
                        Reproducer<G> reproducer = oracle.getLastReproducer();
                        if (reproducer != null) {
                            return reproducer;
                        }
                        throw e;
                    }
                    assert localState != null;
                    localState.executedWithoutError();
                }
            }
        } finally {
            if (isEDCOracle) {
                ((EDCBase<?>) oracle).closeEquStates();
                if (!databaseStructureSet.isEmpty()) {
                    globalState.getLogger().writeCurrent(String.format("===== unique databases: %d =====", databaseStructureSet.size()));
                }
            }
            globalState.getConnection().close();
        }
        return null;
    }

    protected abstract void checkViewsAreValid(G globalState);

    protected TestOracle getTestOracle(G globalState) throws Exception {
        List<? extends OracleFactory<G>> testOracleFactory = globalState.getDbmsSpecificOptions()
                .getTestOracleFactory();
        boolean testOracleRequiresMoreThanZeroRows = testOracleFactory.stream()
                .anyMatch(p -> p.requiresAllTablesToContainRows());
        boolean userRequiresMoreThanZeroRows = globalState.getOptions().testOnlyWithMoreThanZeroRows();
        boolean checkZeroRows = testOracleRequiresMoreThanZeroRows || userRequiresMoreThanZeroRows;
        if (checkZeroRows && globalState.getSchema().containsTableWithZeroRows(globalState)) {
            throw new IgnoreMeException();
        }
        if (testOracleFactory.size() == 1) {
            return testOracleFactory.get(0).create(globalState);
        } else {
            return new CompositeTestOracle(testOracleFactory.stream().map(o -> {
                try {
                    return o.create(globalState);
                } catch (Exception e1) {
                    throw new AssertionError(e1);
                }
            }).collect(Collectors.toList()), globalState);
        }
    }

    public abstract void generateDatabase(G globalState) throws Exception;

}
