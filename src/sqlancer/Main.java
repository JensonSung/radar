package sqlancer;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.JCommander.Builder;

import sqlancer.common.query.Query;
import sqlancer.common.query.SQLancerResultSet;

public final class Main {

    public static final File LOG_DIRECTORY = new File("logs");
    public static volatile AtomicLong nrQueries = new AtomicLong();
    public static volatile AtomicLong nrDatabases = new AtomicLong();
    public static volatile AtomicLong nrSuccessfulActions = new AtomicLong();
    public static volatile AtomicLong nrUnsuccessfulActions = new AtomicLong();
    public static volatile AtomicLong threadsShutdown = new AtomicLong();
    static boolean progressMonitorStarted;

    static {
        System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "ERROR");
        if (!LOG_DIRECTORY.exists()) {
            LOG_DIRECTORY.mkdir();
        }
    }

    private Main() {
    }

    public static class QueryManager<C extends SQLancerDBConnection> {

        private final GlobalState<?, ?, C> globalState;

        QueryManager(GlobalState<?, ?, C> globalState) {
            this.globalState = globalState;
        }

        public boolean execute(Query<C> q, String... fills) throws Exception {
            boolean success;
            success = q.execute(globalState, fills);
            Main.nrSuccessfulActions.addAndGet(1);
            if (globalState.getOptions().loggerPrintFailed() || success) {
                globalState.getState().logStatement(q);
            }
            return success;
        }

        public SQLancerResultSet executeAndGet(Query<C> q, String... fills) throws Exception {
            globalState.getState().logStatement(q);
            SQLancerResultSet result;
            result = q.executeAndGet(globalState, fills);
            Main.nrSuccessfulActions.addAndGet(1);
            return result;
        }

        public void incrementSelectQueryCount() {
            Main.nrQueries.addAndGet(1);
        }

        public void incrementCreateDatabase() {
            Main.nrDatabases.addAndGet(1);
        }

    }

    public static void main(String[] args) {
        System.exit(executeMain(args));
    }

    public static class DBMSExecutor<
            G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> {

        private final DatabaseProvider<G, O, C> provider;
        private final MainOptions options;
        private final O command;
        private final String databaseName;
        private StateLogger logger;
        private StateToReproduce stateToRepro;
        private final Randomly random;

        protected Set<Integer> databaseStructureSet;

        public DBMSExecutor(DatabaseProvider<G, O, C> provider, MainOptions options, O dbmsSpecificOptions,
                            String databaseName, Randomly r) {
            this.provider = provider;
            this.options = options;
            this.databaseName = databaseName;
            this.command = dbmsSpecificOptions;
            this.random = r;
        }

        public void setDatabaseStructureSet(Set<Integer> databaseStructureSet) {
            this.databaseStructureSet = databaseStructureSet;
        }

        private G createGlobalState() {
            try {
                return provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        public void testConnection() throws Exception {
            G state = getInitializedGlobalState(options.getRandomSeed());
            try (SQLancerDBConnection con = provider.createDatabase(state)) {
                return;
            }
        }

        public void run() throws Exception {
            G state = createGlobalState();
            stateToRepro = provider.getStateToReproduce(databaseName);
            stateToRepro.seedValue = random.getSeed();
            state.setState(stateToRepro);
            logger = new StateLogger(databaseName, provider, options);
            state.setRandomly(random);
            state.setDatabaseName(databaseName);
            state.setMainOptions(options);
            state.setDbmsSpecificOptions(command);
            try (C con = provider.createDatabase(state)) {
                QueryManager<C> manager = new QueryManager<>(state);
                try {
                    stateToRepro.databaseVersion = con.getDatabaseVersion();
                } catch (Exception e) {
                    // ignore
                }
                state.setConnection(con);
                state.setStateLogger(logger);
                state.setManager(manager);
                if (options.logEachSelect()) {
                    logger.writeCurrent(state.getState());
                }
                Reproducer<G> reproducer = provider.generateAndTestDatabase(state, databaseStructureSet);
                try {
                    logger.getCurrentFileWriter().close();
                    logger.currentFileWriter = null;
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
                if (reproducer != null && options.useReducer()) {
                    System.out.println("EXPERIMENTAL: Trying to reduce queries using a simple reducer.");
                    System.out.println("Reduced query will be output to stdout but not logs.");
                    G newGlobalState = createGlobalState();
                    newGlobalState.setState(stateToRepro);
                    newGlobalState.setRandomly(random);
                    newGlobalState.setDatabaseName(databaseName);
                    newGlobalState.setMainOptions(options);
                    newGlobalState.setDbmsSpecificOptions(command);
                    QueryManager<C> newManager = new QueryManager<>(newGlobalState);
                    newGlobalState.setStateLogger(new StateLogger(databaseName, provider, options));
                    newGlobalState.setManager(newManager);

                    Reducer<G> reducer = new StatementReducer<>(provider);
                    reducer.reduce(state, reproducer, newGlobalState);
                    throw new AssertionError("Found a potential bug");
                }
            }
        }

        private G getInitializedGlobalState(long seed) {
            G state = createGlobalState();
            stateToRepro = provider.getStateToReproduce(databaseName);
            stateToRepro.seedValue = seed;
            state.setState(stateToRepro);
            logger = new StateLogger(databaseName, provider, options);
            Randomly r = new Randomly(seed);
            state.setRandomly(r);
            state.setDatabaseName(databaseName);
            state.setMainOptions(options);
            state.setDbmsSpecificOptions(command);
            return state;
        }

        public StateLogger getLogger() {
            return logger;
        }

        public StateToReproduce getStateToReproduce() {
            return stateToRepro;
        }
    }

    public static class DBMSExecutorFactory<
            G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> {

        private final DatabaseProvider<G, O, C> provider;
        private final MainOptions options;
        private final O command;

        public DBMSExecutorFactory(DatabaseProvider<G, O, C> provider, MainOptions options) {
            this.provider = provider;
            this.options = options;
            this.command = createCommand();
        }

        private O createCommand() {
            try {
                return provider.getOptionClass().getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        public O getCommand() {
            return command;
        }

        @SuppressWarnings("unchecked")
        public DBMSExecutor<G, O, C> getDBMSExecutor(String databaseName, Randomly r) {
            try {
                return new DBMSExecutor<G, O, C>(provider.getClass().getDeclaredConstructor().newInstance(), options,
                        command, databaseName, r);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        public DatabaseProvider<G, O, C> getProvider() {
            return provider;
        }

    }

    public static int executeMain(String... args) throws AssertionError {
        List<DatabaseProvider<?, ?, ?>> providers = getDBMSProviders();
        Map<String, DBMSExecutorFactory<?, ?, ?>> nameToProvider = new HashMap<>();
        MainOptions options = new MainOptions();
        Builder commandBuilder = JCommander.newBuilder().addObject(options);
        for (DatabaseProvider<?, ?, ?> provider : providers) {
            String name = provider.getDBMSName();
            DBMSExecutorFactory<?, ?, ?> executorFactory = new DBMSExecutorFactory<>(provider, options);
            commandBuilder = commandBuilder.addCommand(name, executorFactory.getCommand());
            nameToProvider.put(name, executorFactory);
        }
        JCommander jc = commandBuilder.programName("SQLancer").build();
        jc.parse(args);

        if (jc.getParsedCommand() == null || options.isHelp()) {
            jc.usage();
            return options.getErrorExitCode();
        }

        Randomly.initialize(options);
        if (options.printProgressInformation()) {
            startProgressMonitor();
            if (options.printProgressSummary()) {
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

                    @Override
                    public void run() {
                        System.out.println("Overall execution statistics");
                        System.out.println("============================");
                        System.out.println(formatInteger(nrQueries.get()) + " queries");
                        System.out.println(formatInteger(nrDatabases.get()) + " databases");
                        System.out.println(
                                formatInteger(nrSuccessfulActions.get()) + " successfully-executed statements");
                        System.out.println(
                                formatInteger(nrUnsuccessfulActions.get()) + " unsuccessfuly-executed statements");
                    }

                    private String formatInteger(long intValue) {
                        if (intValue > 1000) {
                            return String.format("%,9dk", intValue / 1000);
                        } else {
                            return String.format("%,10d", intValue);
                        }
                    }
                }));
            }
        }

        ExecutorService execService = Executors.newFixedThreadPool(options.getNumberConcurrentThreads());
        DBMSExecutorFactory<?, ?, ?> executorFactory = nameToProvider.get(jc.getParsedCommand());

        if (options.performConnectionTest()) {
            try {
                executorFactory.getDBMSExecutor(options.getDatabasePrefix() + "connectiontest", new Randomly())
                        .testConnection();
            } catch (Exception e) {
                System.err.println(
                        "SQLancer failed creating a test database, indicating that SQLancer might have failed connecting to the DBMS. In order to change the username, password, host and port, you can use the --username, --password, --host and --port options.\n\n");
                e.printStackTrace();
                return options.getErrorExitCode();
            }
        }
        final AtomicBoolean someOneFails = new AtomicBoolean(false);
        Set<Integer> dbHashset = new HashSet<>();

        for (int i = 0; i < options.getTotalNumberTries(); i++) {
            final String databaseName = options.getDatabasePrefix() + i;
            final long seed;
            if (options.getRandomSeed() == -1) {
                seed = System.currentTimeMillis() + i;
            } else {
                seed = options.getRandomSeed() + i;
            }
            execService.execute(new Runnable() {

                @Override
                public void run() {
                    Thread.currentThread().setName(databaseName);
                    runThread(databaseName);
                }

                private void runThread(final String databaseName) {
                    Randomly r = new Randomly(seed);
                    try {
                        int maxNrDbs = options.getMaxGeneratedDatabases();
                        // run without a limit if maxNrDbs == -1
                        for (int i = 0; i < maxNrDbs || maxNrDbs == -1; i++) {
                            Boolean continueRunning = run(options, execService, executorFactory, r, databaseName, dbHashset);
                            if (!continueRunning) {
                                someOneFails.set(true);
                                break;
                            }
                        }
                    } finally {
                        threadsShutdown.addAndGet(1);
                        if (threadsShutdown.get() == options.getTotalNumberTries()) {
                            execService.shutdown();
                        }
                    }
                }

                private boolean run(MainOptions options, ExecutorService execService,
                                    DBMSExecutorFactory<?, ?, ?> executorFactory, Randomly r, final String databaseName,
                                    Set<Integer> databaseStructureSet) {
                    DBMSExecutor<?, ?, ?> executor = executorFactory.getDBMSExecutor(databaseName, r);
                    executor.setDatabaseStructureSet(databaseStructureSet);
                    try {
                        executor.run();
                        return true;
                    } catch (IgnoreMeException e) {
                        return true;
                    } catch (Throwable reduce) {
                        reduce.printStackTrace();
                        executor.getStateToReproduce().exception = reduce.getMessage();
                        executor.getLogger().logFileWriter = null;
                        executor.getLogger().logException(reduce, executor.getStateToReproduce());
                        return false;
                    } finally {
                        try {
                            if (options.logEachSelect()) {
                                if (executor.getLogger().currentFileWriter != null) {
                                    executor.getLogger().currentFileWriter.close();
                                }
                                executor.getLogger().currentFileWriter = null;
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

            });
        }
        try {
            if (options.getTimeoutSeconds() == -1) {
                execService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            } else {
                execService.awaitTermination(options.getTimeoutSeconds(), TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return someOneFails.get() ? options.getErrorExitCode() : 0;
    }

    /**
     * To register a new provider, it is necessary to implement the DatabaseProvider interface and add an additional
     * configuration file, see https://docs.oracle.com/javase/9/docs/api/java/util/ServiceLoader.html. Currently, we use
     * an @AutoService annotation to create the configuration file automatically. This allows SQLancer to pick up
     * providers in other JARs on the classpath.
     *
     * @return The list of service providers on the classpath
     */
    static List<DatabaseProvider<?, ?, ?>> getDBMSProviders() {
        List<DatabaseProvider<?, ?, ?>> providers = new ArrayList<>();
        @SuppressWarnings("rawtypes")
        ServiceLoader<DatabaseProvider> loader = ServiceLoader.load(DatabaseProvider.class);
        for (DatabaseProvider<?, ?, ?> provider : loader) {
            providers.add(provider);
        }
        return providers;
    }

    private static synchronized void startProgressMonitor() {
        if (progressMonitorStarted) {
            /*
             * it might be already started if, for example, the main method is called multiple times in a test (see
             * https://github.com/sqlancer/sqlancer/issues/90).
             */
            return;
        } else {
            progressMonitorStarted = true;
        }
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new Runnable() {

            private long timeMillis = System.currentTimeMillis();
            private long lastNrQueries;
            private long lastNrDbs;

            {
                timeMillis = System.currentTimeMillis();
            }

            @Override
            public void run() {
                long elapsedTimeMillis = System.currentTimeMillis() - timeMillis;
                long currentNrQueries = nrQueries.get();
                long nrCurrentQueries = currentNrQueries - lastNrQueries;
                double throughput = nrCurrentQueries / (elapsedTimeMillis / 1000d);
                long currentNrDbs = nrDatabases.get();
                long nrCurrentDbs = currentNrDbs - lastNrDbs;
                double throughputDbs = nrCurrentDbs / (elapsedTimeMillis / 1000d);
                long successfulStatementsRatio = (long) (100.0 * nrSuccessfulActions.get()
                        / (nrSuccessfulActions.get() + nrUnsuccessfulActions.get()));
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                System.out.println(String.format(
                        "[%s] Executed %d queries (%d queries/s; %.2f/s dbs, successful statements: %2d%%). Threads shut down: %d.",
                        dateFormat.format(date), currentNrQueries, (int) throughput, throughputDbs,
                        successfulStatementsRatio, threadsShutdown.get()));
                timeMillis = System.currentTimeMillis();
                lastNrQueries = currentNrQueries;
                lastNrDbs = currentNrDbs;
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

}
