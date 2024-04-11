package sqlancer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import sqlancer.common.log.Loggable;
import sqlancer.common.query.Query;

public final class StateLogger {

    private final File loggerFile;
    private File curFile;
    public FileWriter logFileWriter;
    public FileWriter currentFileWriter;
    private static final List<String> INITIALIZED_PROVIDER_NAMES = new ArrayList<>();
    private final boolean logEachSelect;
    private final DatabaseProvider<?, ?, ?> databaseProvider;

    private static final class AlsoWriteToConsoleFileWriter extends FileWriter {

        AlsoWriteToConsoleFileWriter(File file) throws IOException {
            super(file);
        }

        @Override
        public Writer append(CharSequence arg0) throws IOException {
            System.err.println(arg0);
            return super.append(arg0);
        }

        @Override
        public void write(String str) throws IOException {
            System.err.println(str);
            super.write(str);
        }
    }

    public StateLogger(String databaseName, DatabaseProvider<?, ?, ?> provider, MainOptions options) {
        File dir = new File(Main.LOG_DIRECTORY, provider.getDBMSName());
        if (dir.exists() && !dir.isDirectory()) {
            throw new AssertionError(dir);
        }
        ensureExistsAndIsEmpty(dir, provider);
        loggerFile = new File(dir, databaseName + ".log");
        logEachSelect = options.logEachSelect();
        if (logEachSelect) {
            curFile = new File(dir, databaseName + "-cur.log");
        }
        this.databaseProvider = provider;
    }

    private void ensureExistsAndIsEmpty(File dir, DatabaseProvider<?, ?, ?> provider) {
        if (INITIALIZED_PROVIDER_NAMES.contains(provider.getDBMSName())) {
            return;
        }
        synchronized (INITIALIZED_PROVIDER_NAMES) {
            if (!dir.exists()) {
                try {
                    Files.createDirectories(dir.toPath());
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            File[] listFiles = dir.listFiles();
            assert listFiles != null : "directory was just created, so it should exist";
            for (File file : listFiles) {
                if (!file.isDirectory()) {
                    file.delete();
                }
            }
            INITIALIZED_PROVIDER_NAMES.add(provider.getDBMSName());
        }
    }

    private FileWriter getLogFileWriter() {
        if (logFileWriter == null) {
            try {
                logFileWriter = new AlsoWriteToConsoleFileWriter(loggerFile);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        return logFileWriter;
    }
    
    public FileWriter getCurrentFileWriter() {
        if (!logEachSelect) {
            throw new UnsupportedOperationException();
        }
        if (currentFileWriter == null) {
            try {
                currentFileWriter = new FileWriter(curFile, false);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        return currentFileWriter;
    }

    public void writeCurrent(StateToReproduce state) {
        if (!logEachSelect) {
            throw new UnsupportedOperationException();
        }
        printState(getCurrentFileWriter(), state);
        try {
            currentFileWriter.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeCurrent(String input) {
        write(databaseProvider.getLoggableFactory().createLoggable(input));
    }

    public void writeCurrentNoLineBreak(String input) {
        write(databaseProvider.getLoggableFactory().createLoggableWithNoLinebreak(input));
    }

    private void write(Loggable loggable) {
        if (!logEachSelect) {
            throw new UnsupportedOperationException();
        }
        try {
            getCurrentFileWriter().write(loggable.getLogString());

            currentFileWriter.flush();
        } catch (IOException e) {
            throw new AssertionError();
        }
    }

    public void logException(Throwable reduce, StateToReproduce state) {
        Loggable stackTrace = getStackTrace(reduce);
        FileWriter logFileWriter2 = getLogFileWriter();
        try {
            logFileWriter2.write(stackTrace.getLogString());
            printState(logFileWriter2, state);
        } catch (IOException e) {
            throw new AssertionError(e);
        } finally {
            try {
                logFileWriter2.flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private Loggable getStackTrace(Throwable e1) {
        return databaseProvider.getLoggableFactory().convertStacktraceToLoggable(e1);
    }

    private void printState(FileWriter writer, StateToReproduce state) {
        StringBuilder sb = new StringBuilder();

        sb.append(databaseProvider.getLoggableFactory()
                .getInfo(state.getDatabaseName(), state.getDatabaseVersion(), state.getSeedValue()).getLogString());

        for (Query<?> s : state.getStatements()) {
            sb.append(s.getLogString());
            sb.append('\n');
        }
        try {
            writer.write(sb.toString());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

}