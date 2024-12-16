package org.greenplum.pxf.automation.components.regress;

import io.qameta.allure.Allure;
import io.qameta.allure.Step;
import org.apache.commons.io.FileUtils;
import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Utility class for running pxf_regress
 */
public class Regress extends ShellSystemObject {
    private String regressTestFolder;
    private String regressRunner;
    private String dbName;

    @Override
    public void init() throws Exception {
        ReportUtils.startLevel(report, getClass(), "init");
        regressRunner = new File("pxf_regress/pxf_regress").getAbsolutePath();
        super.init();
        runCommand("source $GPHOME/greenplum_path.sh");
        runCommand("cd " + new File(regressTestFolder).getAbsolutePath());
        ReportUtils.stopLevel(report);
    }

    /**
     * Run the SQL test queries in the given path with pxf_regress
     *
     * @param sqlTestPath path to directory that contains SQL test queries to run
     * @throws IOException if I/O error occurs
     * @throws ShellCommandErrorException if shell command fails
     */
    @Step("Run SQL test")
    public void runSqlTest(final String sqlTestPath) throws IOException, ShellCommandErrorException {
        ReportUtils.startLevel(report, getClass(), "Run test: " + sqlTestPath);
        ReportUtils.report(report, getClass(), "test path: " + sqlTestPath);
        attachFilesToReport(sqlTestPath, "sql");
        attachFilesToReport(sqlTestPath, "expected");

        setCommandTimeout(_10_MINUTES);
        StringJoiner commandToRun = new StringJoiner(" ");

        commandToRun.add("PGDATABASE=" + dbName);
        commandToRun.add(regressRunner);
        commandToRun.add(sqlTestPath);

        ReportUtils.report(report, getClass(), "running command \"" + commandToRun + "\"");

        try {
            runCommand(commandToRun.toString());
        } catch (ShellCommandErrorException e) {
            String fullPath = String.format("./sqlrepo/%s/regression.diffs", sqlTestPath);
            Allure.attachment(e.getMessage(), FileUtils.readFileToString(new File(fullPath)));
            throw e;
        }
        ReportUtils.stopLevel(report);
    }

    public void setRegressTestFolder(String regressTestFolder) {
        this.regressTestFolder = regressTestFolder;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    private void attachFilesToReport(String path, String folder) {
        String basePath = "./sqlrepo/";
        String fullPath = String.format("%s%s/%s/", basePath, path, folder);
        Arrays.stream(Objects.requireNonNull(new File(fullPath).listFiles())).forEach(file -> {
            try {
                String msg = String.format("%s %s", folder.toUpperCase(), file.getName());
                Allure.attachment(msg, FileUtils.readFileToString(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
