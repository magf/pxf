package org.greenplum.pxf.automation;

import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;

import java.io.IOException;

public class PxfTestUtil {

    public static String getCmdResult(PhdCluster cluster, String command) throws ShellCommandErrorException, IOException {
        cluster.runCommand(command);
        String result = cluster.getLastCmdResult();
        String[] results = result.split(System.lineSeparator());
        return results.length > 1 ? results[1].trim() : "Result is empty";
    }
}
