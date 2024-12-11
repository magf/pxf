package listeners;

import annotations.FailsWithFDW;
import annotations.SkipForFDW;
import annotations.WorksWithFDW;
import io.qameta.allure.Allure;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestResult;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Method invocation listener that skips tests that are not annotated as working with FDW when ran in FDW context.
 * It also assigns each test to particular suite in Allure.
 */
public class TestAnalyzer implements IInvokedMethodListener {

    @Override
    public void beforeInvocation(IInvokedMethod invokedMethod, ITestResult result) {
        Method method = result.getMethod().getConstructorOrMethod().getMethod();
        if (method == null) {
            return;
        }
        if (method.isAnnotationPresent(Test.class)) {
            List<String> groups = Arrays.asList(invokedMethod.getTestMethod().getGroups());
            if (groups.contains("smoke")) {
                Allure.suite("Smoke");
            } else if (groups.contains("gpdb")) {
                Allure.suite("GPDB");
            } else if (groups.contains("renadata")) {
                Allure.suite("Arenadata");
            } else {
                Allure.suite("Other");
            }
        }
        // check only @Test annotated method, not @Before.. and @After.. ones
        if (FDWUtils.useFDW && method.isAnnotationPresent(Test.class)) {
            Class<?> clazz = method.getDeclaringClass();
            // check if the method should not be skipped
            if (method.isAnnotationPresent(WorksWithFDW.class) ||
                    (clazz.isAnnotationPresent(WorksWithFDW.class) &&
                     !method.isAnnotationPresent(FailsWithFDW.class) &&
                     !method.isAnnotationPresent(SkipForFDW.class)
                    )
            ) {
                return;
            }
            // in all other cases skip the test
            throw new SkipException("The test is not supported in FDW mode");
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod iInvokedMethod, ITestResult iTestResult) {
    }
}