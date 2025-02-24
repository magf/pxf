package listeners;

import io.qameta.allure.Allure;
import io.qameta.allure.model.Parameter;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestResult;
import org.testng.annotations.Test;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
            String feature = FDWUtils.useFDW ? "FDW" : "External Table";
            String featureId = FDWUtils.useFDW ? "fdw" : "external-table";

            Allure.getLifecycle().updateTestCase(allureResult -> {
                List<Parameter> parameters = new ArrayList<>();
                Parameter tableTypeParameter = new Parameter().setName("tableType").setValue(feature);
                parameters.add(tableTypeParameter);
                String dataProvider = method.getAnnotation(Test.class).dataProvider();
                if (dataProvider != null && !dataProvider.isEmpty()) {
                    Parameter idParameter = new Parameter().setName("parameters")
                            .setValue(Arrays.stream(result.getParameters())
                                    .map(param -> {
                                        if (param.getClass().isArray()) {
                                            List<Object> arrParamList = new ArrayList<>();
                                            for (int i = 0; i < Array.getLength(param); i++) {
                                                arrParamList.add(Array.get(param, i));
                                            }
                                            return arrParamList.toString();
                                        } else {
                                            return param.toString();
                                        }
                                    }).collect(Collectors.joining(", ")));
                    parameters.add(idParameter);
                }
                allureResult.setParameters(parameters);
                allureResult.setHistoryId(String.format("%s-%s", allureResult.getHistoryId(), featureId));
            });
            List<String> groups = Arrays.asList(invokedMethod.getTestMethod().getGroups());
            if (groups.contains("smoke")) {
                Allure.suite("Smoke: " + feature);
            } else if (groups.contains("gpdb")) {
                Allure.suite("GPDB: " + feature);
            } else if (groups.contains("arenadata")) {
                Allure.suite("Arenadata: " + feature);
            } else {
                Allure.suite("Other: " + feature);
            }
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod iInvokedMethod, ITestResult iTestResult) {
    }
}