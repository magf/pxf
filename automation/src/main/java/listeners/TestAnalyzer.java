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

            Allure.getLifecycle().updateTestCase(allureResult -> {
                List<Parameter> parameters = new ArrayList<>();
                Parameter tableTypeParameter = new Parameter().setName("tableType").setValue(feature);
                parameters.add(tableTypeParameter);
                String dataProvider = method.getAnnotation(Test.class).dataProvider();
                if (dataProvider != null && !dataProvider.isEmpty()) {
                    List<Parameter> dataProviderParameters = getDataProviderParameters(method.getParameters(), result.getParameters());
                    parameters.addAll(dataProviderParameters);
                }
                allureResult.setParameters(parameters);
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

    private List<Parameter> getDataProviderParameters(java.lang.reflect.Parameter[] parametersNames, Object[] parametersValues) {
        List<Parameter> parameters = new ArrayList<>();
        for (int i = 0; i < parametersNames.length; i++) {
           parameters.add(new Parameter().setName(parametersNames[i].getName()).setValue(extractParameterStringValue(parametersValues[i])));
        }
        return parameters;
    }

    private String extractParameterStringValue(Object param) {
        if (param == null) {
            return "null";
        } else if (param.getClass().isArray()) {
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < Array.getLength(param); i++) {
                list.add(Array.get(param, i));
            }
            return list.toString();
        } else {
            return param.toString();
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod iInvokedMethod, ITestResult iTestResult) {
    }
}