package listeners;

import annotations.FailsWithFDW;
import annotations.SkipForFDW;
import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.testng.IAnnotationTransformer;
import org.testng.annotations.ITestAnnotation;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Test annotation transformer that skips tests that are not annotated as working with FDW when ran in FDW context.
 */
public class TestAnnotationTransformer implements IAnnotationTransformer {

    @Override
    public void transform(ITestAnnotation iTestAnnotation, Class aClass, Constructor constructor, Method method) {
        if (FDWUtils.useFDW && method != null) {
            Class<?> clazz = method.getDeclaringClass();
            // check if the method should not be skipped
            if (method.isAnnotationPresent(WorksWithFDW.class) ||
                    (clazz.isAnnotationPresent(WorksWithFDW.class) &&
                            !method.isAnnotationPresent(FailsWithFDW.class) &&
                            !method.isAnnotationPresent(SkipForFDW.class)
                    )) {
                return;
            }
            // in all other cases skip the test
            iTestAnnotation.setEnabled(false);
        }
    }
}
