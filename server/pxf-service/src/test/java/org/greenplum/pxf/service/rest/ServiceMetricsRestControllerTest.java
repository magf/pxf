package org.greenplum.pxf.service.rest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.metrics.MetricsEndpoint;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ServiceMetricsRestControllerTest {
    private static final String clusterName = "cluster name";
    private static final String hostName = "host name";
    private final MetricsEndpoint metricsEndpoint = mock(MetricsEndpoint.class);
    private final ServiceMetricsRestController controller = new ServiceMetricsRestController(metricsEndpoint, clusterName, hostName);

    @Test
    public void get() {
        MetricsEndpoint.MetricResponse metricResponse = mock(MetricsEndpoint.MetricResponse.class);
        when(metricsEndpoint.metric(anyString(), any())).thenReturn(metricResponse).thenReturn(null);
        Collection<MetricsEndpoint.MetricResponse> result = controller.get();
        assertEquals(1, result.size());
        result.stream().findAny().filter(metricResponse::equals).orElseGet(Assertions::fail);
    }

    @Test
    public void getClusterMetrics() {
        MetricsEndpoint.MetricResponse metricResponse = mock(MetricsEndpoint.MetricResponse.class);
        when(metricsEndpoint.metric(anyString(), any())).thenReturn(metricResponse).thenReturn(null);
        ServiceMetricsRestController.ClusterMetrics result = controller.getClusterMetrics();
        assertEquals(clusterName, result.getCluster());
        assertEquals(hostName, result.getHostname());
        assertEquals(1, result.getMetrics().size());
        result.getMetrics().stream().findAny().filter(metricResponse::equals).orElseGet(Assertions::fail);
    }
}