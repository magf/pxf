package org.greenplum.pxf.service.rest;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.metrics.MetricsEndpoint;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;


@RestController
@RequestMapping("/service-metrics")
public class ServiceMetricsRestController {
    private static final Collection<String> METRIC_NAMES = Arrays.asList(
            "http.server.requests",
            "jvm.buffer.count",
            "jvm.buffer.memory.used",
            "jvm.buffer.total.capacity",
            "jvm.memory.committed",
            "jvm.memory.max",
            "jvm.memory.used",
            "pxf.bytes.received",
            "pxf.bytes.sent",
            "pxf.fragments.sent",
            "pxf.records.received",
            "pxf.records.sent"
    );
    private static final Collection<String> CLUSTER_METRIC_NAMES = Arrays.asList(
            "jvm.memory.committed",
            "jvm.memory.max",
            "jvm.memory.used",
            "pxf.bytes.received",
            "pxf.bytes.sent",
            "pxf.records.received",
            "pxf.records.sent"
    );
    private final MetricsEndpoint metricsEndpoint;
    private final String clusterName;
    private final String hostName;

    public ServiceMetricsRestController(final MetricsEndpoint metricsEndpoint,
                                        @Value("${cluster-name}") final String clusterName,
                                        @Value("${eureka.instance.hostname}") final String hostName) {
        this.metricsEndpoint = metricsEndpoint;
        this.clusterName = clusterName;
        this.hostName = hostName;
    }

    @GetMapping
    public Collection<MetricsEndpoint.MetricResponse> get() {
        return METRIC_NAMES.stream()
                .map(name -> metricsEndpoint.metric(name, Collections.emptyList()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @GetMapping("/cluster-metrics")
    public ClusterMetrics getClusterMetrics() {
        return new ClusterMetrics(
                clusterName,
                hostName,
                CLUSTER_METRIC_NAMES.stream()
                        .map(name -> metricsEndpoint.metric(name, Collections.emptyList()))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
    }


    @Getter
    @RequiredArgsConstructor
    public static class ClusterMetrics {
        private final String cluster;
        private final String hostname;
        private final Collection<MetricsEndpoint.MetricResponse> metrics;
    }
}
