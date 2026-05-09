package ai.chronon.service;

import ai.chronon.online.metrics.Metrics;
import ai.chronon.online.metrics.OtelMetricsReporter;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.vertx.core.Launcher;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.micrometer.Label;
import io.vertx.micrometer.MicrometerMetricsFactory;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Custom launcher to help configure the Chronon vertx feature service
 * to handle things like setting up a otel metrics registry.
 * We use otel here to be consistent with the rest of our project (e.g. fetcher code).
 * This allows us to send Vertx webservice metrics along with fetcher related metrics to allow users
 * to debug performance issues and set alerts etc.
 */
public class ChrononServiceLauncher extends Launcher {

    private static final String VertxPrometheusPort = "ai.chronon.vertx.metrics.exporter.port";
    private static final String DefaultVertxPrometheusPort = "8906";

    private static final Logger logger = LoggerFactory.getLogger(ChrononServiceLauncher.class);

    @Override
    public void beforeStartingVertx(VertxOptions options) {
        boolean enableMetrics = Optional.ofNullable(System.getProperty(Metrics.MetricsEnabled()))
                .map(Boolean::parseBoolean)
                .orElse(false);

        if (enableMetrics) {
            initializeMetrics(options);
        }
    }

    private void initializeMetrics(VertxOptions options) {
        String serviceName = "ai.chronon";
        String configuredMetricsReader = OtelMetricsReporter.getMetricsReader();
        if (configuredMetricsReader.equalsIgnoreCase(OtelMetricsReporter.MetricsReaderHttp())) {
            configureOtlpHttpMetrics(serviceName, options);
        } else if (configuredMetricsReader.equalsIgnoreCase(OtelMetricsReporter.MetricsReaderPrometheus())) {
            // if prometheus is configured, we need to spin up a Prometheus server configured to vend out
            // Vert.x metrics when it is hit with scrape calls
            configurePrometheusMetrics(options);
        } else {
            logger.warn("Unrecognized configured metrics reader: {}. Skipping Vert.x metrics collection", configuredMetricsReader);
        }
    }

    private void configureOtlpHttpMetrics(String serviceName, VertxOptions options) {
        String exporterUrl = OtelMetricsReporter.getExporterUrl() + "/v1/metrics";
        String exportInterval = OtelMetricsReporter.getMetricsExporterInterval();

        // Configure OTLP using Micrometer's built-in registry
        OtlpConfig otlpConfig = key -> {
            switch (key) {
                case "otlp.url":
                    return exporterUrl;
                case "otlp.step":
                    return exportInterval;
                case "otlp.resourceAttributes":
                    return "service.name=" + serviceName;
                default:
                    return null;
            }
        };

        MeterRegistry registry = new OtlpMeterRegistry(otlpConfig, Clock.SYSTEM);
        MicrometerMetricsFactory metricsFactory = new MicrometerMetricsFactory(registry);

        MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
                .setEnabled(true)
                .setJvmMetricsEnabled(true)
                .setFactory(metricsFactory)
                .addLabels(Label.HTTP_METHOD, Label.HTTP_CODE, Label.HTTP_PATH);

        options.setMetricsOptions(metricsOptions);
    }

    private void configurePrometheusMetrics(VertxOptions options) {
        // Configure Prometheus using Micrometer's built-in registry
        String prometheusPort = System.getProperty(VertxPrometheusPort, DefaultVertxPrometheusPort);
        VertxPrometheusOptions promOptions =
                new VertxPrometheusOptions()
                        .setEnabled(true)
                        .setStartEmbeddedServer(true)
                        .setPublishQuantiles(true)
                        .setEmbeddedServerOptions(new HttpServerOptions().setPort(Integer.parseInt(prometheusPort)));

        MicrometerMetricsFactory metricsFactory = new MicrometerMetricsFactory(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
        MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
                .setEnabled(true)
                .setJvmMetricsEnabled(true)
                .setPrometheusOptions(promOptions)
                .setFactory(metricsFactory)
                .addLabels(Label.HTTP_METHOD, Label.HTTP_CODE, Label.HTTP_PATH);

        options.setMetricsOptions(metricsOptions);
    }

    public static void main(String[] args) {
        new ChrononServiceLauncher().dispatch(args);
    }
}
