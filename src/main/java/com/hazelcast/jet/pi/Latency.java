package com.hazelcast.jet.pi;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pi.impl.Util;
import com.hazelcast.jet.pi.impl.ValidationException;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.HdrHistogram.Histogram;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Properties;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.pi.impl.Util.ensureProp;
import static com.hazelcast.jet.pi.impl.Util.loadProps;
import static com.hazelcast.jet.pi.impl.Util.parseIntProp;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Latency {

    static final String DEFAULT_PROPERTIES_FILENAME = "latency.properties";

    static final String PROP_DISTINCT_INPUT_KEYS = "distinct-input-keys";
    static final String PROP_EVENTS_PER_SECOND = "events-per-second";
    static final String PROP_INITIAL_SOURCE_DELAY_MILLIS = "initial-source-delay-millis";

    static final String PROP_WINDOW_SIZE_MILLIS = "window-size-millis";
    static final String PROP_SLIDING_STEP_MILLIS = "sliding-step-millis";
    static final String PROP_PROCESSING_GUARANTEE = "processing-guarantee";
    static final String PROP_SNAPSHOT_INTERVAL_MILLIS = "snapshot-interval-millis";
    static final String PROP_WARMUP_SECONDS = "warmup-seconds";
    static final String PROP_MEASUREMENT_SECONDS = "measurement-seconds";
    static final String PROP_LATENCY_REPORTING_THRESHOLD_MILLIS = "latency-reporting-threshold-millis";
    static final String PROP_OUTPUT_PATH = "output-path";

    static final long WARMUP_REPORTING_INTERVAL_MS = SECONDS.toMillis(2);
    static final long MEASUREMENT_REPORTING_INTERVAL_MS = SECONDS.toMillis(10);
    static final long BENCHMARKING_DONE_REPORT_INTERVAL_MS = SECONDS.toMillis(1);
    static final String BENCHMARK_DONE_MESSAGE = "benchmarking is done";

    public static void main(String[] args) {
        Properties props = loadProps(Latency.class, DEFAULT_PROPERTIES_FILENAME);

        try {
            int distinctInputKeys = parseIntProp(props, PROP_DISTINCT_INPUT_KEYS);
            int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
            int initDelayMillis = parseIntProp(props, PROP_INITIAL_SOURCE_DELAY_MILLIS);
            int windowSize = parseIntProp(props, PROP_WINDOW_SIZE_MILLIS);
            int slideBy = parseIntProp(props, PROP_SLIDING_STEP_MILLIS);
            String pgString = ensureProp(props, PROP_PROCESSING_GUARANTEE);
            ProcessingGuarantee guarantee = ProcessingGuarantee.valueOf(pgString.toUpperCase().replace('-', '_'));
            int snapshotInterval = parseIntProp(props, PROP_SNAPSHOT_INTERVAL_MILLIS);
            int warmupSeconds = parseIntProp(props, PROP_WARMUP_SECONDS);
            int measurementSeconds = parseIntProp(props, PROP_MEASUREMENT_SECONDS);
            int latencyReportingThresholdMs = parseIntProp(props, PROP_LATENCY_REPORTING_THRESHOLD_MILLIS);
            String outputPath = ensureProp(props, PROP_OUTPUT_PATH);

            Pipeline pipeline = Pipeline.create();
            StreamSource<Long> source = TestSources.longStream(eventsPerSecond, initDelayMillis);
            StreamStage<Tuple2<Long, Long>> latencies = pipeline.readFrom(source)
                    .withNativeTimestamps(0)
                    .groupingKey(l -> Long.toString(l % distinctInputKeys))
                    .window(WindowDefinition.sliding(windowSize, slideBy))
                    .aggregate(AggregateOperations.counting())
                    .mapStateful(() -> new DetermineLatency(latencyReportingThresholdMs), DetermineLatency::map);

            long warmupTimeMillis = SECONDS.toMillis(warmupSeconds);
            long totalTimeMillis = SECONDS.toMillis(warmupSeconds + measurementSeconds);
            latencies
                    .filter(t2 -> t2.f0() < totalTimeMillis)
                    .map(t2 -> String.format("%d,%d", t2.f0(), t2.f1()))
                    .writeTo(Sinks.files(new File(outputPath, "latency-log").getPath()));
            latencies
                    .mapStateful(
                            () -> new RecordLatencyHistogram(warmupTimeMillis, totalTimeMillis),
                            RecordLatencyHistogram::map)
                    .writeTo(Sinks.files(new File(outputPath, "latency-profile").getPath()));

            JobConfig jobCfg = new JobConfig();
            jobCfg.setName("Trade Monitor Benchmark");
            jobCfg.setSnapshotIntervalMillis(snapshotInterval);
            jobCfg.setProcessingGuarantee(guarantee);

            JetInstance jet = Jet.bootstrappedInstance();
            Job job = jet.newJob(pipeline, jobCfg);
            Runtime.getRuntime().addShutdownHook(new Thread(job::cancel));
            System.out.println("Benchmarking job is now in progress, let it run until you see the message");
            System.out.println("\"" + BENCHMARK_DONE_MESSAGE + "\" in the Jet server log,");
            System.out.println("and then stop it here with Ctrl-C. The result files are on the server.");
            job.join();
        } catch (ValidationException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private static class DetermineLatency implements Serializable {
        private final long latencyReportingThresholdMs;
        private long startTimestamp;
        private long lastTimestamp;

        DetermineLatency(long latencyReportingThresholdMs) {
            this.latencyReportingThresholdMs = latencyReportingThresholdMs;
        }

        Tuple2<Long, Long> map(KeyedWindowResult<String, Long> kwr) {
            long timestamp = kwr.end();
            if (timestamp <= lastTimestamp) {
                return null;
            }
            if (lastTimestamp == 0) {
                startTimestamp = timestamp;
            }
            lastTimestamp = timestamp;

            long latency = System.currentTimeMillis() - timestamp;
            long count = kwr.result();
            if (latency == -1) { // very low latencies may be reported as negative due to clock skew
                latency = 0;
            }
            if (latency < 0) {
                throw new RuntimeException("Negative latency: " + latency);
            }
            if (latency >= latencyReportingThresholdMs) {
                System.out.format("Latency %,d ms (first seen key: %s, count %,d)%n", latency, kwr.getKey(), count);
            }
            return tuple2(timestamp - startTimestamp, latency);
        }
    }

    private static class RecordLatencyHistogram implements Serializable {
        private final long warmupTimeMillis;
        private final long totalTimeMillis;
        private Histogram histogram = new Histogram(5);

        public RecordLatencyHistogram(long warmupTimeMillis, long totalTimeMillis) {
            this.warmupTimeMillis = warmupTimeMillis;
            this.totalTimeMillis = totalTimeMillis;
        }

        String map(Tuple2<Long, Long> timestampAndLatency) {
            long timestamp = timestampAndLatency.f0();
            String timeMsg = String.format("%,d ", totalTimeMillis - timestamp);
            if (histogram == null) {
                if (timestamp % BENCHMARKING_DONE_REPORT_INTERVAL_MS == 0) {
                    System.out.format(BENCHMARK_DONE_MESSAGE + " -- %s%n", timeMsg);
                }
                return null;
            }
            if (timestamp < warmupTimeMillis) {
                if (timestamp % WARMUP_REPORTING_INTERVAL_MS == 0) {
                    System.out.format("warming up -- %s%n", timeMsg);
                }
            } else {
                if (timestamp % MEASUREMENT_REPORTING_INTERVAL_MS == 0) {
                    System.out.println(timeMsg);
                }
                histogram.recordValue(timestampAndLatency.f1());
            }
            if (timestamp >= totalTimeMillis) {
                try {
                    return exportHistogram(histogram);
                } finally {
                    histogram = null;
                }
            }
            return null;
        }

        private static String exportHistogram(Histogram histogram) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            PrintStream out = new PrintStream(bos);
            histogram.outputPercentileDistribution(out, 1.0);
            out.close();
            return bos.toString();
        }
    }

}
