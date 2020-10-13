package com.hazelcast.jet.pi;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.pi4j.system.SystemInfo;

public class CpuTemperature {

    public static void main(String[] args) {
        StreamSource<Double> source = SourceBuilder.stream("cpu-temperatur", ctxt -> null)
                .<Double>fillBufferFn((IGNORED, buf) -> SystemInfo.getCpuTemperature())
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(source)
                .withIngestionTimestamps()
                .window(WindowDefinition.sliding(1000, 250))
                .aggregate(AggregateOperations.averagingDouble(d -> d))
                .writeTo(Sinks.logger());

        JetInstance jet = Jet.newJetInstance();
        jet.newJob(p).join();
    }

}
