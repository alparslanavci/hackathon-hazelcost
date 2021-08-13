import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.map.IMap;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;

public class Main2 {
    public static void main(String[] args)
            throws InterruptedException {
        new Main2().start();
    }

    private void start()
            throws InterruptedException {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
        JetService jet = hazelcastInstance.getJet();
        String requestsMapName = "requests";
        IMap<String, HazelcostRequest> requests = hazelcastInstance.getMap(requestsMapName);

        Random random = new Random();

        for (int i = 0; i < 200; i++) {
            requests.set("cpu", new HazelcostRequest("cpu", System.currentTimeMillis(), random.nextDouble(), 256 + random.nextInt(256) * random.nextDouble()));
            requests.set("mem", new HazelcostRequest("mem", System.currentTimeMillis(), random.nextDouble(), 256 + random.nextInt(256) * random.nextDouble()));
        }

        try {
            Pipeline p1 = Pipeline.create();
            StreamStage<Map.Entry<Object, Object>> requestsBatchStage = p1
                    .readFrom(Sources.mapJournal(requestsMapName, START_FROM_OLDEST)).withoutTimestamps();

//            requestsBatchStage
//             .withoutTimestamps()
//             .groupingKey(Map.Entry::getKey).rollingAggregate(AggregateOperations.counting())
//             .writeTo(Sinks.map("requestCounts"));

//            jet.newJob(p1);

//            Pipeline p2 = Pipeline.create();
            StreamStage<Map.Entry<Object, Object>> requestsBatchStage2 = p1
                    .readFrom(Sources.mapJournal(requestsMapName, START_FROM_OLDEST)).withoutTimestamps();

//            requestsBatchStage
//                    .rollingAggregate(AggregateOperations.averagingDouble((Map.Entry<String, HazelcostRequest> t) -> t.getValue().getCpuUsage()))
//                    .writeTo(Sinks.map("cpuAvgs"));
//
//            jet.newJob(p2);

            Pipeline p3 = Pipeline.create();
            StreamSourceStage<Map.Entry<String, HazelcostRequest>> requestsBatchStage3 = p3
                    .readFrom(Sources.mapJournal(requestsMapName, START_FROM_OLDEST));

            requestsBatchStage3
                    .withoutTimestamps()
                    .groupingKey(Map.Entry::getKey)
                    .rollingAggregate(AggregateOperations.averagingDouble((Map.Entry<String, HazelcostRequest> t) -> t.getValue().getMemUsage()))
                    .writeTo(Sinks.map("memAvgs"));

            jet.newJob(p3);

            Pipeline p4 = Pipeline.create();
            BatchStage<Map.Entry<Object, Object>> requestCountsBatchStage = p4.readFrom(Sources.map("requestCounts"));
            BatchStage<Map.Entry<Object, Object>> cpuAvgsBatchStage = p4.readFrom(Sources.map("cpuAvgs"));
            BatchStage<Map.Entry<Object, Object>> memAvgsBatchStage = p4.readFrom(Sources.map("memAvgs"));

            BatchStage<Map.Entry<Object, Object>> merge = requestCountsBatchStage.merge(cpuAvgsBatchStage)
                                                                                 .merge(memAvgsBatchStage);
            merge.writeTo(Sinks.map("output"));
            jet.newJob(p4);

            TimeUnit.SECONDS.sleep(3);
//            IList<HazelcostRequest> tempList = hazelcastInstance.getList("temp");
//            System.out.println("Read " + tempList.size() + " entries from map journal.");
//            for (HazelcostRequest hazelcostRequest : tempList) {
//                System.out.println(hazelcostRequest);
//            }

            for (Map.Entry<Object, Object> entry : hazelcastInstance.getMap("requestCounts").entrySet()) {
                System.out.println(entry.getKey() + " : " + entry.getValue());
            }
            for (Map.Entry<Object, Object> entry : hazelcastInstance.getMap("cpuAvgs").entrySet()) {
                System.out.println(entry.getKey() + " : " + entry.getValue());
            }
            for (Map.Entry<Object, Object> entry : hazelcastInstance.getMap("memAvgs").entrySet()) {
                System.out.println(entry.getKey() + " : " + entry.getValue());
            }
            System.out.println("Output:");
            for (Map.Entry<Object, Object> entry : hazelcastInstance.getMap("output").entrySet()) {
                System.out.println(entry.getKey() + " : " + entry.getValue());
            }
        } finally {
            hazelcastInstance.shutdown();
        }
    }
}
