import com.hazelcast.collection.IList;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.map.IMap;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.pipeline.JoinClause.onKeys;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;

public class Main {
    public static void main(String[] args)
            throws InterruptedException {
        new Main().start();
    }

    private void start()
            throws InterruptedException {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
        JetService jet = hazelcastInstance.getJet();
        String requestsMapName = "requests";
        new Thread(() -> {
            IMap<String, HazelcostRequest> requests = hazelcastInstance.getMap(requestsMapName);

            Random random = new Random();

            while (true) {
                requests.set("cpu", new HazelcostRequest("cpu", System.currentTimeMillis(), random.nextDouble(), 256 + random.nextInt(256) * random.nextDouble()));
                requests.set("mem", new HazelcostRequest("mem", System.currentTimeMillis(), random.nextDouble(), 256 + random.nextInt(256) * random.nextDouble()));
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();


        try {
            Pipeline p1 = Pipeline.create();
            StreamSourceStage<Map.Entry<String, HazelcostRequest>> requestsBatchStage = p1
                    .readFrom(Sources.mapJournal(requestsMapName, START_FROM_OLDEST));

            requestsBatchStage
             .withoutTimestamps()
             .groupingKey(Map.Entry::getKey).rollingAggregate(counting())
             .writeTo(Sinks.map("requestCounts"));

            jet.newJob(p1);

            Pipeline p2 = Pipeline.create();
            StreamSourceStage<Map.Entry<String, HazelcostRequest>> requestsBatchStage2 = p2
                    .readFrom(Sources.mapJournal(requestsMapName, START_FROM_OLDEST));

            requestsBatchStage2
                    .withoutTimestamps()
                    .groupingKey(Map.Entry::getKey)
                    .rollingAggregate(AggregateOperations.averagingDouble((Map.Entry<String, HazelcostRequest> t) -> t.getValue().getCpuUsage()))
                    .writeTo(Sinks.map("cpuAvgs"));

            jet.newJob(p2);

            Pipeline p3 = Pipeline.create();
            StreamSourceStage<Map.Entry<String, HazelcostRequest>> requestsBatchStage3 = p3
                    .readFrom(Sources.mapJournal(requestsMapName, START_FROM_OLDEST));

            requestsBatchStage3
                    .withoutTimestamps()
                    .groupingKey(Map.Entry::getKey)
                    .rollingAggregate(AggregateOperations.averagingDouble((Map.Entry<String, HazelcostRequest> t) -> t.getValue().getMemUsage()))
                    .writeTo(Sinks.map("memAvgs"));

            jet.newJob(p3);

            TimeUnit.SECONDS.sleep(3);

            Pipeline p4 = Pipeline.create();
            StreamSourceStage<Map.Entry<String, Integer>> requestCountsBatchStage = p4.readFrom(Sources.mapJournal("requestCounts", START_FROM_OLDEST));
//            BatchStage<Map.Entry<String, Double>> cpuAvgsBatchStage = p4.readFrom(Sources.map("cpuAvgs"));
//            BatchStage<Map.Entry<String, Double>> memAvgsBatchStage = p4.readFrom(Sources.map("memAvgs"));

            IMap<String, Double> cpuAvgs = hazelcastInstance.getMap("cpuAvgs");
            IMap<String, Double> memAvgs = hazelcastInstance.getMap("memAvgs");
            requestCountsBatchStage.withoutTimestamps().mapUsingIMap(cpuAvgs, Map.Entry::getKey, Tuple2::tuple2)
                                   .mapUsingIMap(memAvgs, tuple2 -> tuple2.f0().getKey(), (tuple2, mem) -> tuple3(tuple2.f0(), tuple2.f1(), mem))
                                   .map(tuple -> {
                                       if (tuple.f1() > 0.3) {
                                           return tuple2(tuple.f0().getKey(), "Scale up!");
                                       }
                                       return tuple2(tuple.f0().getKey(), "Normal");
                                   })
                                   .writeTo(Sinks.map("output"));

            jet.newJob(p4);

            TimeUnit.SECONDS.sleep(3);

            Pipeline p5 = Pipeline.create();
            StreamSourceStage<Map.Entry<String, HazelcostRequest>> outputStreamStage = p5
                    .readFrom(Sources.mapJournal("output", START_FROM_OLDEST));

            outputStreamStage
                    .withoutTimestamps()
                    .writeTo(Sinks.logger());

            jet.newJob(p5);

            TimeUnit.SECONDS.sleep(3);

            //            IList<HazelcostRequest> tempList = hazelcastInstance.getList("temp");
//            System.out.println("Read " + tempList.size() + " entries from map journal.");
//            for (HazelcostRequest hazelcostRequest : tempList) {
//                System.out.println(hazelcostRequest);
//            }

//            for (Map.Entry<Object, Object> entry : hazelcastInstance.getMap("requestCounts").entrySet()) {
//                System.out.println(entry.getKey() + " : " + entry.getValue());
//            }
//            for (Map.Entry<String, Double> entry : cpuAvgs.entrySet()) {
//                System.out.println(entry.getKey() + " : " + entry.getValue());
//            }
//            for (Map.Entry<String, Double> entry : memAvgs.entrySet()) {
//                System.out.println(entry.getKey() + " : " + entry.getValue());
//            }
//            System.out.println("Output:");
//            IMap<Object, Object> output = hazelcastInstance.getMap("output");
//            for (Map.Entry<Object, Object> entry : output.entrySet()) {
//                System.out.println(entry.getKey() + " : " + entry.getValue());
//            }
        } finally {
//            hazelcastInstance.shutdown();
        }
    }
}
