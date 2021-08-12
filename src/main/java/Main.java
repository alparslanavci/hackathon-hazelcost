import com.hazelcast.collection.IList;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.map.IMap;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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
        String resourcesMapName = "resources";
        IMap<String, HazelcostRequest> requests = hazelcastInstance.getMap(requestsMapName);
        IMap<String, HazelcostResource> resources = hazelcastInstance.getMap(resourcesMapName);

        for (int i = 0; i < 200; i++) {
            requests.set("cpu", new HazelcostRequest("cpu", System.currentTimeMillis()));
            requests.set("mem", new HazelcostRequest("mem", System.currentTimeMillis()));
        }
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            resources.set("cpu", new HazelcostResource("cpu", random.nextDouble(), 256 + random.nextInt(256)));
            resources.set("mem", new HazelcostResource("mem", random.nextDouble(), 256 + random.nextInt(256)));
        }

        try {
            Pipeline p = Pipeline.create();
            StreamSourceStage<Map.Entry<Integer, Integer>> requestsBatchStage = p
                    .readFrom(Sources.mapJournal(requestsMapName, START_FROM_OLDEST));
            StreamSourceStage<Map.Entry<Integer, Integer>> resourcesBatchStage = p
                    .readFrom(Sources.mapJournal(resourcesMapName, START_FROM_OLDEST));

            requestsBatchStage
                    .withoutTimestamps().merge(resourcesBatchStage.withoutTimestamps())
                    .rollingAggregate(AggregateOperations.counting())
                    .writeTo(Sinks.logger());

            requestsBatchStage
             .withoutTimestamps()
             .map(Map.Entry::getValue)
             .writeTo(Sinks.list("temp"));

            jet.newJob(p);

            TimeUnit.SECONDS.sleep(3);
            IList<Object> tempList = hazelcastInstance.getList("temp");
            System.out.println("Read " + tempList.size() + " entries from map journal.");
            for (Object o : tempList) {
                System.out.println(o);
            }

        } finally {
            hazelcastInstance.shutdown();
        }
    }
}
