package storm.kafka;

import java.util.Map;

/**
 * Abstraction of a partition state storage.
 *
 * The partition state usually is kept in Json format in the store and in Map format in runtime memory. An example
 * is shown below:
 *
 * Json:
 *  {
 *      "broker": {
 *          "host": "kafka.sample.net",
 *          "port": 9092
 *      },
 *      "offset": 4285,
 *      "partition": 1,
 *      "topic": "testTopic",
 *      "topology": {
 *          "id": "fce905ff-25e0 -409e-bc3a-d855f 787d13b",
 *          "name": "Test Topology"
 *      }
 *  }
 *
 * Memory:
 *  Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
 *        .put("topology", ImmutableMap.of(
 *                "id", "fce905ff-25e0 -409e-bc3a-d855f 787d13b",
 *                "name", "Test Topology"))
 *        .put("offset", 4285)
 *        .put("partition", 1)
 *        .put("broker", ImmutableMap.of(
 *                "host", "kafka.sample.net",
 *                "port", 9092))
 *        .put("topic", "testTopic").build();
 */
public interface StateStore {

    Map<Object, Object> readState(Partition p);

    void writeState(Partition p, Map<Object, Object> state);
}