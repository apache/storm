package backtype.storm.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;

public class ThriftTopologyUtils {
	public static Set<String> getComponentIds(StormTopology topology) {
		Set<String> ret = new HashSet<String>();
		for (StormTopology._Fields f : StormTopology.metaDataMap.keySet()) {
			Map<String, Object> componentMap = (Map<String, Object>) topology
					.getFieldValue(f);
			ret.addAll(componentMap.keySet());
		}
		return ret;
	}

	public static Map<String, Object> getComponents(StormTopology topology) {
		Map<String, Object> ret = new HashMap<String, Object>();
		for (StormTopology._Fields f : StormTopology.metaDataMap.keySet()) {
			Map<String, Object> componentMap = (Map<String, Object>) topology
					.getFieldValue(f);
			ret.putAll(componentMap);
		}
		return ret;
	}

	public static ComponentCommon getComponentCommon(StormTopology topology,
			String componentId) {
		for (StormTopology._Fields f : StormTopology.metaDataMap.keySet()) {
			Map<String, Object> componentMap = (Map<String, Object>) topology
					.getFieldValue(f);
			if (componentMap.containsKey(componentId)) {
				Object component = componentMap.get(componentId);
				if (component instanceof Bolt) {
					return ((Bolt) component).get_common();
				}
				if (component instanceof SpoutSpec) {
					return ((SpoutSpec) component).get_common();
				}
				if (component instanceof StateSpoutSpec) {
					return ((StateSpoutSpec) component).get_common();
				}
				throw new RuntimeException(
						"Unreachable code! No get_common conversion for component "
								+ component);
			}
		}
		throw new IllegalArgumentException(
				"Could not find component common for " + componentId);
	}
}
