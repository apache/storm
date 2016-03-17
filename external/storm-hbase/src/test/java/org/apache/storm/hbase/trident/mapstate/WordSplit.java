package org.apache.storm.hbase.trident.mapstate;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class WordSplit extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sentence = (String) tuple.getValue(0);
		if (sentence != null) {
			sentence = sentence.replaceAll("\r", "");
			sentence = sentence.replaceAll("\n", "");
			for (String word : sentence.split(" ")) {
				if (word.length() != 0)
					collector.emit(new Values(word));
			}
		}
	}
}
