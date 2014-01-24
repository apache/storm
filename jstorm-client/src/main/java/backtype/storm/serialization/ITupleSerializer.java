package backtype.storm.serialization;

import backtype.storm.tuple.Tuple;

public interface ITupleSerializer {
	/**
	 * serialize targetTask before the tuple, it should be stored in 4 bytes
	 * 
	 * @param targetTask
	 * @param tuple
	 * @return
	 */
	byte[] serialize(Tuple tuple);
	// long crc32(Tuple tuple);
}
