package backtype.storm.serialization;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;

import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class KryoTupleSerializer implements ITupleSerializer {
	KryoValuesSerializer _kryo;
	SerializationFactory.IdDictionary _ids;
	Output _kryoOut;

	public KryoTupleSerializer(final Map conf,
			final GeneralTopologyContext context) {
		_kryo = new KryoValuesSerializer(conf);
		_kryoOut = new Output(2000, 2000000000);
		_ids = new SerializationFactory.IdDictionary(context.getRawTopology());
	}

	/**
	 * @@@ in the furture, it will skill serialize 'targetTask' through check
	 *     some flag
	 * @see backtype.storm.serialization.ITupleSerializer#serialize(int,
	 *      backtype.storm.tuple.Tuple)
	 */
	public byte[] serialize(Tuple tuple) {
		try {

			_kryoOut.clear();
			if (tuple instanceof TupleExt) {
				_kryoOut.writeInt(((TupleExt) tuple).getTargetTaskId());
			}

			_kryoOut.writeInt(tuple.getSourceTask(), true);
			_kryoOut.writeInt(
					_ids.getStreamId(tuple.getSourceComponent(),
							tuple.getSourceStreamId()), true);
			tuple.getMessageId().serialize(_kryoOut);
			_kryo.serializeInto(tuple.getValues(), _kryoOut);
			return _kryoOut.toBytes();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static byte[] serialize(int targetTask) {
		ByteBuffer buff = ByteBuffer.allocate((Integer.SIZE / 8));
		buff.putInt(targetTask);
		byte[] rtn = buff.array();
		return rtn;
	}

	// public long crc32(Tuple tuple) {
	// try {
	// CRC32OutputStream hasher = new CRC32OutputStream();
	// _kryo.serializeInto(tuple.getValues(), hasher);
	// return hasher.getValue();
	// } catch (IOException e) {
	// throw new RuntimeException(e);
	// }
	// }
}
