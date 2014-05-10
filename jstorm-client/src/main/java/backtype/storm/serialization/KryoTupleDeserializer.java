package backtype.storm.serialization;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;

import com.esotericsoftware.kryo.io.Input;

import java.io.IOException;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class KryoTupleDeserializer implements ITupleDeserializer {
	private static final Logger LOG = Logger.getLogger(KryoTupleDeserializer.class);
	
	public static final boolean USE_RAW_PACKET = true;

	GeneralTopologyContext _context;
	KryoValuesDeserializer _kryo;
	SerializationFactory.IdDictionary _ids;
	Input _kryoInput;

	public KryoTupleDeserializer(final Map conf,
			final GeneralTopologyContext context) {
		_kryo = new KryoValuesDeserializer(conf);
		_context = context;
		_ids = new SerializationFactory.IdDictionary(context.getRawTopology());
		_kryoInput = new Input(1);
	}

	public Tuple deserialize(byte[] ser) {
		
		int targetTaskId = 0;
		int taskId = 0;
		int streamId = 0;
		String componentName = null;
		String streamName = null;
		MessageId id = null;
		
		try {

			_kryoInput.setBuffer(ser);

			targetTaskId = _kryoInput.readInt();
			taskId = _kryoInput.readInt(true);
			streamId = _kryoInput.readInt(true);
			componentName = _context.getComponentId(taskId);
			streamName = _ids.getStreamName(componentName, streamId);
			id = MessageId.deserialize(_kryoInput);
			List<Object> values = _kryo.deserializeFrom(_kryoInput);
			TupleImplExt tuple = new TupleImplExt(_context, values, taskId,
					streamName, id);
			tuple.setTargetTaskId(targetTaskId);
			return tuple;
		} catch (Throwable e) {
			StringBuilder sb = new StringBuilder();
			
			sb.append("Deserialize error:");
			sb.append("targetTaskId:").append(targetTaskId);
			sb.append(",taskId:").append(taskId);
			sb.append(",streamId:").append(streamId);
			sb.append(",componentName:").append(componentName);
			sb.append(",streamName:").append(streamName);
			sb.append(",MessageId").append(id);
			
			LOG.info(sb.toString(), e );
			throw new RuntimeException(e);
		}
	}

	/**
	 * just get target taskId
	 * 
	 * @param ser
	 * @return
	 */
	public static int deserializeTaskId(byte[] ser) {
		Input _kryoInput = new Input(1);

		_kryoInput.setBuffer(ser);

		int targetTaskId = _kryoInput.readInt();

		return targetTaskId;
	}
}
