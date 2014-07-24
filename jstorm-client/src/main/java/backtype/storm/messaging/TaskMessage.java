package backtype.storm.messaging;

import java.nio.ByteBuffer;

public class TaskMessage {
	private int _task;
	private byte[] _message;

	public TaskMessage(int task, byte[] message) {
		_task = task;
		_message = message;
	}

	public int task() {
		return _task;
	}

	public byte[] message() {
		return _message;
	}
	
	public static boolean isEmpty(TaskMessage message) {
		if (message == null) {
			return true;
		}else if (message.message() == null) {
			return true;
		}else if (message.message().length == 0) {
			return true;
		}
		
		return false;
	}

	@Deprecated
	public ByteBuffer serialize() {
		ByteBuffer bb = ByteBuffer.allocate(_message.length + 2);
		bb.putShort((short) _task);
		bb.put(_message);
		return bb;
	}

	@Deprecated
	public void deserialize(ByteBuffer packet) {
		if (packet == null)
			return;
		_task = packet.getShort();
		_message = new byte[packet.limit() - 2];
		packet.get(_message);
	}

}
