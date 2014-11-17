package backtype.storm.utils.disruptor;

import com.lmax.disruptor.AbstractSequencer;
import com.lmax.disruptor.WaitStrategy;

public abstract class AbstractSequencerExt extends AbstractSequencer {
	private static boolean waitSleep = true;

	public static boolean isWaitSleep() {
		return waitSleep;
	}

	public static void setWaitSleep(boolean waitSleep) {
		AbstractSequencerExt.waitSleep = waitSleep;
	}

	public AbstractSequencerExt(int bufferSize, WaitStrategy waitStrategy) {
		super(bufferSize, waitStrategy);
	}

}
