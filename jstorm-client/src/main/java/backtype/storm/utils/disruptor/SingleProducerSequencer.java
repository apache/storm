package backtype.storm.utils.disruptor;

import java.util.concurrent.locks.LockSupport;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.util.Util;


/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.<p>
 *
 * <p>Generally not safe for use from multiple threads as it does not implement any barriers.</p>
 */
public class SingleProducerSequencer extends AbstractSequencerExt
{
    @SuppressWarnings("unused")
    private static class Padding
    {
        /** Set to -1 as sequence starting point */
        public long nextValue = -1L, cachedValue = -1L, p2, p3, p4, p5, p6, p7;
    }

    private final Padding pad = new Padding();

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    public SingleProducerSequencer(int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
    }

    /**
     * @see Sequencer#hasAvailableCapacity(int)
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity)
    {
        long nextValue = pad.nextValue;

        long wrapPoint = (nextValue + requiredCapacity) - bufferSize;
        long cachedGatingSequence = pad.cachedValue;

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue)
        {
            long minSequence = Util.getMinimumSequence(gatingSequences, nextValue);
            pad.cachedValue = minSequence;

            if (wrapPoint > minSequence)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * @see Sequencer#next()
     */
    @Override
    public long next()
    {
        return next(1);
    }

    /**
     * @see Sequencer#next(int)
     */
    @Override
    public long next(int n)
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        long nextValue = pad.nextValue;

        long nextSequence = nextValue + n;
        long wrapPoint = nextSequence - bufferSize;
        long cachedGatingSequence = pad.cachedValue;

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue)
        {
            long minSequence;
            while (wrapPoint > (minSequence = Util.getMinimumSequence(gatingSequences, nextValue)))
            {
            	if (AbstractSequencerExt.isWaitSleep()) {
            		try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
					}
            	}else {
            		LockSupport.parkNanos(1); 
            	}
            }

            pad.cachedValue = minSequence;
        }

        pad.nextValue = nextSequence;

        return nextSequence;
    }

    /**
     * @see Sequencer#tryNext()
     */
    @Override
    public long tryNext() throws InsufficientCapacityException
    {
        return tryNext(1);
    }

    /**
     * @see Sequencer#tryNext(int)
     */
    @Override
    public long tryNext(int n) throws InsufficientCapacityException
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        if (!hasAvailableCapacity(n))
        {
            throw InsufficientCapacityException.INSTANCE;
        }

        long nextSequence = pad.nextValue += n;

        return nextSequence;
    }

    /**
     * @see Sequencer#remainingCapacity()
     */
    @Override
    public long remainingCapacity()
    {
        long nextValue = pad.nextValue;

        long consumed = Util.getMinimumSequence(gatingSequences, nextValue);
        long produced = nextValue;
        return getBufferSize() - (produced - consumed);
    }

    /**
     * @see Sequencer#claim(long)
     */
    @Override
    public void claim(long sequence)
    {
        pad.nextValue = sequence;
    }

    /**
     * @see Sequencer#publish(long)
     */
    @Override
    public void publish(long sequence)
    {
        cursor.set(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * @see Sequencer#publish(long, long)
     */
    @Override
    public void publish(long lo, long hi)
    {
        publish(hi);
    }

    /**
     * @see Sequencer#isAvailable(long)
     */
    @Override
    public boolean isAvailable(long sequence)
    {
        return sequence <= cursor.get();
    }

    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence)
    {
        return availableSequence;
    }
}

