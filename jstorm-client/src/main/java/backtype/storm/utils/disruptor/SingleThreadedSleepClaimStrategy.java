package backtype.storm.utils.disruptor;

import static com.lmax.disruptor.util.Util.getMinimumSequence;

import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.util.PaddedLong;

/**
 * Copy from com.lmax.disruptor.SingleThreadedClaimStrategy
 * 
 */
public  class SingleThreadedSleepClaimStrategy
    implements ClaimStrategy
{
    private final int bufferSize;
    private final PaddedLong minGatingSequence = new PaddedLong(Sequencer.INITIAL_CURSOR_VALUE);
    private final PaddedLong claimSequence = new PaddedLong(Sequencer.INITIAL_CURSOR_VALUE);

    /**
     * Construct a new single threaded publisher {@link ClaimStrategy} for a given buffer size.
     *
     * @param bufferSize for the underlying data structure.
     */
    public SingleThreadedSleepClaimStrategy(final int bufferSize)
    {
        this.bufferSize = bufferSize;
    }
    
    public SingleThreadedSleepClaimStrategy(ClaimStrategy claim)
    {
        this.bufferSize = claim.getBufferSize();
    }

    @Override
    public int getBufferSize()
    {
        return bufferSize;
    }

    @Override
    public long getSequence()
    {
        return claimSequence.get();
    }

    @Override
    public boolean hasAvailableCapacity(final int availableCapacity, final Sequence[] dependentSequences)
    {
        final long wrapPoint = (claimSequence.get() + availableCapacity) - bufferSize;
        if (wrapPoint > minGatingSequence.get())
        {
            long minSequence = getMinimumSequence(dependentSequences);
            minGatingSequence.set(minSequence);

            if (wrapPoint > minSequence)
            {
                return false;
            }
        }

        return true;
    }

    @Override
    public long incrementAndGet(final Sequence[] dependentSequences)
    {
        long nextSequence = claimSequence.get() + 1L;
        claimSequence.set(nextSequence);
        waitForFreeSlotAt(nextSequence, dependentSequences);

        return nextSequence;
    }

    @Override
    public long incrementAndGet(final int delta, final Sequence[] dependentSequences)
    {
        long nextSequence = claimSequence.get() + delta;
        claimSequence.set(nextSequence);
        waitForFreeSlotAt(nextSequence, dependentSequences);

        return nextSequence;
    }

    @Override
    public void setSequence(final long sequence, final Sequence[] dependentSequences)
    {
        claimSequence.set(sequence);
        waitForFreeSlotAt(sequence, dependentSequences);
    }

    @Override
    public void serialisePublishing(final long sequence, final Sequence cursor, final int batchSize)
    {
        cursor.set(sequence);
    }
    
    @Override
    public long checkAndIncrement(int availableCapacity, int delta, Sequence[] dependentSequences) 
            throws InsufficientCapacityException
    {
        if (!hasAvailableCapacity(availableCapacity, dependentSequences))
        {
            throw InsufficientCapacityException.INSTANCE;
        }
        
        return incrementAndGet(delta, dependentSequences);
    }

    protected void waitForFreeSlotAt(final long sequence, final Sequence[] dependentSequences)
    {
        final long wrapPoint = sequence - bufferSize;
        if (wrapPoint > minGatingSequence.get())
        {
            long minSequence;
            while (wrapPoint > (minSequence = getMinimumSequence(dependentSequences)))
            {
                try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
				}
            }

            minGatingSequence.set(minSequence);
        }
    }
}