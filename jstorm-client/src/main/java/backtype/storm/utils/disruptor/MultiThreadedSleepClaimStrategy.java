package backtype.storm.utils.disruptor;

import java.util.concurrent.atomic.AtomicLongArray;

import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.MultiThreadedLowContentionClaimStrategy;
import com.lmax.disruptor.Sequence;


/**
 * Strategy to be used when there are multiple publisher threads claiming sequences.
 *
 * This strategy is reasonably forgiving when the multiple publisher threads are highly contended or working in an
 * environment where there is insufficient CPUs to handle multiple publisher threads.  It requires 2 CAS operations
 * for a single publisher, compared to the {@link MultiThreadedLowContentionClaimStrategy} strategy which needs only a single
 * CAS and a lazySet per publication.
 */
public final class MultiThreadedSleepClaimStrategy extends AbstractMultithreadedSleepClaimStrategy
    implements ClaimStrategy
{
    private static final int RETRIES = 1000;

    private final AtomicLongArray pendingPublication;
    private final int pendingMask;

    /**
     * Construct a new multi-threaded publisher {@link ClaimStrategy} for a given buffer size.
     *
     * @param bufferSize for the underlying data structure.
     * @param pendingBufferSize number of item that can be pending for serialisation
     */
    public MultiThreadedSleepClaimStrategy(final int bufferSize, final int pendingBufferSize)
    {
        super(bufferSize);
        
        if (Integer.bitCount(pendingBufferSize) != 1)
        {
            throw new IllegalArgumentException("pendingBufferSize must be a power of 2, was: " + pendingBufferSize);
        }

        this.pendingPublication = new AtomicLongArray(pendingBufferSize);
        this.pendingMask = pendingBufferSize - 1;
    }

    /**
     * Construct a new multi-threaded publisher {@link ClaimStrategy} for a given buffer size.
     *
     * @param bufferSize for the underlying data structure.
     */
    public MultiThreadedSleepClaimStrategy(final int bufferSize)
    {
        this(bufferSize, 1024);
    }
    
    public MultiThreadedSleepClaimStrategy(ClaimStrategy claim) {
    	this(claim.getBufferSize());
    }

    @Override
    public void serialisePublishing(final long sequence, final Sequence cursor, final int batchSize)
    {
        int counter = RETRIES;
        while (sequence - cursor.get() > pendingPublication.length())
        {
            if (--counter == 0)
            {
                Thread.yield();
                counter = RETRIES;
            }
        }

        long expectedSequence = sequence - batchSize;
        for (long pendingSequence = expectedSequence + 1; pendingSequence < sequence; pendingSequence++)
        {
            pendingPublication.lazySet((int) pendingSequence & pendingMask, pendingSequence);
        }
        pendingPublication.set((int) sequence & pendingMask, sequence);

        long cursorSequence = cursor.get();
        if (cursorSequence >= sequence)
        {
            return;
        }

        expectedSequence = Math.max(expectedSequence, cursorSequence);
        long nextSequence = expectedSequence + 1;
        while (cursor.compareAndSet(expectedSequence, nextSequence))
        {
            expectedSequence = nextSequence;
            nextSequence++;
            if (pendingPublication.get((int) nextSequence & pendingMask) != nextSequence)
            {
                break;
            }
        }
    }
}
