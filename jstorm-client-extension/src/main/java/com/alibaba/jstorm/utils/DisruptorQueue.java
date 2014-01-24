//package com.alibaba.jstorm.utils;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.Executor;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//import org.apache.commons.lang.mutable.MutableObject;
//
//import com.lmax.disruptor.EventFactory;
//import com.lmax.disruptor.ExceptionHandler;
//import com.lmax.disruptor.FatalExceptionHandler;
//import com.lmax.disruptor.RingBuffer;
//import com.lmax.disruptor.Sequence;
//import com.lmax.disruptor.SequenceBarrier;
//import com.lmax.disruptor.Sequencer;
//import com.lmax.disruptor.WaitStrategy;
//import com.lmax.disruptor.WorkHandler;
//import com.lmax.disruptor.WorkProcessor;
//import com.lmax.disruptor.util.Util;
//
//public class DisruptorQueue<T> {
//    private final RingBuffer<MutableObject> ringBuffer;
//    private final SequenceBarrier           sequenceBarrier;
//    private final ExceptionHandler          exceptionHandler;
//    private final List<WorkProcessor>       workProcessors;
//    private final Sequence                  workSequence;
//    private final AtomicBoolean             started = new AtomicBoolean(false);
//    
//    public DisruptorQueue(boolean isMultiProducer, int bufferSize,
//            WaitStrategy waitStrategy) {
//        if (isMultiProducer) {
//            ringBuffer = RingBuffer.createMultiProducer(
//                    new ObjectEventFactory(), bufferSize, waitStrategy);
//        } else {
//            ringBuffer = RingBuffer.createSingleProducer(
//                    new ObjectEventFactory(), bufferSize, waitStrategy);
//        }
//        
//        sequenceBarrier = ringBuffer.newBarrier();
//        exceptionHandler = new FatalExceptionHandler();
//        workProcessors = new ArrayList<WorkProcessor>();
//        workSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
//    }
//    
//    public void register(WorkHandler<T> handler) {
//        WorkProcessor workProcessor = new WorkProcessor(ringBuffer,
//                sequenceBarrier, new HandleWraper(handler), exceptionHandler,
//                workSequence);
//        
//        ringBuffer.addGatingSequences(workProcessor.getSequence());
//        
//        workProcessors.add(workProcessor);
//    }
//    
//    void cleanup() {
//        
//    }
//    
//    /**
//     * Start the worker pool processing events in sequence.
//     * 
//     * @param executor
//     *            providing threads for running the workers.
//     * @return the {@link RingBuffer} used for the work queue.
//     * @throws IllegalStateException
//     *             if the pool has already been started and not halted yet
//     */
//    public void start() {
//        if (!started.compareAndSet(false, true)) {
//            throw new IllegalStateException(
//                    "WorkerPool has already been started and cannot be restarted until halted.");
//        }
//        
//        final long cursor = ringBuffer.getCursor();
//        workSequence.set(cursor);
//        
//        for (WorkProcessor<T> processor : workProcessors) {
//            processor.getSequence().set(cursor);
//            new Thread(processor).start();
//        }
//        
//        return;
//    }
//    
//    public Sequence[] getWorkerSequences() {
//        final Sequence[] sequences = new Sequence[workProcessors.size()];
//        for (int i = 0, size = workProcessors.size(); i < size; i++) {
//            sequences[i] = workProcessors.get(i).getSequence();
//        }
//        
//        return sequences;
//    }
//    
//    /**
//     * Wait for the {@link RingBuffer} to drain of published events then halt
//     * the workers.
//     */
//    public void drainAndHalt() {
//        Sequence[] workerSequences = getWorkerSequences();
//        while (ringBuffer.getCursor() > Util
//                .getMinimumSequence(workerSequences)) {
//            Thread.yield();
//        }
//        
//        for (WorkProcessor<?> processor : workProcessors) {
//            processor.halt();
//        }
//        
//        started.set(false);
//    }
//    
//    /**
//     * Halt all workers immediately at the end of their current cycle.
//     */
//    public void halt() {
//        for (WorkProcessor<?> processor : workProcessors) {
//            processor.halt();
//        }
//        
//        started.set(false);
//    }
//    
//    public void offer(T o) {
//        long sequence = ringBuffer.next();
//        ringBuffer.get(sequence).setValue(o);
//        ringBuffer.publish(sequence);
//    }
//    
//    public static class ObjectEventFactory implements
//            EventFactory<MutableObject> {
//        
//        public MutableObject newInstance() {
//            return new MutableObject();
//        }
//    }
//    
//    public static class HandleWraper<T> implements WorkHandler<MutableObject> {
//        private WorkHandler<T> handler;
//        
//        public HandleWraper(WorkHandler<T> handler) {
//            this.handler = handler;
//        }
//        
//        public void onEvent(MutableObject event) throws Exception {
//            // TODO Auto-generated method stub
//            handler.onEvent((T) event.getValue());
//        }
//        
//    }
// }
