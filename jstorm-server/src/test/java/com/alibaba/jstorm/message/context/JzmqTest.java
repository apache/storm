package com.alibaba.jstorm.message.context;

import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Context;
import org.junit.Test;


public class JzmqTest {
	
	static class Client extends Thread {
		
		private boolean isRunning = false;
		
        private ZMQ.Socket s = null;
        public Client (Context ctx) {
            s = ctx.socket(ZMQ.PULL);
        }

        @Override
        public void run () {
        	isRunning = true;
            System.out.println("Start client thread ");
            s.connect( "tcp://127.0.0.1:6669");
            byte[] recvBytes;
            
            
            while (isRunning) {
            	try {
	            	recvBytes = s.recv(0);
	            	if (recvBytes != null) {
	            		System.out.println("recv: " + new String(recvBytes));
	            	}
            	}catch(Exception e ) {
            		break;
            	}
            }
        	s.close();
            System.out.println("Stop client thread ");
        }
        
        public void cleanup() {
        	isRunning = false;
        }
    }

	@Test
	public void test_jzmq() {
		ZMQ.Context context = ZMQ.context(1);
        final Client client = new Client (context);
        
        ZMQ.Socket sender = context.socket (ZMQ.PUSH);
        sender.bind ("tcp://127.0.0.1:6669");

//        ZMQ.Poller outItems;
//        outItems = context.poller ();
//        outItems.register (sender, ZMQ.Poller.POLLOUT);

//        while (!Thread.currentThread ().isInterrupted ()) {
//
//            outItems.poll (1000);
//            if (outItems.pollout (0)) {
//                sender.send ("OK", 0);
//                System.out.println ("ok");
//                break;
//            } else {
//                System.out.println ("not writable");
//                client.start ();
//            }
//        }
        int i = 0;
        Thread thread = new Thread(new Runnable(){

			@Override
			public void run() {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
				}
				System.out.println("start client...");
				client.start();
			}
        	
        });
        thread.start();
        
        while (true) {
        	if (i >= 5) {
        		break;
        	}
        	sender.send("Oks" + i, 0);
        	System.out.println("send: " + "Oks" + i);
        	++i;
        }

        try {
        	client.cleanup();
        	client.join();
        } catch (Exception e) {
        }
        sender.close ();
        context.term ();
        System.out.println("Finish jeroMq test");
	}
}
