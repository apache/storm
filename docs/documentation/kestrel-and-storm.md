---
layout: documentation
title: Kestrel and Storm
---
<!--Content Begin-->
<div class="content">
	<div class="container-fluid">
    	<div class="row">
        	<div class="col-md-12">
            	<h1 class="page-title">Using non-JVM languages with Storm</h1>
    <p>This page explains how to use to Storm to consume items from a Kestrel cluster.</p>

<h2>Preliminaries</h2>

<h3>Storm</h3>

<p>This tutorial uses examples from the <a href="https://github.com/nathanmarz/storm-kestrel" target="_blank">storm-kestrel</a> project and the <a href="http://github.com/apache/storm/blob/master/examples/storm-starter" target="_blank">storm-starter</a> project. It's recommended that you clone those projects and follow along with the examples. Read <a href="https://github.com/apache/storm/wiki/Setting-up-development-environment" target="_blank">Setting up development environment</a> and <a href="https://github.com/apache/storm/wiki/Creating-a-new-Storm-project" target="_blank">Creating a new Storm project</a> to get your machine set up.</p>

<h3>Kestrel</h3>

<p>It assumes you are able to run locally a Kestrel server as described <a href="https://github.com/nathanmarz/storm-kestrel" target="_blank">here</a>.</p>

<h2>Kestrel Server and Queue</h2>

<p>A single kestrel server has a set of queues. A Kestrel queue is a very simple message queue that runs on the JVM and uses the memcache protocol (with some extensions) to talk to clients. For details, look at the implementation of the <a href="https://github.com/nathanmarz/storm-kestrel/blob/master/src/jvm/backtype/storm/spout/KestrelThriftClient.java" target="_blank">KestrelThriftClient</a> class provided in <a href="https://github.com/nathanmarz/storm-kestrel" target="_blank">storm-kestrel</a> project.</p>

<p>Each queue is strictly ordered following the FIFO (first in, first out) principle. To keep up with performance items are cached in system memory; though, only the first 128MB is kept in memory. When stopping the server, the queue state is stored in a journal file.</p>

<p>Further, details can be found <a href="https://github.com/nathanmarz/kestrel/blob/master/docs/guide.md" target="_blank">here</a>.</p>

<p>Kestrel is:
* fast
* small
* durable
* reliable</p>

<p>For instance, Twitter uses Kestrel as the backbone of its messaging infrastructure as described <a href="http://bhavin.directi.com/notes-on-kestrel-the-open-source-twitter-queue/">here</a>.</p>

<h2>Add items to Kestrel</h2>

<p>At first, we need to have a program that can add items to a Kestrel queue. The following method takes benefit of the KestrelClient implementation in <a href="https://github.com/nathanmarz/storm-kestrel">storm-kestrel</a>. It adds sentences into a Kestrel queue randomly chosen out of an array that holds five possible sentences.</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">    private static void queueSentenceItems(KestrelClient kestrelClient, String queueName)
            throws ParseError, IOException {

        String[] sentences = new String[] {
                "the cow jumped over the moon",
                "an apple a day keeps the doctor away",
                "four score and seven years ago",
                "snow white and the seven dwarfs",
                "i am at two with nature"};

        Random _rand = new Random();

        for(int i=1; i&lt;=10; i++){

            String sentence = sentences[_rand.nextInt(sentences.length)];

            String val = "ID " + i + " " + sentence;

            boolean queueSucess = kestrelClient.queue(queueName, val);

            System.out.println("queueSucess=" +queueSucess+ " [" + val +"]");
        }
    }
</code></pre></div>
<h2>Remove items from Kestrel</h2>

<p>This method dequeues items from a queue without removing them.
```
    private static void dequeueItems(KestrelClient kestrelClient, String queueName) throws IOException, ParseError
             {
        for(int i=1; i&lt;=12; i++){</p>
<div class="highlight"><pre><code class="language-text" data-lang="text">        Item item = kestrelClient.dequeue(queueName);

        if(item==null){
            System.out.println("The queue (" + queueName + ") contains no items.");
        }
        else
        {
            byte[] data = item._data;

            String receivedVal = new String(data);

            System.out.println("receivedItem=" + receivedVal);
        }
    }
</code></pre></div><div class="highlight"><pre><code class="language-text" data-lang="text">This method dequeues items from a queue and then removes them.
</code></pre></div><div class="highlight"><pre><code class="language-text" data-lang="text">private static void dequeueAndRemoveItems(KestrelClient kestrelClient, String queueName)
throws IOException, ParseError
     {
        for(int i=1; i&lt;=12; i++){

            Item item = kestrelClient.dequeue(queueName);


            if(item==null){
                System.out.println("The queue (" + queueName + ") contains no items.");
            }
            else
            {
                int itemID = item._id;


                byte[] data = item._data;

                String receivedVal = new String(data);

                kestrelClient.ack(queueName, itemID);

                System.out.println("receivedItem=" + receivedVal);
            }
        }
}
</code></pre></div><div class="highlight"><pre><code class="language-text" data-lang="text">## Add Items continuously to Kestrel

This is our final program to run in order to add continuously sentence items to a queue called **sentence_queue** of a locally running Kestrel server.

In order to stop it type a closing bracket char ']' in console and hit 'Enter'.
</code></pre></div><div class="highlight"><pre><code class="language-text" data-lang="text">import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import backtype.storm.spout.KestrelClient;
import backtype.storm.spout.KestrelClient.Item;
import backtype.storm.spout.KestrelClient.ParseError;

public class AddSentenceItemsToKestrel {

    /**
     * @param args
     */
    public static void main(String[] args) {

        InputStream is = System.in;

        char closing_bracket = ']';

        int val = closing_bracket;

        boolean aux = true;

        try {

            KestrelClient kestrelClient = null;
            String queueName = "sentence_queue";

            while(aux){

                kestrelClient = new KestrelClient("localhost",22133);

                queueSentenceItems(kestrelClient, queueName);

                kestrelClient.close();

                Thread.sleep(1000);

                if(is.available()&gt;0){
                 if(val==is.read())
                     aux=false;
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (ParseError e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println("end");

    }
}
</code></pre></div><div class="highlight"><pre><code class="language-text" data-lang="text">## Using KestrelSpout

This topology reads sentences off of a Kestrel queue using KestrelSpout, splits the sentences into its constituent words (Bolt: SplitSentence), and then emits for each word the number of times it has seen that word before (Bolt: WordCount). How data is processed is described in detail in [Guaranteeing message processing](Guaranteeing-message-processing.html).
</code></pre></div><div class="highlight"><pre><code class="language-text" data-lang="text">TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("sentences", new KestrelSpout("localhost",22133,"sentence_queue",new StringScheme()));
builder.setBolt("split", new SplitSentence(), 10)
            .shuffleGrouping("sentences");
builder.setBolt("count", new WordCount(), 20)
        .fieldsGrouping("split", new Fields("word"));
</code></pre></div><div class="highlight"><pre><code class="language-text" data-lang="text">## Execution

At first, start your local kestrel server in production or development mode.

Than, wait about 5 seconds in order to avoid a ConnectException.

Now execute the program to add items to the queue and launch the Storm topology. The order in which you launch the programs is of no importance.

If you run the topology with TOPOLOGY_DEBUG you should see tuples being emitted in the topology.
</code></pre></div>
            </div>
        </div>
    </div>
</div>
<!--Content End-->

