---
layout: documentation
title: Creating a new storm project---
<!--Content Begin-->
  <div class="container-fluid">
      <div class="row">
          <div class="col-md-12">
              <h1 class="page-title">Creating a new storm project</h1>
            </div>
        </div>
        <div class="row">
            <div class="col-md-12">
                <p>This page outlines how to set up a Storm project for development. The steps are:</p>

                <ol>
                <li>Add Storm jars to classpath</li>
                <li>If using multilang, add multilang dir to classpath</li>
                </ol>

                <p>Follow along to see how to set up the <a href="https://github.com/apache/storm/blob/master/examples/storm-starter">storm-starter</a> project in Eclipse.</p>

                <h3 id="add-storm-jars-to-classpath">Add Storm jars to classpath</h3>

                <p>You'll need the Storm jars on your classpath to develop Storm topologies. Using <a href="Maven.html">Maven</a> is highly recommended. <a href="https://github.com/apache/storm/blob/master/examples/storm-starter/pom.xml">Here's an example</a> of how to setup your pom.xml for a Storm project. If you don't want to use Maven, you can include the jars from the Storm release on your classpath.</p>

                <p>To set up the classpath in Eclipse, create a new Java project, include <code>src/jvm/</code> as a source path, and make sure all the jars in <code>lib/</code> and <code>lib/dev/</code> are in the <code>Referenced Libraries</code> section of the project.</p>

                <h3 id="if-using-multilang,-add-multilang-dir-to-classpath">If using multilang, add multilang dir to classpath</h3>

                <p>If you implement spouts or bolts in languages other than Java, then those implementations should be under the <code>multilang/resources/</code> directory of the project. For Storm to find these files in local mode, the <code>resources/</code> dir needs to be on the classpath. You can do this in Eclipse by adding <code>multilang/</code> as a source folder. You may also need to add multilang/resources as a source directory.</p>

                <p>For more information on writing topologies in other languages, see <a href="Using-non-JVM-languages-with-Storm.html">Using non-JVM languages with Storm</a>.</p>

                <p>To test that everything is working in Eclipse, you should now be able to <code>Run</code> the <code>WordCountTopology.java</code> file. You will see messages being emitted at the console for 10 seconds.</p>

            </div>
        </div>
      </div>
  </div>


	