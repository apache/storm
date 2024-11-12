# Apache Storm Development Dockerfile

This Dockerfile provides a complete development environment for Apache Storm, aligning with the GitHub Actions CI setup 
for building and testing various modules of Apache Storm. 

It installs and configures Java, Maven, Python, Node.js, and Ruby, allowing you to run builds and tests for different Storm modules in a containerized environment.

This is especially useful for people on Mac OSX or Windows. It also provides a consistent environment for all developers.

## Usage

Build it by running:

```bash
docker build -t storm-dev .
```

## Run a build

```bash
docker run -it \
--name storm-dev \
-e MAVEN_OPTS="-Xmx768m -XX:ReservedCodeCacheSize=64m -Xss2048k" \
-v $(pwd)/m2:/home/ubuntu/.m2 \
-v $(pwd):/opt/project \
-w /opt/project \
storm-dev
```

Advanced cases such as remote debugging are also possible. Just map the debugger port and start maven accordingly.