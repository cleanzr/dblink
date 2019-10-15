# Installing Java 8+ on macOS

We recommend using the [AdoptOpenJDK](https://adoptopenjdk.net) Java 
distribution on macOS. 

It can be installed using the [Homebrew](https://brew.sh/) package manager. 
Simply run the following commands in a terminal:
```bash
$ brew tap AdoptOpenJDK/openjdk
$ brew cask install adoptopenjdk8
```

Note that it's possible to have multiple versions of Java installed in 
parallel. If you run 
```bash
$ java -version
```
and don't see a version number like 1.8.x or 8.x, then you'll need to 
manually select Java 8.

To do this temporarily in a terminal, run
```
$ export JAVA_HOME=$(/usr/libexec/java_home -v1.8)
```
All references to Java within this session will then make use of Java 8.
