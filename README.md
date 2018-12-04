# how to unit test

	./mvnw clean test

# how to run JMH

	./mvnw clean package -DskipTests
	java -jar perf/target/benchmarks.jar
	
### JMH hints
    Run only one iteration of JMH tests
    java -jar perf/target/benchmarks.jar -f 1 -wi 1 -i 1