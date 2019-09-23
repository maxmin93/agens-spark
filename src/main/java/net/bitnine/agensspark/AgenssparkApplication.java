package net.bitnine.agensspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootApplication
public class AgenssparkApplication implements ApplicationRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(AgenssparkApplication.class);

	private String appName = "agensspark";
	private String nodes = "27.117.163.21";
	private String port = "15619";
	private String sparkHome = "/Users/bgmin/Servers/spark";
	private String resource = "elasticvertex";
	private String extraCP = "/Users/bgmin/Servers/es-hadoop/dist/elasticsearch-hadoop-7.3.1.jar"
				+ ",/Users/bgmin/Servers/es-hadoop/dist/elasticsearch-spark-20_2.11-7.3.1.jar";

	public static void main(String[] args) {
		SpringApplication.run(AgenssparkApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments applicationArguments) throws Exception {

		LOGGER.info("SpringBootSpark To Elasticsearch Application: {}, {}, {}, {}"
				, appName, nodes, port, sparkHome);

		String master = "local[*]";
		SparkConf conf = new SparkConf()
				.setAppName(appName)
				.setSparkHome(sparkHome)
				.setMaster(master)
                .set("spark.executor.memory", "2g")
				.set("spark.driver.memory", "2g")
				.set("spark.eventLog.enabled","false")
				.set("spark.driver.extraClassPath", extraCP)
				// es 접속정보
				.set("es.nodes.wan.only", "true")
				.set("es.nodes", nodes)
				.set("es.port", port)
				.set("es.mapping.id", "id")
				.set("es.write.operation", "upsert")
				.set("es.index.read.missing.as.empty", "true");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		// for DEBUG
		System.out.println("\n\n==================================\n");
		System.out.println(Arrays.stream(jsc.getConf().getAll()).map(t->{
			return t._1()+"="+t._2();
		}).collect(Collectors.joining(", ")));

		JavaPairRDD<String, Map<String, Object>> esRDD =
				JavaEsSpark.esRDD(jsc, resource, "?q=datasource:sample");
		System.out.println("\n**count = "+esRDD.count());

	}
}
