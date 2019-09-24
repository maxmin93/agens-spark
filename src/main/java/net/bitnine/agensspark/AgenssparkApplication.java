package net.bitnine.agensspark;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootApplication
public class AgenssparkApplication implements ApplicationRunner {

	static final Logger LOGGER = LoggerFactory.getLogger(AgenssparkApplication.class);

	String master = "local";
	String appName = "agensspark";
	String sparkHome = "/Users/bgmin/Servers/spark-2.4.3-bin-hadoop2.7";
	String readResource = "elasticvertex";
	String writeResource = "sparktemp";
//	String extraCP = "/Users/bgmin/Servers/es-hadoop/dist/elasticsearch-hadoop-7.3.1.jar"
//				+ ",/Users/bgmin/Servers/es-hadoop/dist/elasticsearch-spark-20_2.11-7.3.1.jar";

	public static void main(String[] args) {
		SpringApplication.run(AgenssparkApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments applicationArguments) throws Exception {

		LOGGER.info("SpringBootSpark To Elasticsearch Application: {}, {}, {}, {}"
				, master, appName, sparkHome, readResource);

		// When run by java -jar, Fail
		javaEsSparkTest();

		// When run by java -jar, Fail also
		// sparkSessionTest();
	}

	private SparkConf sparkConf(){
		return new SparkConf()
				.setAppName(appName)
				.setSparkHome(sparkHome)
				.setMaster(master)
				.set("spark.executor.memory", "2g")
				.set("spark.driver.memory", "2g")
				.set("spark.eventLog.enabled","false")
				;
	}

	private Map<String,String> esConf(){
		Map<String,String> conf = new HashMap<>();
		conf.put("es.nodes", "27.117.163.21");
		conf.put("es.port", "15619");
		conf.put("es.nodes.wan.only", "true");
		conf.put("es.mapping.id", "id");
		conf.put("es.write.operation", "upsert");
		conf.put("es.index.auto.create", "true");
		conf.put("es.scroll.size", "10000");
		conf.put("es.mapping.exclude", "removed,*.present");
		return ImmutableMap.copyOf(conf);
	}

	private void sparkSessionTest(){
		final SparkConf conf = sparkConf();
		final SparkSession sql = SparkSession.builder().config(conf).getOrCreate();
		sql.sparkContext().setLogLevel("ERROR");

		String datasource = "modern";
		Dataset<Row> readDF = sql.read().format("es").options(esConf())
				.option("es.query", "?q=datasource:"+datasource)
				.load(readResource);

		readDF.printSchema();
		readDF.show();

		// for DEBUG
		System.out.println("\n\n==================================\n");
		System.out.println("\n**count = "+readDF.count());
	}

	private void javaEsSparkTest(){
		final SparkConf conf = sparkConf();
		final JavaSparkContext jsc = new JavaSparkContext(conf);

		String datasource = "modern";
		Map<String,String> cfgEsHadoop = new HashMap<>(esConf());
		cfgEsHadoop.put("es.resource", readResource);
		cfgEsHadoop.put("es.query", "?q=datasource:"+datasource);

		JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, cfgEsHadoop);

		// for DEBUG
		System.out.println("\n\n==================================\n");
		System.out.println("\n**count = "+esRDD.count());

		Map<String, Map<String, Object>> rddMap = esRDD.collectAsMap();
		String mapAsString = rddMap.keySet().stream()
				.map(key -> key + "=[ " + rddMap.get(key).toString() +" ]")
				.collect(Collectors.joining(", ", "{", "}"));
		System.out.println("\n  ==> "+mapAsString);
	}
}
