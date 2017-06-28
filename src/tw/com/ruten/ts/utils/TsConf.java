package tw.com.ruten.ts.utils;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

public class TsConf {
	public static final String UUID_KEY = "job.conf.uuid";

	private TsConf() {}

	private static void setUUID(Configuration conf) {
		UUID uuid = UUID.randomUUID();
		conf.set(UUID_KEY, uuid.toString());
	}

	public static String getUUID(Configuration conf) {
		return conf.get(UUID_KEY);
	}

	public static Configuration create() {
		Configuration conf = new Configuration();
		setUUID(conf);
		addResource(conf);
		return conf;
	}

	private static Configuration addResource(Configuration conf) {
		conf.addResource("job-default.xml");
		conf.addResource("job-site.xml");
		return conf;
	}
}

