package org.apache.nutch.segment;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.LogUtil;

public class SegmentConveyor extends Configured implements Tool {
    public static final String DEFAULT_USER_AGENT = "ufl-webadmin-crawler-conveyor";
    public static final long DEFAULT_SLEEP_TIME = 500; // milliseconds
    private static final Log LOG = LogFactory.getLog(SegmentConveyor.class);

    public static class Map extends Mapper<Text, Writable, Text, NullWritable> {
        private URL[] services = null;
        private String userAgent = null;
        private long sleepTime = 0;

        public void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);

            Configuration conf = context.getConfiguration();

            String[] urls = conf.getStrings("segment.conveyor.urls");
            this.services = new URL[urls.length];
            for (int i = 0; i < urls.length; i++) {
                this.services[i] = new URL(urls[i]);
            }

            this.userAgent = conf.get("segment.conveyor.user.agent");
            this.sleepTime = conf.getLong("segment.conveyor.sleep.time",
                    DEFAULT_SLEEP_TIME);
        }

        public void map(Text key, Writable value, Context context)
                throws IOException, InterruptedException {
            Content content = (Content) value;

            String url = content.getUrl();
            String body = this.getBody(content);

            for (URL service : this.services) {
                try {
                    this.send(url, body, service);
                }
                catch (IOException e) {
                    e.printStackTrace(LogUtil.getWarnStream(LOG));
                }

                Thread.sleep(this.sleepTime);
            }

            context.write(key, NullWritable.get());
        }

        private void send(String url, String body, URL service)
                throws IOException {
            LOG.info("Sending [" + url + "] to [" + service + "]");

            HttpURLConnection conn = (HttpURLConnection) service.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            conn.setRequestProperty("User-Agent", this.userAgent);
            conn.setDoOutput(true);

            OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream());
            out.write(body);
            out.close();

            LOG.info("Got [" + conn.getResponseCode() + "] from [" + service + "]");
        }

        private String getBody(Content content)
                throws UnsupportedEncodingException {
            HashMap<String, String> parameters = this.getParameters(content);

            StringBuffer body = new StringBuffer();
            for (Entry<String, String> entry : parameters.entrySet()) {
                body.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                body.append("=");
                body.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
                body.append("&");
            }

            return body.substring(0, body.length() - 1);
        }

        private HashMap<String, String> getParameters(Content content) {
            HashMap<String, String> parameters = new HashMap<String, String>();

            parameters.put("url", content.getUrl());
            parameters.put("content", new String(content.getContent()));

            Metadata metadata = content.getMetadata();
            for (String key : metadata.names()) {
                String[] values = metadata.getValues(key);
                for (String value : values) {
                    parameters.put(key, value);
                }
            }

            return parameters;
        }
    }

    public int run(String segment, String[] urls, String userAgent, long sleepTime)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = this.getConf();
        conf.setStrings("segment.conveyor.urls", urls);
        conf.set("segment.conveyor.user.agent", userAgent);
        conf.setLong("segment.conveyor.sleep.time", sleepTime);

        Job job = new Job(conf);
        job.setJarByClass(SegmentConveyor.class);
        job.setJobName("segment.conveyor");

        FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(SegmentConveyor.Map.class);
        job.setReducerClass(Reducer.class);

        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        LOG.info("Beginning to convey segment [" + segment + "]");
        boolean success = job.waitForCompletion(true);
        LOG.info("Finished conveying segment [" + segment + "]");

        return success ? 0 : 1;
    }

    public int run(String[] args) throws IOException, InterruptedException,
            ClassNotFoundException {
        String segment = null;
        ArrayList<String> urls = new ArrayList<String>();
        String userAgent = DEFAULT_USER_AGENT;
        long sleepTime = DEFAULT_SLEEP_TIME;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-s")) {
                segment = args[++i];
            }
            else if (args[i].equals("-u")) {
                urls.add(args[++i]);
            }
            else if (args[i].equals("-a")) {
                userAgent = args[++i];
            }
            else if (args[i].equals("-t")) {
                sleepTime = Long.parseLong(args[++i]);
            }
        }

        if (segment == null || urls.size() <= 0) {
            usage();
            return 1;
        }

        String[] u = new String[urls.size()];
        return this.run(segment, urls.toArray(u), userAgent, sleepTime);
    }

    private static void usage() {
        System.err.println("Usage: SegmentConveyor "
                + "-s <segment_dir> -u <url> [-u <url> ...] [-a <user_agent>] [-t <sleep_time>]");
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new SegmentConveyor(), args);
        System.exit(rc);
    }
}
