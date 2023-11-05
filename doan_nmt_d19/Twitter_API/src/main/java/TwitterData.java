package src.main.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class TwitterData {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties twitterCredentials = new Properties();
        twitterCredentials.setProperty(TwitterSource.CONSUMER_KEY, "eG0KgXy1TTkfzgp7IPrWWIGuo");
        twitterCredentials.setProperty(TwitterSource.CONSUMER_SECRET, "xZSUzV8SMmg7mIpIpbkErPFTP2uvplr9l3UYpG6l2yQsHeVGuF");
        twitterCredentials.setProperty(TwitterSource.TOKEN, "1720088374076465152-jCU4Xoz3obyGyJWQKwY9shCnnig2Q7");
        twitterCredentials.setProperty(TwitterSource.TOKEN_SECRET, "SGUDK4DMZk6zUgoHkwROx3TRWESc798xqea0kZKZFkNn8");

        DataStream< String > twitterData = env.addSource(new TwitterSource(twitterCredentials));

        twitterData.flatMap(new TweetParser())
                .addSink(StreamingFileSink
                        .forRowFormat(new Path("/home/thanhnm95/tweet"),
                                new SimpleStringEncoder<Tuple2< String, Integer >>("UTF-8"))
                        .withRollingPolicy(DefaultRollingPolicy.builder().build())
                        .build());

        env.execute("Twitter Example");
    }

    public static class TweetParser implements FlatMapFunction< String, Tuple2 < String, Integer >> {

        public void flatMap(String value, Collector< Tuple2 < String, Integer >> out) throws Exception {
            ObjectMapper jsonParser = new ObjectMapper();
            JsonNode node = jsonParser.readValue(value, JsonNode.class);

            boolean isEnglish =
                    node.has("user") &&
                            node.get("user").has("lang") &&
                            node.get("user").get("lang").asText().equals("en");

            boolean hasText = node.has("text");

            if (isEnglish && hasText) {
                String tweet = node.get("text").asText();

                out.collect(new Tuple2 < String, Integer > (tweet, 1));
            }
        }
    }
}
