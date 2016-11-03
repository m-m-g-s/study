package mmgs.study.bigdata.spark.kwmatcher;

import mmgs.study.bigdata.spark.kwmatcher.conf.AppProperties;
import mmgs.study.bigdata.spark.kwmatcher.crawler.MeetupEventsCrawler;
import mmgs.study.bigdata.spark.kwmatcher.crawler.MeetupVenuesCrawler;
import mmgs.study.bigdata.spark.kwmatcher.crawler.SNCrawler;
import mmgs.study.bigdata.spark.kwmatcher.crawler.SNItem;
import mmgs.study.bigdata.spark.kwmatcher.model.TaggedClick;
import mmgs.study.bigdata.spark.kwmatcher.model.TaggedSN;
import mmgs.study.bigdata.spark.kwmatcher.model.WeightedKeyword;
import mmgs.study.bigdata.spark.kwmatcher.storage.HBaseStorage;
import mmgs.study.bigdata.spark.kwmatcher.storage.Storage;
import mmgs.study.bigdata.spark.kwmatcher.tokenizer.KeywordsExtractor;
import mmgs.study.bigdata.spark.kwmatcher.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

@ComponentScan
@EnableAutoConfiguration
public class KeywordsMatcher {

    private static final int TOP_AMT = 10;

    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(KeywordsMatcher.class).run(args);
        AppProperties props = ctx.getBean(AppProperties.class);

        String filterDate = args[1];
        // Initialize spark application
        SparkConf sparkConf = ctx.getBean(SparkConf.class)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .setMaster("local[*]")
                .registerKryoClasses(new Class[]{TaggedClick.class, TaggedSN.class});

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        // initialize meetup connection keys
        // Stub for keys
        // TODO: provide file path as a parameter
        JavaRDD<String> keysRDD = javaSparkContext.textFile(props.getMeetupProp().getPathToKeys());
        List<String> keysArr = keysRDD.collect();
        javaSparkContext.broadcast(keysArr);

        // initialize click dataset
        Storage storage = new HBaseStorage();
        JavaRDD<TaggedClick> taggedClicksRDD = storage.readTaggedClicks(sqlContext, props.getHbaseProp().getZookeeper(), filterDate);
        taggedClicksRDD = taggedClicksRDD.map(new Function<TaggedClick, TaggedClick>() {
            @Override
            public TaggedClick call(TaggedClick taggedClick) throws Exception {
                taggedClick.setDay(taggedClick.getDay().substring(0, 8));
                taggedClick.setTags(taggedClick.getTags().replace(",", " "));
                return taggedClick;
            }
        }).distinct();

        System.out.println(taggedClicksRDD.first());

        SNCrawler eventsCrawler = new MeetupEventsCrawler();
        SNCrawler venuesCrawler = new MeetupVenuesCrawler();

        // transformations
        // extract keywords for each click
        // TODO: make sure that keywords are delimited by spaces
        JavaPairRDD<TaggedClick, List<WeightedKeyword>> enrichedTaggedClicksRDD = taggedClicksRDD.flatMapToPair(new PairFlatMapFunction<TaggedClick, TaggedClick, List<WeightedKeyword>>() {
            private final Random random = new Random();

            @Override
            public Iterable<Tuple2<TaggedClick, List<WeightedKeyword>>> call(TaggedClick taggedClick) throws Exception {
                int i = random.nextInt(keysArr.size());
                String key = keysArr.get(i);
                List<SNItem> snItems = new ArrayList<>();
                snItems.addAll(eventsCrawler.extract(taggedClick, key));
                snItems.addAll(venuesCrawler.extract(taggedClick, key));
                Stream<List<WeightedKeyword>> listStream = snItems.stream().map(x -> {
                    try {
                        return KeywordsExtractor.getTopN(x.getDescription(), TOP_AMT);
                    } catch (IOException e) {
                        // TODO: handle exception properly
                        e.printStackTrace();
                        return new ArrayList<WeightedKeyword>();
                    }
                });
                return listStream.map(x -> new Tuple2<>(taggedClick, x))::iterator;
            }
        });

        System.out.println(enrichedTaggedClicksRDD.first());

        // combine clicks for identical
        JavaPairRDD<TaggedClick, List<WeightedKeyword>> aggregatedTaggedClicksRDD = enrichedTaggedClicksRDD.filter(new Function<Tuple2<TaggedClick, List<WeightedKeyword>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<TaggedClick, List<WeightedKeyword>> taggedClickListTuple2) throws Exception {
                return taggedClickListTuple2._2().size() > 0 ? true : false;
            }
        }).reduceByKey((Function2<List<WeightedKeyword>, List<WeightedKeyword>, List<WeightedKeyword>>) (keywords1, keywords2) -> {
            List<WeightedKeyword> keywords = new ArrayList<>();
            keywords.addAll(keywords1);
            keywords.addAll(keywords2);
            return keywords;
        });

//        System.out.println(aggregatedTaggedClicksRDD.first());
        // convert to final dataset structure
        JavaRDD<TaggedSN> snTaggedRDD = aggregatedTaggedClicksRDD.map(
                (Function<Tuple2<TaggedClick, List<WeightedKeyword>>, TaggedSN>) taggedClickListTuple2
                        -> new TaggedSN(taggedClickListTuple2._1(), Utils.getTopN(taggedClickListTuple2._2(), TOP_AMT)));

        HiveContext hiveContext = new HiveContext(javaSparkContext);
        DataFrame dataFrame = hiveContext.createDataFrame(snTaggedRDD.rdd(), TaggedSN.class);

        // debugging
        dataFrame.show();

        dataFrame.registerTempTable(props.getHiveProp().getTableSavePath() + filterDate);

        // save as hive table
        // TODO: generate valid file name
        dataFrame.write().format("orc").insertInto(props.getHiveProp().getTableSavePath());
    }
}

