import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.*;;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Referenes for help:
 * https://stackoverflow.com/questions/25362942/parse-csv-as-dataframe-dataset-with-apache-spark-and-java
 * https://www.cloudera.com/tutorials/setting-up-a-spark-development-environment-with-java/.html
 * https://spark.apache.org/docs/2.1.0/sql-programming-guide.html#interoperating-with-rdds
 * https://github.com/apache/spark/tree/master/examples/src/main/java/org/apache/spark/examples
 */
public class DBRunner {
    private static final String FILE_2006 = "2006.txt";
    private static final String FILE_2007 = "2007.txt";
    private static final String FILE_2008 = "2008.txt";
    private static final String FILE_2009 = "2009.txt";
    private static final String LOCATIONS_FILE = "WeatherStationLocations.csv";
    private static final String DUMMY_CSV = "dummycsv.csv";
    private static final String DUMMY_TXT = "dummy.txt";
    private static String[] recordingsHeaders = null;

    public static void main(String[] args){
        disableWarningandInfo(); //makes it easier to read
        if (args.length != 3)
            System.out.println("Did not format arguments as <locations_folder_path> " +
                    "<recordings_folder_path> <output_folderpath>");
        else {
            String locationsFolder = formatDir(args[0]);
            String recordingsFolder = formatDir(args[1]);
            String outputFolder = formatDir(args[2]);

            SparkSession spark = SparkSession
                    .builder()
                    .appName("US Weather Stations")
                    .config("spark.master", "local")
                    .getOrCreate();
            SQLContext context = new SQLContext(spark);

//        System.out.println("\nLocations Dataset");
            Dataset<Row> locationsDS = createLocationsDS(context,
                    locationsFolder + LOCATIONS_FILE);
//        Dataset<Row> locationsDS = createLocationsDS(context,DUMMY_CSV);
//        locationsDS.printSchema();
//        locationsDS.show();
//        locationsDS.describe("USAF").show();
//        System.out.println("Count: " + locationsDS.count());

//        System.out.println("\nRecordings Dataset");
            Dataset<Row> recordings2006 = createRecordingsDS(context, recordingsFolder
                    + FILE_2006);
            Dataset<Row> recordings2007 = createRecordingsDS(context, recordingsFolder
                    + FILE_2007);
            Dataset<Row> recordings2008 = createRecordingsDS(context, recordingsFolder
                    + FILE_2008);
            Dataset<Row> recordings2009 = createRecordingsDS(context, recordingsFolder
                    + FILE_2009);

            Dataset<Row> recordingsDS = recordings2006.union(recordings2007)
                    .union(recordings2008).union(recordings2009).toDF();
//        System.out.println("Count: " + recordingsDS.count());
//        recordingsDS.show();

            runTasks(context, locationsDS, recordingsDS, outputFolder);
            spark.stop();
        }
    }

    private static void runTasks(SQLContext context, Dataset<Row> locationsDS,
                          Dataset<Row> recordingsDS, String outputDir) {
        //converting Scala wrapped array to String for saving purposes
        context.udf().register("wrappedArrToStr", new UDF1<WrappedArray<String>,
                String>() {
            @Override
            public String call(WrappedArray<String> precip) throws Exception {
                System.out.println(precip.mkString(","));
                return precip.mkString();
            }
        },DataTypes.StringType);

        //Task 1
        //=========================================================================
        System.out.println("\nTask 1");
        long startTime = System.currentTimeMillis();
        Column ctryCol = locationsDS.col("CTRY");
        Column stateCol = locationsDS.col("STATE").alias("State");
        Column idCol = locationsDS.col("USAF");
        Dataset<Row> task1 = locationsDS.filter(ctryCol.equalTo("US"))
                .filter(stateCol.isNotNull())
                .groupBy(stateCol).agg(collect_list(idCol).alias("USAFs"))
                .toDF();

        task1.show(55);
        long endTime = System.currentTimeMillis();
        System.out.println("Time to perform Task 1: " + (endTime - startTime)/1000.0 + "s");

        //Task 2
        //=========================================================================
        System.out.println("\nTask 2");

        context.udf().register("combinedPrecip", new UDF2<String, Double, String>() {
            @Override
            public String call(String mth, Double precip) throws Exception {
                return mth + " " + precip;
            }
        },DataTypes.StringType);

        startTime = System.currentTimeMillis();
        Column recordingUSAF = recordingsDS.col("STN");
        Dataset<Row> explodeDS = task1.withColumn("USAF", explode(task1.col("USAFs")))
                .toDF();
//        explodeDS.show();

        Dataset<Row> recordingsJoin = explodeDS.join(recordingsDS)
                .where(explodeDS.col("USAF").equalTo(recordingUSAF))
                .drop(task1.col("USAFs"))
                .drop(recordingsDS.col("PRCP"))
                .drop(recordingsDS.col("YEARMODA"))
                .drop(recordingsDS.col("WBAN")).toDF()
                .filter(recordingsDS.col("PrecipCalc").notEqual(-1));
//        recordingsJoin.show();

        Dataset<Row> fullPrecipList = recordingsJoin.groupBy(
                recordingsJoin.col("State").alias("St"),
                recordingsJoin.col("Month"))
                .agg(avg("PrecipCalc").alias("Average Precipitation")).toDF();
//        fullPrecipList.show();

        Column month = fullPrecipList.col("Month");
        Column  avgPrecip = fullPrecipList.col("Average Precipitation");
        Column st = fullPrecipList.col("St").alias("State");
        Dataset<Row> task2 = fullPrecipList
                .withColumn("Precip",
                        functions.callUDF("combinedPrecip", month, avgPrecip))
                .groupBy(st).agg(collect_list("Precip").alias("Precip List"))
                .toDF();

        task2.show(55);
        endTime = System.currentTimeMillis();
        System.out.println("Time to perform Task 2: " + (endTime - startTime)/1000.0 + "s");

        //Task 3
        //=========================================================================
        System.out.println("\nTask 3");
        context.udf().register("getLowestPrecip", new UDF1<WrappedArray<String>,
                String>() {
            @Override
            public String call(WrappedArray<String> precip) throws Exception {
                ArrayList<String[]> result = new ArrayList<>();
                scala.collection.immutable.List<String> convert = precip.toList();

                int lowestIndex = 0;
                String[] split = convert.apply(0).split(" ");
                double lowestPrecip = Double.parseDouble(split[1]);
                for (int i = 1; i < convert.size(); i++) {
                    split = convert.apply(i).split(" ");
                    double currentPrecip = Double.parseDouble(split[1]);
                    if (currentPrecip < lowestPrecip) {
                        lowestPrecip = currentPrecip;
                        lowestIndex = i;
                    }
                }
                return precip.apply(lowestIndex);
            }
        },DataTypes.StringType);

        context.udf().register("getHighestPrecip", new UDF1<WrappedArray<String>,
                String>() {
            @Override
            public String call(WrappedArray<String> precip) throws Exception {
                ArrayList<String[]> result = new ArrayList<>();
                scala.collection.immutable.List<String> convert = precip.toList();

                int highestIndex = 0;
                String[] split = convert.apply(0).split(" ");
                double highestPrecip = Double.parseDouble(split[1]);
                for (int i = 1; i < convert.size(); i++) {
                    split = convert.apply(i).split(" ");
                    double currentPrecip = Double.parseDouble(split[1]);
                    if (currentPrecip > highestPrecip) {
                        highestPrecip = currentPrecip;
                        highestIndex = i;
                    }
                }
                return precip.apply(highestIndex);
            }
        },DataTypes.StringType);

        startTime = System.currentTimeMillis();

        Dataset<Row> task3NoSplit = task2.withColumn("Low",
                functions.callUDF("getLowestPrecip", task2.col("Precip List")))
                .withColumn("High",
                        functions.callUDF("getHighestPrecip", task2.col("Precip List")))
                .drop("Precip List").toDF();

        Dataset<Row> task3 = task3NoSplit.withColumn("Lowest Month",
                split(task3NoSplit.col("Low"), " ").getItem(0))
                .withColumn("Lowest Precipitation",
                        split(task3NoSplit.col("Low"), " ").getItem(1))
                .withColumn("Highest Month",
                        split(task3NoSplit.col("High"), " ").getItem(0))
                .withColumn("Highest Precipitation",
                        split(task3NoSplit.col("High"), " ").getItem(1))
                .drop(task3NoSplit.col("Low")).drop(task3NoSplit.col("High"));


        task3.show(55);
        endTime = System.currentTimeMillis();
        System.out.println("Time to perform Task 3: " + (endTime - startTime)/1000.0 + "s");

        //Task 4
        //=========================================================================
        System.out.println("\nTask 4");
        startTime = System.currentTimeMillis();
        Dataset<Row> task4 = task3.withColumn("Difference",expr(
                "`Highest Precipitation` - `Lowest Precipitation`"))
                .sort("Difference");

        task4.show(55);
        endTime = System.currentTimeMillis();
        System.out.println("Time to perform Task 4: " + (endTime - startTime)/1000.0 + "s");

        task4.coalesce(1).write().option("header", "true")
                .format("csv").save(outputDir + "result csv files");
    }

    private static String formatDir(String dir) {
        if (dir.indexOf(dir.length()-1) != '/')
            return dir + '/';
        return dir;
    }

    private static String[] processRecordingsHeader(String header, String splitRegex) {
        String[] split = header.split(splitRegex);
        String calculatedPrecipTitle = "PrecipCalc";
        String id = "STN";
        String month = "Month";
        String[] recordingsHeader = {id, split[1], split[2], month, split[13],
                calculatedPrecipTitle};
        return recordingsHeader;
    }

    private static double calcPrecipitation(String precip) {
        final String[] KEYS = {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        int lastIndex = precip.length() - 1;
        String key = precip.substring(lastIndex);
        double value = Double.parseDouble(precip.substring(0, lastIndex));

        if(key.equals(KEYS[0]))
            return value * 4;
        if(key.equals(KEYS[1]) || key.equals(KEYS[4]))
            return value * 2;
        if(key.equals(KEYS[2])) {
            double multiplier = 24.0/18.0;
            return value * multiplier;
        }
        if(key.equals(KEYS[3])|| key.equals(KEYS[5])|| key.equals(KEYS[6]))
            return value;
        if(key.equals(KEYS[7]) || key.equals(KEYS[8]))
            return 0;
        return -1;
    }

    private static String getMonth(String num) {
        switch(num) {
            case "01": return "January";
            case "02": return "February";
            case "03": return "March";
            case "04": return "April";
            case "05": return "May";
            case "06": return "June";
            case "07": return "July";
            case "08": return "August";
            case "09": return "September";
            case "10": return "October";
            case "11": return "November";
            case "12": return "December";
            default: return null;
        }
    }


    private static Dataset<Row> createRecordingsDS(SQLContext context, String filename) {
        String spacesRegex = "\\s+";
        String templateName = filename.substring(0, filename.indexOf('.'));
        //data types as defined by data
        DataType[] types = {DataTypes.StringType, DataTypes.IntegerType,
                DataTypes.StringType, DataTypes.StringType, DataTypes.StringType,
                DataTypes.DoubleType};

        //remove the headers of the original file
        JavaRDD<String> data = context.read().textFile(filename).javaRDD();
        String header = data.first();
        data = data.filter(row -> !row.equals(header));

        //generate the final headers once since all the recording headers are the same
        if (recordingsHeaders == null)
            recordingsHeaders = processRecordingsHeader(header, spacesRegex);

        //create schema with headers created
        List<StructField> fields = new ArrayList<>();
        for (int i = 0; i < recordingsHeaders.length; i++) {
            StructField field = DataTypes.createStructField(recordingsHeaders[i],
                    types[i], false);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        //create and fill the dataset object with data
        JavaRDD<Row> rowRDD = data.map((Function<String, Row>) line -> {
            String[] attributes = line.split(spacesRegex);
            return RowFactory.create(attributes[0], Integer.parseInt(attributes[1]),
                    attributes[2], getMonth(attributes[2].substring(4, 6)),
                    attributes[19], calcPrecipitation(attributes[19]));
        });
        Dataset<Row> recordings = context.createDataFrame(rowRDD, schema);

        return recordings;
    }

    private static Dataset<Row> createLocationsDS(SQLContext context, String filename) {
        return context.read().option("header", true).csv(filename);
    }

    public static void disableWarningandInfo() {
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);
        System.err.close();
        System.setErr(System.out);
    }
}