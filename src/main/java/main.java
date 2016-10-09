import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import java.io.Serializable;
import java.util.List;


class Main
{
    public static void main(String[]args)
    {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]").getOrCreate();

        Dataset<Row> df   = spark.read().csv("/home/edge7/workspace2/everis2/data/ex.csv");
        Dataset<Row> df2 = df.withColumnRenamed("_c0", "name").withColumnRenamed("_c1", "age");
        df2.printSchema();
        df2.show();
        Dataset<Row> filter = df2.filter(row -> {
            String age = row.getAs("age");
            try {
                Integer.parseInt(age);
            } catch (NumberFormatException e) {
                System.out.println("Wrong number");
                return false;
            }
            return true;
        });
        Dataset<Row> filter2 = filter.withColumn("age2", filter.col("age").cast("int")).drop("age").withColumnRenamed("age2", "age");
        Dataset<Row> filter3 = filter2.select("name", "age").filter("age > 30");
        List<Row> rowJavaRDD = filter3.toJavaRDD().collect();
        rowJavaRDD.forEach((i) -> System.out.println(i));
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> as = filter3.as(personEncoder);
        List<Person> persons = as.collectAsList();
        System.out.println("enrico");


    }
}