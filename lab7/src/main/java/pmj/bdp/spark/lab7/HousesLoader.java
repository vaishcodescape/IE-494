package pmj.bdp.spark.lab7;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public final class HousesLoader {

    private HousesLoader() {
    }

    public static Dataset<Row> load(SparkSession spark, Path csv) {
        if (!Files.isRegularFile(csv)) {
            throw new IllegalArgumentException("houses csv not found: " + csv);
        }
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("x1", DataTypes.DoubleType, false)
                .add("x2", DataTypes.DoubleType, false)
                .add("x3", DataTypes.DoubleType, false)
                .add("x4", DataTypes.DoubleType, false)
                .add("x5", DataTypes.DoubleType, false)
                .add("x6", DataTypes.DoubleType, false)
                .add("y", DataTypes.DoubleType, false);
        return spark.read()
                .option("header", "true")
                .schema(schema)
                .csv(csv.toString());
    }
}
