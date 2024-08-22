// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable
import spark.sqlContext.implicits._
import org.apache.spark.sql.functions._

// COMMAND ----------

val spark = SparkSession.builder().getOrCreate()

// COMMAND ----------

var data = spark.read.format("csv").option("header","True").option("sep",";")
.load("dbfs:/FileStore/shared_uploads/matthg7@outlook.com/estadisticas202404-2.csv")
data.write.format("delta").save("dbfs:/FileStore/shared_uploads/matthg7@outlook.com/estadisticas202404-2.delta")

// COMMAND ----------

var data = spark.read.format("delta").option("header","True").option("sep",";").load("dbfs:/FileStore/shared_uploads/matthg7@outlook.com/estadisticas202404-2.delta")

// COMMAND ----------

/// QUITAMOS LOS VALORES CON ESPACIOS.
val columnas_to_trim = List("DESC_DISTRITO", "DESC_BARRIO", "COD_EDAD_INT", "FX_DATOS_INI", "FX_DATOS_FIN")
for (i <- columnas_to_trim){
   data = data.withColumn(i,trim(col(i)))
}

// COMMAND ----------

val otherResult = data.groupBy(col("DESC_DISTRITO"),col("DESC_BARRIO")).agg(
  sum(when(col("ESPANOLESHOMBRES").isNull, 0).otherwise(col("ESPANOLESHOMBRES"))).alias(
            "SUM_ESPANOLESHOMBRES"),
            sum(when(col("ESPANOLESMUJERES").isNull, 0).otherwise(col("ESPANOLESMUJERES"))).alias(
            "SUM_ESPANOLESMUJERES"),
          sum(when(col("EXTRANJEROSHOMBRES").isNull, 0).otherwise(col("EXTRANJEROSHOMBRES"))).alias(
            "SUM_EXTRANJEROSHOMBRES"),
          sum(when(col("EXTRANJEROSMUJERES").isNull, 0).otherwise(col("EXTRANJEROSMUJERES"))).alias(
            "SUM_EXTRANJEROSMUJERES")
    ).orderBy(col("SUM_EXTRANJEROSMUJERES").desc).orderBy(col("SUM_EXTRANJEROSHOMBRES").desc).show(5)

// COMMAND ----------

data.createOrReplaceTempView("vistaTemData")
spark.table("vistaTemData")

// COMMAND ----------

val numData = data.withColumn("COD_EDAD_INT", regexp_extract(col("COD_EDAD_INT"), "(\\d+)", 1))
val cleanData = numData.withColumn("COD_EDAD_INT",col("COD_EDAD_INT").cast("Integer"))

// COMMAND ----------

val bara = cleanData.select(col("DESC_DISTRITO"),col("ESPANOLESMUJERES")).where(col("DESC_DISTRITO").equalTo("BARAJAS"))
bara.show()

// COMMAND ----------

val pivot = cleanData
  .groupBy(col("COD_EDAD_INT"))
  .pivot(col("DESC_DISTRITO"),Seq("BARAJAS","CENTRO","RETIRO"))
  .agg(sum("ESPANOLESMUJERES"))
  .orderBy(col("COD_EDAD_INT"))

// COMMAND ----------

pivot.show()

// COMMAND ----------

val porcent = pivot.select(
    col("*"),
    round(col("BARAJAS") + col("CENTRO") + col("RETIRO"), 2).alias("suma_esp_mujeres"),
    round(col("BARAJAS") / col("suma_esp_mujeres") * 100, 2).alias("%BARAJAS"),
    round(col("CENTRO") / col("suma_esp_mujeres") * 100, 2).alias("%CENTRO"),
    round(col("RETIRO") / col("suma_esp_mujeres") * 100, 2).alias("%RETIRO")
)

// COMMAND ----------

porcent.show()

// COMMAND ----------

cleanData
  .write
  .partitionBy("DESC_DISTRITO", "DESC_BARRIO")
  .format("delta")
  .mode("overwrite")
  .save("dbfs:/FileStore/shared_uploads/matthg7@outlook.com/newDelta")

// COMMAND ----------

cleanData.write.partitionBy("DESC_DISTRITO","DESC_BARRIO")
.format("parquet").mode("overwrite").save("dbfs:/FileStore/shared_uploads/matthg7@outlook.com/newParquet")

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE pet_procedures;
// MAGIC CREATE TABLE pet_procedures (
// MAGIC     PetID STRING,
// MAGIC     Date STRING,
// MAGIC     ProcedureType STRING,
// MAGIC     ProcedureSubCode STRING
// MAGIC )
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY ","
// MAGIC STORED AS TEXTFILE;
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC LOAD DATA INPATH "dbfs:/FileStore/Pets.csv" INTO TABLE pet_procedures

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE owner_details (
// MAGIC     OwnerID STRING,
// MAGIC     Name STRING,
// MAGIC     Surname STRING,
// MAGIC     StreetAddress STRING,
// MAGIC     City STRING,
// MAGIC     StateAbbreviation STRING,
// MAGIC     StateFullName STRING,
// MAGIC     ZipCode STRING
// MAGIC )
// MAGIC ROW FORMAT DELIMITED
// MAGIC FIELDS TERMINATED BY ","
// MAGIC STORED AS TEXTFILE;

// COMMAND ----------

// MAGIC %sql
// MAGIC LOAD DATA INPATH "dbfs:/FileStore/Owners.csv" INTO TABLE owner_details

// COMMAND ----------

val dframeOwners = spark.table("owner_details")
val dframePets = spark.table("pet_procedures")

// COMMAND ----------

display(dframeOwners)

// COMMAND ----------

display(dframePets)

// COMMAND ----------

// Crear una instancia de SparkSession
val spark = SparkSession.builder()
  .appName("Importar Datos desde Parquet en Spark")
  .getOrCreate()

// Leer los datos desde un archivo Parquet
val df = spark.read.parquet("ruta/al/archivo.parquet")

// Mostrar el DataFrame
df.show()

// Cerrar la sesión de Spark al finalizar
spark.stop()

