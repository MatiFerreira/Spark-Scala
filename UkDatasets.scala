// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

// COMMAND ----------

val distric_counsin = "dbfs:/FileStore/district_councils.csv"
val london_boroughs = "dbfs:/FileStore/london_boroughs.csv"
val metropolitan_Districts = "dbfs:/FileStore/metropolitan_districts.csv"
val unitary_authorities = "dbfs:/FileStore/unitary_authorities.csv"
// property
val property_avg_price = "dbfs:/FileStore/property_avg_price.csv"
val property_sales_volume = "dbfs:/FileStore/property_sales_volume.csv"
val spark = SparkSession.builder().getOrCreate()

// COMMAND ----------

// esto es un metodo para cargar los dataframe

def cargarCSV(path: String): DataFrame = {
  spark.read.options(Map("header"->"true","inferSchema"->"true",
                            "sep"->",","encoding" -> "utf-8")).csv(path)
}

// COMMAND ----------

// Londres district cargado en formato spark
val distric_counsin_df = cargarCSV(distric_counsin)
val london_boroughs_df = cargarCSV(london_boroughs)
val metropolitan_Districts_df = cargarCSV(metropolitan_Districts)
val unitary_authorities_df = cargarCSV(unitary_authorities)

// COMMAND ----------

val property_avg_price_df = cargarCSV(property_avg_price)
val property_sales_volume_df = cargarCSV(property_sales_volume)

// COMMAND ----------

val extract_councils = (df1 : DataFrame,df2: DataFrame,df3: DataFrame,df4: DataFrame) =>{
    val newdf1 = df1.withColumn("council_type",lit("District Council"))
    val newdf2 = df2.withColumn("council_type",lit("London Borough"))
    val newdf3 = df3.withColumn("council_type",lit("Metropolitan District"))
    val newdf4 = df4.withColumn("council_type",lit("Unitary Authority"))
    newdf1.union(newdf2).union(newdf3).union(newdf4)
}

// COMMAND ----------

val dfUnion = extract_councils(distric_counsin_df,london_boroughs_df,metropolitan_Districts_df,unitary_authorities_df)
display(dfUnion)

// COMMAND ----------

val extract_avg_price = (df1:DataFrame) => {
  val newdf = df1.withColumn("council",col("local_authority")).select("council","avg_price_nov_2019")
  newdf
}

// COMMAND ----------

val avg_precio = extract_avg_price(property_avg_price_df)
avg_precio.show()

// COMMAND ----------

val extract_sales_volume = (df1 : DataFrame) => {
    val newdf = df1.withColumn("council",col("local_authority")).select("council","sales_volume_sep_2019")
    newdf
}

// COMMAND ----------

val property_sales = extract_sales_volume(property_sales_volume_df)
display(property_sales)

// COMMAND ----------

val transform = (df1:DataFrame,df2:DataFrame,df3:DataFrame) =>{
   df1.join(df2, df1("council") === df2("council"), "left").drop(df2("council"))
   .join(df3,df1("council")===df3("council"),"left").drop(df3("council"))
}

// COMMAND ----------

val agrupacion = transform(dfUnion,avg_precio,property_sales)
agrupacion.show()
