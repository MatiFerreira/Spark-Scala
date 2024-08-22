// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

// COMMAND ----------

val PATH = "/FileStore/US_PopAgeStruct_20230713030811.csv"

val spark = SparkSession.builder().getOrCreate()

// COMMAND ----------

val newCSV = spark.read.options(Map("header" ->"true","inferSchema" ->"true")).csv(PATH)

// COMMAND ----------

display(newCSV)

// COMMAND ----------

newCSV.printSchema

// COMMAND ----------

// contar los registros totales del df

val totalRegistro = newCSV.count

// COMMAND ----------

val registro2020 = newCSV.filter(col("Year") === 2020)
display(registro2020)

// COMMAND ----------

val agrupacionEconomy = newCSV.groupBy(col("Economy")).count().orderBy(col("count").asc)
agrupacionEconomy.show()

// COMMAND ----------

// Calcular el promedio del valor absoluto (Absolute value in thousands) por cada Sex y mostrar los resultados
val promedio = newCSV.groupBy("Sex Label").agg(avg("Absolute value in thousands").as("PROMEDIO"))
promedio.show()

// COMMAND ----------

promedio.printSchema

// COMMAND ----------

// Añadir una nueva columna que indique si el valor absoluto está por encima de 1000 o no

val newColumn = promedio.withColumn("PROMEDIO>1000",when(col("PROMEDIO")>1000,"ES > 1000").otherwise("ES < 1000"))
newColumn.show()

// COMMAND ----------

// Filtrar los registros que tienen valores nulos en alguna de las columnas
val noNullCount = newCSV.filter(row => row.anyNull).count()

// COMMAND ----------

// Reemplazar los valores nulos en la columna Absolute value in thousands con la media de esa columna
val media = newCSV.select(mean(col("Absolute value in thousands"))).first().getDouble(0)
val cleanCSV = newCSV.withColumn("Absolute value in thousands",when(col("Absolute value in thousands").isNull,media).otherwise(col("Absolute value in thousands")))

display(cleanCSV)

// COMMAND ----------

// Ordenar los datos por Absolute value in thousands en orden descendente
val orden = cleanCSV.orderBy(col("Absolute value in thousands").desc)
display(orden)

// COMMAND ----------

// Crear una columna nueva que convierta el valor absoluto de miles a unidades (multiplicando por 1000)
val newCol = cleanCSV.withColumn("VALOR ABSOLUTO X1000",col("Absolute value in thousands")*1000)
display(newCol)

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
// Calcular la media móvil del valor absoluto (Absolute value in thousands) por Economy y Sex usando una ventana de 3 años
val windowSpec = Window
  .partitionBy("Economy", "Sex")
  .orderBy("Year")
  .rowsBetween(-2, 0) // Ventana de 3 años, incluyendo el año actual y los dos anteriores

  val mediaMovil = cleanCSV.withColumn("MEDIA_ABSOLUTA",avg(col("Absolute value in thousands")).over(windowSpec))
  display(mediaMovil)

// COMMAND ----------

// Unir el DataFrame consigo mismo para comparar los valores de Absolute value in thousands de cada Economy y Sex con el año anterior
val yearOne = cleanCSV.withColumnRenamed("Year","yearOne").withColumnRenamed("Absolute value in thousands","VALOR 1")
val yearTwo = cleanCSV.withColumnRenamed("Year", "yearTwo")
                      .withColumnRenamed("Absolute value in thousands", "VALOR 2")
                      .withColumn("yearTwo", col("yearTwo") - 1)

val comparacion = yearOne.join(yearTwo,yearOne("Economy") === yearTwo("Economy") && yearOne("Sex") === yearTwo("Sex"),"inner")

val resultDF = comparacion.select(yearOne("Sex"),yearOne("Economy"),yearOne("YearOne"),yearOne("VALOR 1"),yearTwo("yearTwo"),yearTwo("VALOR 2"))

display(resultDF)


// COMMAND ----------

// Pivotar los datos para obtener una tabla donde las columnas sean los años y las filas sean las combinaciones de Economy y Sex, mostrando los valores absolutos
// Combinar las columnas 'Economy' y 'Sex' en una sola columna 'Economy_Sex'
val combinedDF = cleanCSV.withColumn("Economy_Sex", concat($"Economy", lit("_"), $"Sex"))
val tablaPivot = combinedDF.groupBy("Economy_Sex").pivot("Year").sum("Absolute value in thousands")
display(tablaPivot)

// COMMAND ----------

// Definir una UDF para clasificar los valores absolutos en categorías (Low, Medium, High) y aplicarla a una nueva columna
// Define a function that takes a Double as input and returns a String
import org.apache.spark.sql.functions.udf

// Definir la función de categoría como una función literal
val categoryUDF = udf((value: Double) => {
  if (value < 100) {
    "LOW"
  } else if (value < 500) {
    "MEDIUM"
  } else {
    "HIGH"
  }
})

// COMMAND ----------

val udfuse = cleanCSV.withColumn("CATEGORIA",categoryUDF(col("Absolute value in thousands")))
display(udfuse)

// COMMAND ----------

// Calcular el porcentaje del valor absoluto(sum+avg) de cada AgeClass con respecto al total de su Economy y Sex(ag)

val porcent = cleanCSV.groupBy("Economy","Sex").agg(avg("AgeClass")).as("PORCENT%")
display(porcent)
