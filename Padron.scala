package jca

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Padron extends App with spark {
  /* Realiza la lectura del fichero padron.csv. Intenta inlcuir opciones para quitar las comillas,
     se ignoren espacios innecesarios, se infiera el esquema y se sustituyan los espacios vacios por 0
   */
  val padron = spark.read.option("sep", ";")
    .option("inferSchema", value = true)
    .option("header", value = true).csv("/user/cloudera/Rango_Edades_Seccion_202208.csv").na.fill(0)
    .withColumn("DESC_DISTRITO", trim(col("DESC_DISTRITO")))
    .withColumn("DESC_BARRIO", trim(col("DESC_BARRIO")))


  //padron.show(10,truncate = false)

  // Enumera los distintos barrios

  val barrios = padron.select("DESC_BARRIO").distinct()
  //barrios.show()

  // Crea una vista temporal de nombre padron y a través de ella cuenta el numero de barrios diferentes que hay
  padron.createOrReplaceTempView("padron")
  //spark.sql("SELECT distinct(DESC_BARRIO) from padron").show()

  // Crea una nueva columna que muestre la longitud del campo desc_distrito que se llame longitud
  //padron.withColumn("longitud", length(col("DESC_DISTRITO"))).show()

  // Crea una nueva columna que muestre el valor 5
  val constante = padron.withColumn("valor", lit(5))
  //constante.show()

  // Borra la columna anterior
  //constante.drop(col("valor")).show()

  // Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO
  val padronParticionado = padron.repartition(col("DESC_DISTRITO"), col("DESC_BARRIO"))
  //padronParticionado.show()

  // Almacenalo en caché
  val padronParticionadoCache = padronParticionado.cache()

  // Lanza una consulta contra este DF en la que muestres el número total de espanoleshombres/mujeres y extranjeroshombres/mujeres para casa barrio de cada distrito.
  // Las columnas distrito y barrio deben salir las primeras y ordenadas segun extranjerosmujeres de mas a menos y desempatadas por extranjeros hombres
  val padronSumHabitantesByDistritoAndBarrio = padronParticionadoCache.groupBy("DESC_DISTRITO", "DESC_BARRIO")
    .agg(
      sum(col("espanoleshombres")).alias("sumEspHom"),
      sum(col("espanolesmujeres")).alias("sumEspMuj"),
      sum(col("extranjeroshombres")).alias("sumExtHom"),
      sum(col("extranjerosmujeres")).alias("sumExtMuj")
    ).sort(desc("sumExtMuj"), desc("sumExtHom"))
  //padronSumHabitantesByDistritoAndBarrio.show()

  // Elimina el registro en caché
  padronParticionadoCache.unpersist()

  // Crea un nuevo DF a partir del original que muestre las columnas DESC_BARRIO, DESC_DISTRITO y otra con españoleshombres totales. Unelo con un join con el dataframe original
  val padronAgrupado = padron.groupBy("DESC_BARRIO", "DESC_DISTRITO").agg(sum(col("espanoleshombres")).alias("sumEspHom"))
  //padronAgrupado.show()

  val padronJoined = padron.join(padronAgrupado, padron("DESC_DISTRITO") === padronAgrupado("DESC_DISTRITO") && padron("DESC_BARRIO") === padronAgrupado("DESC_BARRIO"), "inner")
  //padronJoined.show()

  //Realiza el ejemplo anterior pero usando Windows como alternativa
  val padronWindowed = padron.withColumn("sumEspHom", sum("espanoleshombres").over(Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO")))
  //padronWindowed.show(50)

  // A través de pivot muestra una tabla que contenga los valores totales de espanolesmujeres para cada distrito y en cada rango de edad. Los distritos incluidos deben ser unicamente
  // BARAJAS - CENTRO - RETIRO
  val padronMujEspEdadDistrito = padron.select("COD_EDAD_INT", "DESC_DISTRITO", "espanolesmujeres").where(expr("DESC_DISTRITO = 'BARAJAS' or DESC_DISTRITO = 'CENTRO' or DESC_DISTRITO = 'RETIRO'"))
  //padronMujEspEdadDistrito.show()
  val pivot = padronMujEspEdadDistrito.groupBy("COD_EDAD_INT").pivot("DESC_DISTRITO").agg(sum("espanolesmujeres")).na.fill(0).sort(asc("COD_EDAD_INT"))
  //pivot.show()

  // utilizando este nuevo DF, crea 3 columnas que hagan referencia a que porcentaje de la suma de "espanolesmujeres" representa cada uno de los 3 distritos
  pivot.withColumn("% Barajas", bround(col("BARAJAS")/(col("BARAJAS") + col("CENTRO") + col("RETIRO")) * 100, 2))
    .withColumn("% Centro", bround(col("CENTRO")/(col("BARAJAS") + col("CENTRO") + col("RETIRO")) * 100, 2))
    .withColumn("% Retiro", bround(col("Retiro")/(col("BARAJAS") + col("CENTRO") + col("RETIRO")) * 100, 2)).show()

  // Guardar el archivo original csv particionado por distrito y barrio (en ese orden) en un directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba que es la esperada
  padron.write.option("header", value = true).partitionBy("DESC_DISTRITO", "DESC_BARRIO").mode("overwrite").csv("/user/cloudera/padronParticionadoCSV")

  // Ahora guardalo en formato parquet y compara el tamoaño de los archivos con los anteriores
  padron.write.option("header", value = true).partitionBy("DESC_DISTRITO", "DESC_BARRIO").mode("overwrite").parquet("/user/cloudera/padronParticionadoParquet")
}
