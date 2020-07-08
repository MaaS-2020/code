val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val kd = new java.io.File("C:/Study/spark/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports").listFiles.filter(_.getName.endsWith(".csv"))
val fileMap = kd.map(x => (x.getName().replaceAll(".csv", ""), x))
val dfsMap = fileMap map { case(x, y) => (x, sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema", "true").option("delimiter", ",").load(y.getAbsolutePath()))}
val columnNamesList = dfsMap map {case(x, df) => df.columns}
val columnNames = columnNamesList.reduce(_ ++ _).toSet
val columnMaps = Map("Last Update" -> "Last_Update","Lat" -> "Latitude", "Long_" -> "Longitude", "Province/State" -> "Province_State", "Country/Region" -> "Country_Region")
val dfsRenameColumns = dfsMap map {
    case(x, someDF) => (
        x,
        {
            val cols = someDF.columns.collect(name => columnMaps.get(name) match { 
                case Some(newname) => col(name).as(newname) 
                case None => col(name)
            })
            someDF.select(cols: _*)
        }

    )
}
val onlyColumnNames = columnNames.map(x => columnMaps.getOrElse(x, x))
val dfsAddMissingColumns = dfsRenameColumns map {
    case (x, df) => (
        x,
        {
            val columnsAdded = onlyColumnNames.foldLeft(df) { 
                case (d, c) => if (d.columns.contains(c)) {
                        // column exists; skip it
                        d
                    } else {
                        // column is not available so add it
                        d.withColumn(c, lit(null))
                }
            }   
            columnsAdded  
        }
    )
}
import org.apache.spark.sql
val convertToIntegerColumns = dfsAddMissingColumns map {
    case (x, df) => (
        x,
            df.withColumn("Active", $"Active".cast(sql.types.IntegerType))
               .withColumn("Confirmed", $"Confirmed".cast(sql.types.IntegerType))
               .withColumn("Deaths", $"Deaths".cast(sql.types.IntegerType))
               .withColumn("Recovered", $"Recovered".cast(sql.types.IntegerType))
    )
}
val monthNames = Map(
    "01" -> "January", 
    "02" -> "February", 
    "03" -> "March", 
    "04" -> "April", 
    "05" -> "May", 
    "06" -> "June", 
    "07" -> "July", 
    "08" -> "August",
    "09" -> "September",
    "10" -> "October",
    "11" -> "November",
    "12" -> "December")
val dfsMapAddDateCol = convertToIntegerColumns map {
    case(x, y) => (
        x, 
        y.withColumn("repDate", lit(x))
          .withColumn("month", lit(x.split("-")(0)))
          .withColumn("monthName", lit(monthNames(x.split("-")(0))))
          .withColumn("Day", lit(x.split("-")(1)))
          .withColumn("Year", lit(x.split("-")(2)))
    )
}
val mergedDF = dfsMapAddDateCol map {
    case(x, y) => y
}
val wholeDF = mergedDF.reduce(_.unionByName(_))
val convertedDF = wholeDF.withColumn("Active", $"Active".cast(sql.types.IntegerType))
                        .withColumn("Confirmed", $"Confirmed".cast(sql.types.IntegerType))
                        .withColumn("Deaths", $"Deaths".cast(sql.types.IntegerType))
                        .withColumn("Recovered", $"Recovered".cast(sql.types.IntegerType))
val wholeDFNullFix = convertedDF.na.fill(0)
wholeDFNullFix.schema
val dumpDF = wholeDFNullFix.na.fill(0,Array("Confirmed"))
dumpDF.schema
wholeDF.createOrReplaceTempView("covid_data")
val sqlDF = spark.sql("SELECT repDate, sum(Confirmed) as confirmed, sum(Deaths) as deaths, sum(Active) as active, sum(Recovered) as recovered from covid_data group by repDate")
sqlDF.show()
sqlDF
   .coalesce(1)
   .write.format("com.databricks.spark.csv")
   .option("header", "true")
   .save("C:/Study/spark/output/mydata.csv")