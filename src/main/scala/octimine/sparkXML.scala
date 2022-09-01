package brgroup

import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.ini4j.Wini

import java.io.{File, FileNotFoundException, IOException}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{array_distinct, array_position, col, collect_list, concat, element_at, expr, flatten, udf}
import org.apache.spark.sql.streaming.Trigger
import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI


object sparkXML extends App with Logger{
  //  get input parameters
  val log4Path : String =  args(0)
  val iniFilePath: String = args(1)

  //  Setup log4j class
  PropertyConfigurator.configure(log4Path)
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.INFO)

  val ini :Wini =
    try {
      new Wini(new File(iniFilePath))
    }
    catch {
      case x: FileNotFoundException =>
        println("Exception: File missing")
        error("Exception - ",x)
        null
      case x: IOException   =>
        println("Input/output Exception")
        error("Exception - ",x)
        null
    }

  //Read all section from config file : Common Sections
  val sparkSessionSection = ini.get("SparkSession")
  val inputStreamSection = ini.get("ReadStream")
  val writeStreamSection = ini.get("WriteStream")

  info("the ini file has been loaded Successfully")
  
  val sparkSession:SparkSession = new SparkSessionHandler(sparkSessionSection).getSparkSession

  val df = sparkSession.read.format(inputStreamSection.get("format", classOf[String]))
    .option("rowTag", inputStreamSection.get("option.rowTag", classOf[String]))
    //.load("src/main/resources/DOCDB-202044-CreateDelete-PubDate20201028-EP-0001.xml")
    .load(inputStreamSection.get("hdfs.uri", classOf[String]) + inputStreamSection.get("input.path", classOf[String]))
  // selecting reqierd data from input xml whitout applying aggregation based on family_id
  // and droping invalid documents and duplicate values by uinque_doc_id
  // uinque_doc_id is comination of three fields country, doc-umber, kind
  val dfWithoutDuplicateByKey = df.select(
    inputStreamSection.get("input.xml.familyTag", classOf[String]),
    inputStreamSection.get("input.xml.countryTag", classOf[String]),
    inputStreamSection.get("input.xml.docNumberTag", classOf[String]),
    inputStreamSection.get("input.xml.kindTag", classOf[String]),
    inputStreamSection.get("input.xml.docIdTag", classOf[String]),
    inputStreamSection.get("input.xml.datePublTag", classOf[String]),
    inputStreamSection.get("input.xml.inventionTitleValueTag", classOf[String]),
    inputStreamSection.get("input.xml.inventionTitleLangTag", classOf[String]),
    inputStreamSection.get("input.xml.classIpcrTag", classOf[String]),
    inputStreamSection.get("input.xml.applicantNameTag", classOf[String]),
    inputStreamSection.get("input.xml.abstractTag", classOf[String])
  )
    .withColumn("index", array_position(col("_lang"), "en"))
    .withColumn("invention_title_en",
      element_at(
        col("_value"),

        array_position(col("_lang"), "en").cast("int")

      )
    )
    .withColumn("splted_text_col_by_space", expr("transform(text , x -> split(x, ' ')[0])"))
    .withColumn("dist_splted_text_col_by_space", array_distinct(col("splted_text_col_by_space")))
    .withColumn("uinque_doc_id", concat(col("_country"), col("_kind"), col("_doc-number")))
    .na.drop().dropDuplicates("uinque_doc_id")
    // check if any prior batch are processed or not in order to load log ids
    //In each batch processing, all of document ids (combination of 3 fields including country, doc-number, kind) are logged in hdfs directory and in the begining of next batch, the repeated data will be excluded by checking with logs.
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(new URI(inputStreamSection.get("hdfs.uri", classOf[String])), conf);
    val exists = fs.exists(new org.apache.hadoop.fs.Path(inputStreamSection.get("hdfs.uri", classOf[String])+"/deltadirectory/_SUCCESS"))

   val deltaTableDf : DataFrame = exists
    match {
     case true => sparkSession.read.option("header", "true").csv(inputStreamSection.get("hdfs.uri", classOf[String]) + inputStreamSection.get("input.deltadir.path", classOf[String]))
     case _ => dfWithoutDuplicateByKey.select("uinque_doc_id").where("1=2")
   }

  // removing repeated uinque_doc_id from original DF using left anti join
  val dfDeduplicatedByDeltaTable = dfWithoutDuplicateByKey
    .join(deltaTableDf, dfWithoutDuplicateByKey("uinque_doc_id")===deltaTableDf("uinque_doc_id"), "left_anti" )

  // aggregating DF based on family id
  val aggregatedDf = dfDeduplicatedByDeltaTable
    .groupBy(col("_family-id").as("family_id"))
    .agg (
      collect_list("_country").as("country"),
      collect_list("_doc-number").as("doc_number"),
      collect_list("_kind").as("kind"),
      collect_list("_doc-id").as("doc_id"),
      collect_list("_date-publ").as("date_publ"),
      collect_list("invention_title_en").as("invention_title_en"),
      collect_list("dist_splted_text_col_by_space").as("classification_ipcr"),
      collect_list("name").as("aplicant_name"),
      collect_list(col("exch:p")).as("abstract")
    ).withColumn("classification_ipcr",array_distinct(flatten(col("classification_ipcr"))))
    .withColumn("aplicant_name",array_distinct(flatten(col("aplicant_name"))))


  //Implement Upsert dataframe to postgresql table
  val conn_str = "jdbc:postgresql://postgres:5432/brgroup?user=postgres&password=postgres"

  val conn = DriverManager.getConnection(conn_str)
  try {
    val stm = conn.createStatement(ResultSet.CLOSE_CURSORS_AT_COMMIT, ResultSet.CONCUR_READ_ONLY)

    stm.executeUpdate("DROP TABLE IF EXISTS BRGROUP.TEMP_PATENT_DATA")
    stm.executeUpdate("CREATE TABLE BRGROUP.TEMP_PATENT_DATA AS SELECT * FROM BRGROUP.PATENT_DATA WHERE false")

    aggregatedDf
      .write
      .format("jdbc")
      .option("url","jdbc:postgresql://postgres:5432/brgroup")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "BRGROUP.TEMP_PATENT_DATA")
      .option("user", "postgres")
      .option("password", "postgres")
      .option("numPartitions", 10)
      .mode(SaveMode.Append)
      .save()

    aggregatedDf.show(false)

    stm.executeUpdate(
      """INSERT INTO BRGROUP.PATENT_DATA (family_id, country, doc_number, kind, doc_id, date_publ, invention_title_en, classification_ipcr, aplicant_name, abstract)
    SELECT family_id, country, doc_number, kind, doc_id, date_publ, invention_title_en, classification_ipcr, aplicant_name, abstract FROM BRGROUP.TEMP_PATENT_DATA
    ON CONFLICT (family_id) DO
      UPDATE SET
    country = PATENT_DATA.country || EXCLUDED.country,
    doc_number = PATENT_DATA.doc_number || EXCLUDED.doc_number,
    kind = PATENT_DATA.kind || EXCLUDED.kind,
    doc_id = PATENT_DATA.doc_id || EXCLUDED.doc_id,
    date_publ = PATENT_DATA.date_publ || EXCLUDED.date_publ,
    invention_title_en = PATENT_DATA.invention_title_en || EXCLUDED.invention_title_en,
    classification_ipcr = PATENT_DATA.classification_ipcr || EXCLUDED.classification_ipcr,
    aplicant_name = PATENT_DATA.aplicant_name || EXCLUDED.aplicant_name,
    abstract = PATENT_DATA.abstract || EXCLUDED.abstract""")

  } finally {
    conn.close()
  }
  // Write DeltaTable
  dfDeduplicatedByDeltaTable.select(col("uinque_doc_id")).write
    .mode(SaveMode.Append)
    .option("header",true)
    .csv(inputStreamSection.get("hdfs.uri", classOf[String]) + writeStreamSection.get("output.deltadir.path", classOf[String]))

}

