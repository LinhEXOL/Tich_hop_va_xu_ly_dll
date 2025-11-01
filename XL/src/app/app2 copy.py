import os

from pyspark.sql.types import *
import config
import queries, io_cluster
import udfs
import patterns
from pathlib import Path

# schema = StructType([
#     StructField("tên công việc", StringType(), True),
#     StructField("tên công ty", StringType(), True),
#     StructField("Địa điểm công việc", StringType(), True),
#     StructField("Mức lương", StringType(), True),
#     StructField("Kinh nghiệm", StringType(), True),
#     StructField("mô tả công việc", StringType(), True),
#     StructField("kĩ năng yêu cầu", StringType(), True),
#     StructField("thông tin liên hệ", StringType(), True),
#     StructField("loại công việc", StringType(), True),
#     StructField("cấp bậc", StringType(), True),
#     StructField("học vấn", StringType(), True),
#     StructField("giới tính", StringType(), True),
#     StructField("tuổi", StringType(), True),
#     StructField("ngành nghề", StringType(), True),
# ])

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("source", StringType(), True),
    StructField("original_id", StringType(), True),

    StructField("job_title", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),

    StructField("salary_min", IntegerType(), True),
    StructField("salary_max", IntegerType(), True),
    StructField("salary_currency", StringType(), True),

    StructField("job_type", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("sector", StringType(), True),

    StructField("skills", ArrayType(StringType()), True),

    StructField("education_level", StringType(), True),
    StructField("experience_years", IntegerType(), True),
    StructField("job_description", StringType(), True),

    StructField("sentiment_score", DoubleType(), True),

    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
])



if __name__ == "__main__":
    APP_NAME = "PreprocessData"

    app_config = config.Config(elasticsearch_host="elasticsearch",
                               elasticsearch_port="9200",
                               elasticsearch_input_json="yes",
                               hdfs_namenode="hdfs://node01:8020"
                               )
    spark = app_config.initialize_spark_session(APP_NAME)
    sc = spark.sparkContext
    sc.addPyFile(os.path.dirname(__file__) + "/patterns.py")
    # print()

    raw_recruit_df = spark.read.schema(schema).option("multiline", "true").json(
        "hdfs://node01:8020//result/*.json")
    
    print('read data successully!!!!')
    raw_recruit_df.show(5)

    extracted_recruit_df = raw_recruit_df.select(raw_recruit_df["job_title"].alias("JobName"),
                                                 raw_recruit_df['company_name'].alias("CompanyName"),
                                                 #udfs.extract_location("location").alias("Location"),
                                                 raw_recruit_df['experience_years'].alias("Experience"),
                                                 raw_recruit_df['loại công việc'].alias("JobType"),
                                                 raw_recruit_df["cấp bậc"].alias('Level'),
                                                 udfs.extract_education("education_level").alias('Education'),
                                                raw_recruit_df["giới tính"].alias('Sex'),
                                                udfs.extract_old_pattern("tuổi").alias("Old"),
                                                udfs.extract_framework_plattform("job_description","skills").alias(
                                                    "Knowledges"),
                                                 udfs.extract_IT_language("mô tả công việc", "kĩ năng yêu cầu").alias(
                                                     "JobLanguages"),
                                                 udfs.extract_language("skills").alias("Languages"),
                                                 udfs.extract_design_pattern("mô tả công việc",
                                                                             "kĩ năng yêu cầu").alias("DesignPatterns"),
                                                 udfs.extract_knowledge("mô tả công việc", "kĩ năng yêu cầu").alias(
                                                     "FrameworkPlattforms"),
                                                udfs.normalize_salary_vals("salary_min","salary_max","salary_currency" ).alias("Salaries"),
                                                 raw_recruit_df['thông tin liên hệ'].alias("Contact"),
                                                udfs.extract_job_type("ngành nghề").alias("JobSummary"),
                                                 )
    print('extract successuly!!!!')
    extracted_recruit_df.cache()
    extracted_recruit_df.show(5)

    # salaries_not_null= queries.get_not_null_salary(extracted_recruit_df)
    # salaries_not_null.show(5)

    # ##========save extracted_recruit_df to hdfs========================
    df_to_hdfs = (extracted_recruit_df,)
    df_hdfs_name = ("extracted_recruit",)
    io_cluster.save_dataframes_to_hdfs("extracted_data", app_config, df_to_hdfs, df_hdfs_name)
    print('save to hdfs success!!!')



    # ##========make some query==========================================
    # knowledge_df = queries.get_counted_knowledge(extracted_recruit_df)
    # knowledge_df.cache()
    # knowledge_df.show(5)

    # udfs.broadcast_labeled_knowledges(sc, patterns.labeled_knowledges)
    # grouped_knowledge_df =
    # grouped_knowledge_df.show(10)

    # extracted_recruit_df = extracted_recruit_df.drop("Knowledges")
    # extracted_recruit_df.cache()

    ##========save some df to elasticsearch========================
    df_to_elasticsearch = (
        extracted_recruit_df,
        # salaries_not_null
        # grouped_knowledge_df
    )

    df_es_indices = (
        "recruit",
        # 'salaries'
        # "grouped_knowledges"
    )
    # extracted_recruit_df.show(5)
    print('start save to elasticsearch!!!!')
    io_cluster.save_df_to_elastic(df_to_elasticsearch, df_es_indices, app_config)
    print('done all tasks!!!!')