import os

from pyspark.sql.types import *
import config
import queries, io_cluster
import udfs
import patterns
from pathlib import Path
import re
import unicodedata
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import ArrayType, IntegerType, StringType
from pyspark.sql import functions as F

schema = StructType([
    # StructField("tên công việc", StringType(), True),
    # StructField("tên công ty", StringType(), True),
    # StructField("địa điểm", StringType(), True),
    # StructField("mức lương", StringType(), True),
    # StructField("kinh nghiệm", StringType(), True),
    # StructField("mô tả công việc", StringType(), True),
    # StructField("kĩ năng yêu cầu", StringType(), True),
    # StructField("thông tin liên hệ", StringType(), True),
    # StructField("loại công việc", StringType(), True),
    # StructField("cấp bậc", StringType(), True),
    # StructField("học vấn", StringType(), True),
    # StructField("giới tính", StringType(), True),
    # StructField("tuổi", StringType(), True),
    # StructField("ngành nghề", StringType(), True),
    # StructField("quyền lợi", StringType(), True),
    # StructField("thời gian làm việc", StringType(), True),
    # StructField("hạn nộp", StringType(), True),


    StructField("source",            StringType(), True),
    StructField("job_title_clean",   StringType(), True),
    StructField("company_name",      StringType(), True),
    StructField("location_clean",    StringType(), True),
    StructField("salary_min",        StringType(),   True),
    StructField("salary_max",        StringType(),   True),
    StructField("job_type",          StringType(), True),
    StructField("job_level",          StringType(), True),
    StructField("experience",          StringType(), True),
    StructField("industry",          StringType(), True),
    StructField("skills",            StringType(), True),
    StructField("job_description",   StringType(), True),
    StructField("education",   StringType(), True),
    StructField("salary_text",       StringType(), True),
    StructField("experience_text",   StringType(), True)


])



@F.udf(returnType=ArrayType(IntegerType()))
def extract_exp_pattern(kinh_nghiem: str):
    # lấy ra tất cả các số nguyên <-> số năm kinh nghiệm
    if kinh_nghiem is None:
        return []
    exp = re.findall(r'\b\d+\b', kinh_nghiem)
    exp_range = [int(number) for number in exp]
    if len(exp_range) == 2:
        end = exp_range[1]
        start = exp_range[0]
        if end >= 10:
            end = 10
        start = int(start / 1)
        end = int(end / 1)
        exp_range = [1 * i for i in range(start, end + 1)]
    return exp_range

def _strip_accents(s: str) -> str:
    if s is None:
        return ""
    # bỏ dấu, lower, loại ký tự không cần thiết
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

@F.udf(returnType=StringType())
def normalize_text(s: str) -> str:
    return _strip_accents(s)

# Loại các từ hình thức DN & filler khỏi tên công ty để so khớp khoan dung hơn
COMPANY_STOP = set("""
cong ty tnhh mtv mtvv co phan cp tap doan doanh nghiep tmdv tm dv thuong mai dich vu
company jsc llc ltd co., co, inc., inc group viet nam vietnam vn
""".split())

@F.udf(returnType=StringType())
def signature_tokens(s: str) -> str:
    s = _strip_accents(s)
    toks = [t for t in s.split() if t not in COMPANY_STOP]
    # dùng tập token đã sắp xếp → ổn định trước khác biệt nhỏ về thứ tự
    toks = sorted(set(toks))
    return " ".join(toks)

@F.udf(returnType=StringType())
def render_exp_from_list(exp_list):
    # chuyển [1,2,3] -> "1 - 3 năm"; [2] -> "2 năm"
    if not exp_list:
        return None
    xs = sorted(set([int(x) for x in exp_list if x is not None]))
    if not xs:
        return None
    mn, mx = xs[0], xs[-1]
    if mn == mx:
        return f"{mn} năm"
    return f"{mn} - {mx} năm"

def pick_longest(colname: str):
    # lấy giá trị dài nhất (thường đầy đủ hơn) trong nhóm, bỏ qua null/rỗng
    expr = f"max_by(coalesce(`{colname}`, ''), length(coalesce(`{colname}`, '')))"
    return F.expr(expr).alias(colname)

COL_TITLE = "tên công việc"
COL_COMP  = "tên công ty"
COL_CITY  = "địa điểm"
COL_SAL   = "mức lương"
COL_EXP   = "kinh nghiệm"

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
        "hdfs://node01:8020/result/*.json")
    
# --- Trích xuất salary_min và salary_max bằng UDF (thêm vào ngay sau khi đọc raw_recruit_df) ---
    raw_recruit_df = raw_recruit_df.withColumn(
        "salary_range",
        udfs.extract_salary_range(F.col("salary_text"))
    )
    raw_recruit_df = raw_recruit_df.withColumn("salary_min", F.col("salary_range").getItem(0).cast("double"))
    raw_recruit_df = raw_recruit_df.withColumn("salary_max", F.col("salary_range").getItem(1).cast("double"))
# --- end ---

    

    # raw_1 = spark.read.schema(schema).option("multiline", "true").json("hdfs://node01:8020/result/data1.json").withColumn("_src", F.lit("s1"))
    # raw_2 = spark.read.schema(schema).option("multiline", "true").json("hdfs://node01:8020/result/data2.json").withColumn("_src", F.lit("s2"))
    # raw_3 = spark.read.schema(schema).option("multiline", "true").json("hdfs://node01:8020/result/data3.json").withColumn("_src", F.lit("s3"))
    # df = raw_1.unionByName(raw_2).unionByName(raw_3)
    # df.show(5)
    # df_std = (
    #     df
    #     .withColumn("_title_norm", normalize_text(F.col(COL_TITLE)))
    #     .withColumn("_comp_norm",  normalize_text(F.col(COL_COMP)))
    #     .withColumn("_city_norm",  normalize_text(F.col(COL_CITY)))
    #     .withColumn("_title_sig",  signature_tokens(F.col(COL_TITLE)))
    #     .withColumn("_comp_sig",   signature_tokens(F.col(COL_COMP)))
    #     .withColumn("_exp_list",   extract_exp_pattern(F.col(COL_EXP)))
    # )

    # # Khóa chặn (blocking key) — thành phố + chữ ký tiêu đề + chữ ký công ty
    # df_blk = df_std.withColumn(
    #     "_block_key",
    #     F.sha2(F.concat_ws("|", F.col("_city_norm"), F.col("_title_sig"), F.col("_comp_sig")), 256)
    # )

    # # =========================
    # # 3) Hợp nhất theo cụm trùng (survivorship)
    # # - Gộp theo _block_key
    # # - Lấy bản "đầy đủ nhất" cho từng trường (chuẩn: dài nhất)
    # # - Điền kinh nghiệm nếu thiếu bằng danh sách gom lại
    # # =========================
    # agg_cols = [
    #     pick_longest(COL_TITLE),
    #     pick_longest(COL_COMP),
    #     pick_longest(COL_CITY),
    #     pick_longest(COL_SAL),
    #     pick_longest(COL_EXP),
    #     pick_longest("mô tả công việc"),
    #     pick_longest("kĩ năng yêu cầu"),
    #     pick_longest("quyền lợi"),
    #     pick_longest("thời gian làm việc"),
    #     pick_longest("hạn nộp"),
    #     F.array_distinct(F.flatten(F.collect_list(F.col("_exp_list")))).alias("_exp_list_all"),
    # ]

    # merged = (
    #     df_blk
    #     .groupBy("_block_key")
    #     .agg(*agg_cols)
    #     # nếu cột "kinh nghiệm" đang rỗng, điền từ _exp_list_all
    #     .withColumn(
    #         COL_EXP,
    #         F.when(
    #             (F.col(COL_EXP).isNull()) | (F.length(F.trim(F.col(COL_EXP))) == 0),
    #             render_exp_from_list(F.col("_exp_list_all"))
    #         ).otherwise(F.col(COL_EXP))
    #     )
    # )

    # # =========================
    # # 4) Làm sạch & đánh lại STT, giữ nguyên schema gốc
    # # =========================
    # # điền STT mới (1..N)
    # win = W.orderBy(F.col(COL_TITLE).asc_nulls_last())
    # # merged2 = merged.withColumn("stt", F.row_number().over(win).cast("int"))

    # # chọn đúng thứ tự cột theo schema (giữ nguyên)
    # schema_cols = [f.name for f in schema]  # đảm bảo đúng tên & thứ tự
    # # raw_recruit_df = merged.select(*schema_cols)
    # out_dir = "file:///D:/AHUST/TichHop/Bigdata_Hang/src/result/merged_json"
    # (raw_recruit_df
    # .coalesce(1)
    # .write.mode("overwrite")
    # .json(out_dir))

    print('read data successully!!!!')
    raw_recruit_df.show(5)

    # extracted_recruit_df = raw_recruit_df.select(raw_recruit_df["tên công việc"].alias("JobName"),
    #                                              raw_recruit_df['tên công ty'].alias("CompanyName"),
    #                                              udfs.extract_location("địa điểm").alias("Location"),
    #                                              udfs.extract_exp_pattern('kinh nghiệm').alias("Experience"),
    #                                              raw_recruit_df['loại công việc'].alias("JobType"),
    #                                              raw_recruit_df["cấp bậc"].alias('Level'),
    #                                              udfs.extract_education( "kĩ năng yêu cầu").alias('Education'),
    #                                              raw_recruit_df["giới tính"].alias('Sex'),
    #                                              udfs.extract_old_pattern("tuổi").alias("Old"),
    #                                              udfs.extract_framework_plattform("mô tả công việc",
    #                                                                               "kĩ năng yêu cầu").alias(
    #                                                  "FrameworkPlattforms"),
    #                                              udfs.extract_IT_language("mô tả công việc", "kĩ năng yêu cầu").alias(
    #                                                  "JobLanguages"),
    #                                              udfs.extract_language("kĩ năng yêu cầu").alias("Languages"),
    #                                              udfs.extract_design_pattern("mô tả công việc",
    #                                                                          "kĩ năng yêu cầu").alias("DesignPatterns"),
    #                                              udfs.extract_knowledge("mô tả công việc", "kĩ năng yêu cầu").alias(
    #                                                  "Knowledges"),
    #                                              udfs.normalize_salary("mức lương").alias("Salaries"),
    #                                              raw_recruit_df['thông tin liên hệ'].alias("Contact"),
    #                                             udfs.extract_job_type("ngành nghề").alias("JobSummary"),
    #                                              ).withColumn('Knowledge', udfs.get_grouped_knowledge("Knowledges"))
    
    extracted_recruit_df = (
        raw_recruit_df.select(
            F.col("source").alias("source"),
            F.col("job_title_clean").alias("title"),
            F.col("company_name").alias("company"),
            F.col("location_clean").alias("location_raw"),

            # F.col("salary_min").cast("long").alias("salary_min"),
            # F.col("salary_max").cast("long").alias("salary_max"),
            F.col("salary_min"),
            F.col("salary_max"),


            F.col("job_type").alias("job_type"),
            F.col("job_level").alias("job_level"),
            F.col("experience").alias("experience_avg"),
            F.col("industry").alias("industry"),

            F.col("skills").alias("skills"),

            F.col("job_description").alias("description"),
            F.col("salary_text").alias("salary_text"),
            F.col("experience_text").alias("experience_text"),

            
            udfs.extract_IT_language("job_description", "skills").alias("JobLanguages"),
            udfs.extract_language("skills").alias("Languages"),
            udfs.extract_design_pattern("job_description",
                                        "skills").alias("DesignPatterns"),
            udfs.extract_knowledge("job_description", "skills").alias(
                "Knowledges"),
            udfs.extract_education("education").alias('Education'),
            udfs.extract_job_type("industry").alias("JobSummary"), 
            udfs.extract_exp_pattern('experience_text').alias("Experience"),   

            # ======= tiện ích / chuẩn hoá nhẹ =======
            # Trung bình lương (nếu có cả min và max)
            F.when(
                F.col("salary_min").isNotNull() & F.col("salary_max").isNotNull(),
                (F.col("salary_min") + F.col("salary_max")) / 2.0
            ).cast("double").alias("salary_avg"),

            # Tách khu vực & tỉnh/thành từ location (dựa theo dấu phẩy)
            F.trim(F.split(F.col("location_clean"), r",\s*").getItem(0)).alias("location_area"),
            F.trim(F.split(F.col("location_clean"), r",\s*").getItem(1)).alias("province")
        )
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