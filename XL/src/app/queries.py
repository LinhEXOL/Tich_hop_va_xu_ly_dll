from pyspark.sql.functions import explode, col, size
from app import udfs


def get_counted_knowledge(extracted_recruit_df):
    '''
    Return a dataframe of 2 columns:
    Col1 (Knowledge): StringType, name of knowledge
    Col2 (count): IntegerType, number of appearances for each knowledge

    Parameters
    ----------
    extracted_recruit_df : orginal dataframe
    '''
    return extracted_recruit_df.withColumn("Knowledge", explode("Knowledges")).select("Knowledge").groupBy("Knowledge").count().orderBy("count",ascending=False)

def get_grouped_knowledge(knowledge_df, mapped_knowledge):
    '''
    Return a dataframe of 2 columns:
    Col1 (Category): StringType, name of category of knowledge
    Col2 (count): IntegerType, number of appearances for each category of knowledge

    Parameters
    ----------
    knowledge_df : dataframe of knowledges
    '''

    try:
        mapped_knowledge= mapped_knowledge.value['Knowledge']
    except:
        return None

    value = mapped_knowledge.value['Knowledge']
    return knowledge_df.withColumn('Category', value)\
          .groupBy('Category').sum("count").filter("Category!='null'")

def get_count_job_type(extracted_recruit_df):
    return extracted_recruit_df.withColumn('HardOrSoftWare', udfs.extract_job_type("JobSummary")).groupBy('HardOrSoftWare').sum('count').filter("HardOrSoftWare!='null'")

def get_not_null_salary(extracted_recruit_df):
    return  extracted_recruit_df.filter(size(col("Salaries")) > 0)
