"""
Module processor.py: main driver of application
"""
import logging
from pyspark.sql import functions as F

from io_handler import IOHandler
from data_transformer import DataTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

def join_dfs(student_df, teacher_df, join_key):
    """ Joins dataframes to combine student and teacher data """

    # Updating column names to disambiguate after join
    student_df = student_df.select([F.col(c).alias('s_' + c) for c in student_df.columns])
    teacher_df = teacher_df.select([F.col(c).alias('t_' + c) for c in teacher_df.columns])

    joined_df = student_df.join(
        teacher_df,
        on=student_df["s_" + join_key] == teacher_df["t_" + join_key],
        how='left'
    )
    return joined_df

def run(spark, student_file, teacher_file, out_path='report.json'):
    """ Main driver function of data processor application """

    io_handler = IOHandler(spark)
    try:
        student_df = io_handler.spark_read_file(student_file, delim='_')
        logger.info("Successfully loaded student file from %s", student_file)
        teacher_df = io_handler.spark_read_file(teacher_file)
        logger.info("Successfully loaded teacher file from %s", teacher_file)
    except FileNotFoundError as error_message:
        logger.error(error_message)
        return

    joined_df = join_dfs(student_df, teacher_df, 'cid')
    logger.info("Finished joining dataframes")

    transformer = DataTransformer(spark)
    output_df = transformer.df_with_output_schema(joined_df)
    logger.info("Fit data to output schema:")
    output_df.show()

    io_handler.write_report(output_df, 'json', out_path)
    logger.info("Processing completed")
