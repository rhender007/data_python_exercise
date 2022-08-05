import json

class DataTransformer:
    """
    A Class used to handle the data transformations we need to perform on the data
    
    ...

    Attributes
    ----------
    spark_df: Spark DataFrame
        A Spark DataFrame to work with
    schema : json
        The loaded json mapping from internal format to the output format

    """

    def __init__(self, spark_df, schema_path="resources/column_mapping.json"):
        """
        Initializes a new Transformer object with what it needs to transform the data

        Parameters
        ----------
        spark_df : Spark DataFrame
            A Spark DataFrame
        schema_path : string
            A file path to a json file
        """

        self.spark_df = spark_df
        self.schema = self.load_output_schema(schema_path)

    def load_output_schema(self, schema_path=""):
        """ Loads a file to output the correct schema """

        with open(schema_path, "r") as schema_file:
            schema = json.load(schema_file)
        return schema

    def df_with_output_schema(self, output_df):
        """ Reformats the Spark DataFrame to match the output schema format """
        
        output_df = output_df.select(*self.schema.keys())
        for col in output_df.columns:
            output_df = output_df.withColumnRenamed(col, self.schema[col])
        return output_df
