import pyspark.sql.functions as F


class ManipulacaoNumeros:

    def manipulandoNumeros(self, spark):
        df = spark.read.parquet("C:/Users/Felipe Alves/Downloads/DATASETS/DATASETS/IMC.parquet")

        df.show()

        #aqui basicamente ele esta arrendondando o numero
        df.withColumn("round", F.round(df.peso, 1))

        sum = df.withColumn("sum", df.peso + df.altura)

        #sum.show()

