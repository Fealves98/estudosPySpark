import pyspark.sql.functions as F


class ManipulacaoDatas:

    def manipulacao_datas(self,spark):
        df = spark.read.parquet("C:/Users/Felipe Alves/Downloads/DATASETS/DATASETS/LOGINS.parquet")

        manipula = (
            #acrescenta um mes
            df
            .withColumn("mes", F.add_months(df.data_cadastro, 1))
            # remove um mes
            .withColumn("remove_mes", F.add_months(df.data_cadastro, -1))
            # pega data de hj
            .withColumn("data_hora", F.current_date())
            # pega hora de agr
            .withColumn("hora_de_agora", F.current_timestamp())
            #adiciona dias
            .withColumn("add_dias", F.date_add(df.data_cadastro, 15))
            #formata_data
            .withColumn("date_format", F.date_format(df.data_cadastro, "d/M/y"))

        )
        manipula.show()