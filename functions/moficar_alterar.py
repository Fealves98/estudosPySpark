from pyspark.sql.functions import col, lit, when


class ModificarOuAlterar:

    def modificar_ou_alterar_data_frame(self, df):

        # aqui estou criando uma coluna nova e atribuindo valor a ela
        df_novo = df.withColumn("pais", lit("Brasil"))

        # aqui basicamente ele esta criando uma nova coluna e copiando os dados da coluna estado
        df_copia_coluna = df_novo.withColumn("sigla_estado", col("estado"))

        #aqui basicamente ele faz um if E else pra alterar os valor dada determinada conticao - E ASSIM PARA DOS DOS ESTADOS
        df_copia_when = df_novo.withColumn("nome_estado", when(df.estado == "RR", "RORAIMA").otherwise("OUTROS"))

        df_copia_when.show()
