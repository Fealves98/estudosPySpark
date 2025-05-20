from pyspark.sql.functions import col


class SelectDataFrame:
    def select_data_frame(self,df):

        # exibe todas as colunas do dataframe
        df.columns

        # faz um filtro das colunas que ele quer pegar
        df.select("email","senha")

        #outra forma de pegar tmb é
        df.select(df.email)

        #outra forma de pegar tmb é
        df.select(df["email"])

        #aqui pega todos porem o df ja tem tudo basicamente
        df.select("*")

