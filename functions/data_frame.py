
class LeituraArquivo:
    def leitor_de_arquivos(self, spark):

        #Extrai os arquivos e monta o data frame
        df = spark.read.csv("C:/Users/Felipe Alves/Downloads/DATASETS/DATASETS/LOGINS.csv", sep=";", header=True)
        df_txt = spark.read.csv("C:/Users/Felipe Alves/Downloads/DATASETS/DATASETS/LOGINS.txt", sep=";", header=True)
        df_parquet = spark.read.parquet("C:/Users/Felipe Alves/Downloads/DATASETS/DATASETS/LOGINS.parquet")

        return df_parquet