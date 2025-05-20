import pyspark.sql.functions as F

class ManipulacaoString:

    def manipulacao_de_string(self, df):
       minupulado =  df.select(df.email, df.senha, df.estado, df.cor_favorita, df.profissao)

       #aqui ele faz o split geral
       minupulado = minupulado.withColumn("usuario_quebrado", F.split(df.email, "@"))

       #aqui eu só pego a primeira posicao do split
       minupulado = minupulado.withColumn("usuario", F.split(df.email, "@").getItem(0))


       # Concatenando String
       minupulado = minupulado.withColumn("concat", F.concat(df.profissao,F.lit(" - ") ,F.lit(df.cor_favorita)))


