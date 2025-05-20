from pyspark.shell import spark
from pyspark.sql import SparkSession

from functions.data_frame import LeituraArquivo
from functions.filter import FiltraDados
from functions.moficar_alterar import ModificarOuAlterar
from functions.select import SelectDataFrame

# instanciacao para subir o pyspark'
spark =  (SparkSession.builder
          .appName("Manipulacao")
          .config("spark.sql.repll.eagerEval.enable", True)
          .getOrCreate())

print(spark.getActiveSession)

df = LeituraArquivo().leitor_de_arquivos(spark)
SelectDataFrame().select_data_frame(df)
FiltraDados().filtra_dados(df)
ModificarOuAlterar().modificar_ou_alterar_data_frame(df)

print(df)