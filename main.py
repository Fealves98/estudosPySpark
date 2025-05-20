from pyspark.sql import SparkSession

from functions.data_frame import LeituraArquivo
from functions.filter import FiltraDados
from functions.manipulacao_datas import ManipulacaoDatas
from functions.manipulacao_numeros import ManipulacaoNumeros
from functions.manipulacao_string import ManipulacaoString
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
ManipulacaoString().manipulacao_de_string(df)
ManipulacaoNumeros().manipulandoNumeros(spark)
ManipulacaoDatas().manipulacao_datas(spark)

print(df)