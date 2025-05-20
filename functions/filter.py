class FiltraDados:
    def filtra_dados(self,df):

       #filtra apenas dados de clientes de MG
       df.filter(df.estado == "MG")

       # aqui faz um and com estado mas cor favorita por exemplo - uma forma de fazer
       df.filter((df.estado == "MG") & (df.cor_favorita == "Azul"))


       # aqui faz um and com estado mas cor favorita por exemplo - outra forma de fazer
       filtra_por_and = df.filter(df.estado == "MG").filter(df.cor_favorita == "Azul")

       # aqui basicamente esta fazendo um or - uma forma de fazer
       filtra_por_ou = df.filter((df.cor_favorita == "Ciano") | (df.cor_favorita == "Azul"))

       # Outra forma de fazer o OR
       df.filter(df.cor_favorita.isin("Ciano","Azul"))

       #Aqui eu tmb posso utilizar o WHERE tmb pra fazer o filter - acho mais interessante
       df.where(df.cor_favorita.isin("Ciano","Azul"))