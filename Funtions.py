import pandas
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

class Function:
    # Spieleinfluss Ausgeben
    def print_games_effect(self, filename, RDD):
        file = open(filename, "w")
        file.writelines("-" * 63+"\n")
        file.writelines('|{name: <50}|{desc: <10}|\n'.format(name='Name',desc='Effekt'))
        file.writelines("-" * 63+"\n")

        for i in RDD:
            line = '|{name: <50}|{desc: <10}|\n'.format(name=str(i[0]),desc=i[1])
            file.writelines(line)

        file.writelines("-" * 63+"\n")

    # nicht genutzt
    def printToFileSale_Platform(self, filename, RDD):
        file = open(file=filename)
        for i in RDD :
            line = i[0]+","+i[1][0]+","+i[1][1]
            file.writelines(line)

    def printToFile(self,RDD,FILE):
        file = open(FILE, "w")
        for i in RDD:
            file.writelines(str(i))


    # Hardware Ausgabe
    def print_Hardware(self, filename,RDD):
        file = open(filename, "w")
        file.writelines("-" * 83 + "\n")
        file.writelines('|{Genre: <50}|{Year: <15}|{Prise: <15}|\n'.format(Genre='Genre', Year='Year',Prise='Prise'))
        file.writelines("-" * 83 + "\n")

        for i in RDD:
            line = '|{Genre: <50}|{Year: <15}|{Prise: <15}|\n'.format(Genre=str(i[0]), Year=i[1],Prise=i[2])
            file.writelines(line)

        file.writelines("-" * 83 + "\n")

    # Verkauf Ausgabe
    def print_sales(self, filename,RDDName,RDD):
        file = open(filename, "a")

        file.writelines("-" * 20 + "\n")
        file.writelines('|{name: <19}|\n'.format(name=RDDName))
        file.writelines("-" * 63+"\n")
        file.writelines('|{name: <40}|{desc: <20}|\n'.format(name='Name',desc='Sales'))
        file.writelines("-" * 63+"\n")

        for i in RDD:
            line = '|{name: <40}|{desc: <20}|\n'.format(name=str(i[0]),desc=str(i[1][1]))
            file.writelines(line)

        file.writelines("-" * 63+"\n")

    # Graphic Ausgeben
    def createGrahpHardware(self,GRAFIC_FILE,RDD,sc):
        spark = SparkSession(sc)
        _df = RDD.toDF(["Year", "Genre", "Price"]).distinct()
        df = _df.sort(col("Year")).toPandas()
        df_year = df['Year'].drop_duplicates()[:23]
        df_price = df.groupby(np.arange(len(df)) // 8).mean()
        df_row = pd.DataFrame({'Year': np.array(df_year), 'Price': np.concatenate(np.array(df_price))})
        df_row.plot.line(subplots=True, x='Year', y='Price')
        plt.savefig(GRAFIC_FILE)

    # Statistic Ausgabe
    def print_Statistics(self,filename ,RDD, Min, Max):
        file = open(filename, "a")

        file.writelines("-" * 30 + "\n")
        file.writelines('|{name: <19}|{min: <10}|\n'.format(name='MIN',min=Min))
        file.writelines("-" * 30 + "\n")
        file.writelines('|{name: <19}|{max: <10}|\n'.format(name='MAX',max=Max))
        file.writelines("-" * 63 + "\n")
        file.writelines('|{name: <40}|{desc: <20}|\n'.format(name='Name', desc='Anteil'))
        file.writelines("-" * 63 + "\n")

        for i in RDD:
            line = '|{name: <40}|{desc: <20}|\n'.format(name=str(i[0]), desc=str(i[1]))
            file.writelines(line)

        file.writelines("-" * 63 + "\n")

    # Berechnet einen Rank von der Liste für Die Hardware. Die erste Aufgabe.
    def get_Rank(self, list):
        return str(int(list[0])+int(list[1])+int(list[2]))

    # Avarage von gesamten Preisliste Berechnen. Die Erste Aufgabe
    def _AVG(self, list):
        sum = 0.0
        for i in list:
            sum += float(i)

        return sum/len(list)