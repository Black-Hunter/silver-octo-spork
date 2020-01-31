# Pyspark
from builtins import print

import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

#Weitere Imports
import Sales as s
import Funtions as f
import Game_Feature as gf
import Hardware as h
import projectConfig as config
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# wichtige objecte
game = gf.Game_Feature() #--> Spielmerkmale
conf = config.Config()   #--> config.ini
func = f.Function()      #--> Klasse für Funktionen
sc = SparkContext(conf=conf.getConfig())

# Ausgabe txt und png Files
GAMES_FILE_NEGATIVE = "Negative_games.txt"
GAMES_FILE_POSITIVE = "Positive_games.txt"
CATEGORIES_FILE     = "cat.text"
STATISTIC_FILE      = "STATISTIC.txt"
HARDWARE_PRICE_FILE = "hardware_price.txt"
GRAPHIC_FILE        =[
                        "Hardware_Requirement_Action.png"
                     ]
LAPTOP_PRICE_FILE   = "hardware_price.txt"
LAPTOP_PRICE_GRAPH  = "Laptop_Price.png"

#
# Wenn Sie Die einzelne Aufgaben starten möchten, setzen Sie bitte in config.ini für jeden Block
# true fürs Starten und false anders.
# Erster Block ist GAME_HARDWARE, zweiter ist GAME_IMPACT, dritter ist STATISTICS uns vierter Block ist SALES
#

# ---------------------- Minimum Requierment und Hardware price von each Gerne in each year
if "true" in conf.get("GAME_HARDWARE"):

    # Spiel Anforderungen auf Rank und Spiel Name und Jahr Mappen
    games_requirement = sc.textFile("steam_hardware.txt").map(lambda line: h.SteamReqiertment(line)).map(lambda game: ([game.Genre,game.Year],h.RequiermentList(game.Requierment))).persist()
    # Hardware auf Leistung und Price Mappen
    laptop_prices = sc.textFile("laptops.csv").map(lambda line: h.laptop(line)).map(lambda hardware: (hardware.PRICE,func.get_Rank([hardware.CPU,hardware.RAM,hardware.GPU])))
    laptop_prices = laptop_prices

    # Konkatinieren mit einem Dict oder eine Hashtabelle
    PRICES = {}
    for a, b in laptop_prices.collect():
        PRICES.setdefault(str(b), []).append(a)

    # Übertragung der von Rank und Price in der Liste der Spiele
    games_requirement = games_requirement.map(lambda game: (game[0],[func._AVG(PRICES.get(func.get_Rank([game[1].CPU, game[1].RAM,game[1].GPU]))),[h.CPU(game[1].CPU).name, h.RAM(game[1].RAM).name,h.GPU(game[1].GPU).name]])).persist()
    games_requirement = games_requirement.map(lambda x: (x[0][0], x[0][1], x[1][0]))

    # Ausgabe der Hardwareprice - Rank list in Textfile.
    func.print_Hardware(HARDWARE_PRICE_FILE,games_requirement.collect())

    # Action Spiele aus der gesamten Liste
    action_requierment = games_requirement.filter(lambda x : "Action" in x[0]).map(lambda x :(x[1], x[0], x[2])).persist()
    #Strategy_requierment = games_requirement.filter(lambda x : "Strategy" in x[0]).map(lambda x :(x[1], x[0], x[2])).persist()

    # Zeicnen der Actionspeile - Hardwareprice  Kurve
    requirement_list = [
                        action_requierment
                        #,Strategy_requierment
                        ]
    for i in range(1):
        func.createGrahpHardware(GRAPHIC_FILE[i], requirement_list[i], sc)

    # Zeichnen der Hardware - Price Kurve
    spark = SparkSession(sc)
    RDD = laptop_prices.map(lambda x: (int(x[1]), float(x[0])))

    # Ich bin von diesem Ergebniss nicht ganz sicher!!.
    _df = RDD.toDF(["Rank", "Price"]).distinct()
    df = _df.sort(col("Rank")).toPandas().sort_values(by=['Rank'], ascending=False)
    df = df.groupby(np.arange(len(df)) // 8).mean()
    df = df.groupby(np.arange(len(df)) // 2).mean()
    df.plot(subplots=True, x='Rank', y='Price')
    plt.savefig(LAPTOP_PRICE_GRAPH)

    #action_requierment = games_requierment.filter(lambda x : "" in x[0]).distinct()
    #df.pivot(index='Year', columns='Genre', values='Price')
    #games_requierment= games_requierment.sortBy(lambda x: x[1]).collect()

# ----------------------- GAME_IMPACT
if "true" in conf.get("GAME_IMPACT"):

    games = sc.textFile("games.txt")
    games = games.map(lambda line: gf.Game(line)).persist()
    impacts = games.map(lambda line:(line.Name ,game.get_feature(line.Desc))).map(lambda list: (list[0], game.get_Effect(list[1]))).map(lambda effect:(effect[0],gf.Impact(int(effect[1])).name)).persist()
    pos = impacts.filter(lambda line: "Positive" in line).collect()
    neg = impacts.filter(lambda line: "Negative" in line).collect()

    # Resultat Ausgeben
    func.print_games_effect(GAMES_FILE_POSITIVE, pos)
    func.print_games_effect(GAMES_FILE_NEGATIVE, neg)


# ----------------------- STATISTICS
if "true" in conf.get("STATISTICS"):

    sales = sc.textFile("sales.csv").map(lambda line: s.Sales(line)).persist()

    # Histogramm
    histogram = sales.filter(lambda sale: not ("NA_Sales" in str(sale.Global_Sales)) and not (not str(sale.Global_Sales)))\
        .map(lambda sale: (float(sale.Global_Sales), 1 )).reduceByKey(lambda a, b: a+b).persist()

    spark = SparkSession(sc)
    _df = histogram.toDF(["Name", "Frequency"]).distinct()
    df = _df.toPandas()
    df = df.drop_duplicates()
    #df.pivot(index='Year', columns='Genre', values='Price')
    df.plot.hist(x='Name', y='Frequency')
    #df.plot.line( subplots=True , x='Genre', y='Anteil')
    plt.savefig('Histogram.png')


    # Anteile und Min und Max
    Anteile = sales.filter(lambda sale: not ("NA_Sales" in str(sale.Global_Sales)) and not (not str(sale.Global_Sales)))\
        .map(lambda sale: (sale.Name , float(sale.Global_Sales) )).reduceByKey(lambda a, b: a+b).persist()
    sum = sales.filter(lambda sale: not ("NA_Sales" in str(sale.Global_Sales)) and not (not str(sale.Global_Sales)))\
        .map(lambda sale: (1, float(sale.Global_Sales))).reduceByKey(lambda a, b : a+b).collect()
    sum = int(sum[0][1])
    MAX_MIN = Anteile.map(lambda a: (a[1], a[0])).sortByKey()
    MIN = MAX_MIN.min()
    MAX = MAX_MIN.max()

    Anteile_ = Anteile.map(lambda a: (a[0], a[1] / sum)).collect()

    func.print_Statistics(STATISTIC_FILE,Anteile_,MIN[0],MAX[0])


# ----------------------- SALES
if "true" in conf.get("SALES"):

    dataset = sc.textFile("sales.csv")
    sales = dataset.map(lambda line: s.Sales(line)).persist()

    # The Best Sails per categorie
    sport_sales = sales.filter(lambda sale: "Sports" in sale.Genre).map(
        lambda sale: (sale.Name, [sale.Genre, sale.Global_Sales])) \
        .sortBy(lambda sale: sale[1][1], False).take(5)

    action_sales = sales.filter(lambda sale: "Shooter" in sale.Genre or "Action" in sale.Genre).map(
        lambda sale: (sale.Name, [sale.Genre, sale.Global_Sales])) \
        .sortBy(lambda sale: sale[1][1], False).take(5)

    print(action_sales)

    Puzzle_sales = sales.filter(lambda sale : "Puzzle" in sale.Genre).map(lambda sale : (sale.Name, [sale.Genre, sale.Global_Sales]))\
        .sortBy(lambda sale: sale[1][1], False).take(5)

    print(Puzzle_sales)

    Misc_sales =  sales.filter(lambda sale : "Misc" in sale.Genre).map(lambda sale : (sale.Name, [sale.Genre, sale.Global_Sales]))\
        .sortBy(lambda sale: sale[1][1], False).take(5)

    print(Misc_sales)

    racing_sales =  sales.filter(lambda sale : "Racing" in sale.Genre).map(lambda sale : (sale.Name, [sale.Genre, sale.Global_Sales]))\
        .sortBy(lambda sale: sale[1][1], False).take(5)

    print(racing_sales)

    # best sails overall in categories
    best_overall = sales.filter(lambda sale: not ("NA_Sales" in str(sale.Global_Sales)) and not (not str(sale.Global_Sales)))\
        .map(lambda sale: (sale.Genre, float(sale.Global_Sales))).reduceByKey(lambda a,b: a+b).sortBy(lambda x : x[1], False)

    # Bieliebste Platforms
    wii_sales= sales.filter(lambda sale : "Wii"in sale.Platform).map(lambda sale : (sale.Name, [sale.Genre, sale.Global_Sales]))\
        .sortBy(lambda sale: sale[1][1], False).take(5)

    print(wii_sales)

    PS_sales = sales.filter(lambda sale : "PS" in sale.Platform).map(lambda sale : (sale.Name, [sale.Genre, sale.Global_Sales]))\
        .sortBy(lambda sale: sale[1][1], False).take(5)

    print(PS_sales)

    PC_sales = sales.filter(lambda sale : "PC" in sale.Platform).map(lambda sale : (sale.Name, [sale.Genre, sale.Global_Sales]))\
        .sortBy(lambda sale: sale[1][1], False).take(5)

    print(PC_sales)

    GB_sales = sales.filter(lambda sale : "DC" in sale.Platform).map(lambda sale : (sale.Name, [sale.Genre, sale.Global_Sales]))\
        .sortBy(lambda sale: sale[1][1], False).take(5)

    print(GB_sales)

    DC_sales = sales.filter(lambda sale : "GB" in sale.Platform).map(lambda sale : (sale.Name, [sale.Genre, sale.Global_Sales]))\
        .sortBy(lambda sale: sale[1][1], False).take(5)

    # Beliebste Spiele Pro Jahr  --> Das Ja
    _2000s_sails = sales.filter(lambda sale: sale.Year.isnumeric()).filter(lambda sale : int(sale.Year) >= 2010).take(5)
    _1900s_sails = sales.filter(lambda sale: sale.Year.isnumeric()).filter(lambda sale : int(sale.Year) >= 2000  and int(sale.Year) < 2010).take(5)
    _2010s_sails = sales.filter(lambda sale: sale.Year.isnumeric()).filter(lambda sale : int(sale.Year) < 2000 ).take(5)

    meinsten = sales.map(lambda sale : (sale.Global_Sales, sale.Name)).sortByKey().take(10)

    #print
    types = {"Sport Sales" : sport_sales,
             "Action Sales": action_sales}
             # ,
             #"2000s Sales" :_2000s_sails ,
             #"2000s Sales" : _1900s_sails,
             #"2000s Sales" : _2010s_sails,
             #"Over all"    : best_overall,
             #"wii Sales"   : wii_sales,
             #"PS Sales"    :PS_sales,
             #"PC Sales"    : PC_sales}

    for key in types:
        func.print_sales(CATEGORIES_FILE,key,types.get(key))

    # Barchart für die Fünf besten Spiele
    spark = SparkSession(sc)
    best_overall_df = best_overall.toDF(["Kategorie", "Verkaufe in Millionen"]).distinct()
    best_overall_df = best_overall_df.sort(col("Verkaufe in Millionen").desc()).limit(5).toPandas()
    best_overall_df = best_overall_df.drop_duplicates()
    # df.pivot(index='Year', columns='Genre', values='Price')
    best_overall_df.plot.bar(x='Kategorie', y='Verkaufe in Millionen')
    # df.plot.line( subplots=True , x='Genre', y='Anteil')
    plt.savefig('Sales.png')


#if __name__="__main__":