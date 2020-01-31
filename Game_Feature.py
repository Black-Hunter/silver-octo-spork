# Game Description zerlegen
class Game(object):
    def __init__(self, line):
        lines = line.split("#SEPARATOR#")

        self.Name = lines[0]
        self.Desc = lines[1]


# Wrappers, die die Werte auf Zahlen konvertieren und umgekehrt
import enum
from scipy.spatial import distance

class Genre(enum.Enum):
   Action   = 1
   Shooting = 2
   Puzzel   = 3
   Strategy = 4
   other    = 5

class Player(enum.Enum):
   Multi  = 1
   Singel = 0

class blood(enum.Enum):
   Ja   = 1
   Nein = 2

class Target(enum.Enum):
   kein    = 0
   Human   = 1
   Monster = 2
   Other   = 3

class Impact(enum.Enum):
    Positive = 0
    Negative = 1


class Game_Feature(object):

    def __init__(self):
        print("")

    # liefert ob a singel oder multi player gibt.
    def get_player(self, line):
         # if not Multplayer then singel
        _match_words_player_multi = ["Multi", "Mul", "multi", "Many Player", "Many player"]
        line = line.lower()

        for word in _match_words_player_multi:
            word = word.lower()
            if word in line:
                return Player.Multi.value

            return Player.Singel.value


    # liefert ob blood gibt oder nicht
    def get_blood(self, line):
        # if not Multplayer then singel
        line = line.lower()
        if "blood" in line:
            return blood.Ja.value

        return blood.Nein.value

    # Liefert der Target
    def get_target(self, line, gerne):
        if gerne != Genre.Action.value and gerne == Genre.Shooting.value:
            _match_words_target_monster = ["zombi", "monster","undead","beast", "devil", "dragon","horror"]
            line = line.lower()

            for word in _match_words_target_monster:
                word = word.lower()
                if word in line:
                    return Target.Human.value

            if "human" in line:
                return Target.Human.value

            return Target.Other.value

        else:
            return Target.kein.value


    # Liefert der Type des Spieles
    def get_gerne(self, line):
        # if not Multplayer then singel
        _match_words_player_gerne_action = ["Action"]
        _match_words_player_gerne_shooting = ["Gun", "target", "teams"]
        _match_words_player_gerne_strategy = ["strategy", "station", "country"]
        line = line.lower()

        for word1 in _match_words_player_gerne_action:
            word1 = word1.lower()

            if word1 in line:
                return Genre.Action.value

        for word2 in _match_words_player_gerne_shooting:
            word2 = word2.lower()
            if word2 in line:
                return Genre.Shooting.value

        for word3 in _match_words_player_gerne_strategy:
            word3 = word3.lower()
            if word3 in line:
                return Genre.Strategy.value

            return Genre.Puzzel.value


    # Wandelt die Ausgabe zu einer Liste
    def get_feature(self, line):
        player = self.get_player(line)
        gerne  = self.get_gerne(line)
        blood  = self.get_blood(line)
        target = self.get_target(line, gerne)

        __match_list = [player,gerne,blood,target]
        return __match_list

    # rechnet einen Anteil
    def get_ratio(self,line_list, CLASS_list):
        # ersetzt die matches mit leer und subtrahiert das gesamt davon --> Anteil der Matches
        #SIZE = float(len(line_list))
        #replace = line_list.replace(str(CLASS_list), "")
        #print(str(CLASS_list).replace("]","").replace("[",""))
        ratio = distance.hamming(line_list,CLASS_list)
        #float(((SIZE - float(len(replace)) )) / SIZE)
        return (1 - ratio)

    # Liefert --> Positive oder Negative abhängig von dem Anteil --> Was ist mehr wharscheinlich Positive oder Negative?
    def get_Effect(self, list):
        _FILE_NAME = "Positive_Class.txt"
        file = open(_FILE_NAME)
        positive_rate = -1.0;
        negative_rate = -1.0;
        for line in file:
            if "#" not in line:
                line_parts = line.split(",")
                # jede Zeile in einer Liste tun
                _parameters = [int(line_parts[0]),int(line_parts[1]),int(line_parts[2]),int(line_parts[2])]
                # listen vergleichen, und Ähnlichkeit berechnen
                ratio = float(self.get_ratio(list, _parameters))
                # schauen ob das Positive oder Negative ist
                if int(line_parts[4]) == 1:
                    if ratio > negative_rate:
                        negative_rate = ratio
                else:
                    if ratio > positive_rate:
                        positive_rate = ratio
        # Vergleiche die Warcheinlichkeit welches am wharscheinlisćhsten
        if negative_rate > positive_rate:
            return Impact.Negative.value

        return Impact.Positive.value

    # Nicht genutzt weil ich alles in Mapper in Pyspark berechnet ---> Aufgabe 2
    def get_Impcat(self, line):
        __match_list = self.get_feature(line)
        effect = self.get_Effect(__match_list)
        return Impact(int(effect)).name