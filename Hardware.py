import string
import re
import enum
import Game_Feature as gf

# Wrappers
class GPU(enum.Enum):
   NoNe    = 0
   GB1     = 1
   GB2     = 2
   GB3     = 3
   GB4     = 4
   GB8     = 5
   GB16    = 6

class CPU(enum.Enum):
   NoNe     = 0
   DualCore = 1
   Core2Due = 2
   Corei3   = 3
   Corei5   = 4
   Corei7   = 5

class RAM(enum.Enum):
    NoNe  = 0
    GB2   = 1
    GB3   = 2
    GB4   = 3
    GB8   = 4
    GB16  = 5

#  Anforderungen für Spiel --> liefert relevante Spalten
class SteamReqiertment(object):
    def __init__(self,line):
        lines = line.split("#SEPARATOR#")
        game = gf.Game_Feature()
        self.Genre       = gf.Genre(int(game.get_gerne(lines[0]))).name
        self.Year        = self.parseYear(lines[1])
        self.Requierment = lines[2]

    def parseYear(self,Year):
        return re.search(r"(\d{4})", Year).group(1)

# Liefert eine Liste der Anforderung bzw. Die Eigenschaften
class RequiermentList(object):
    def __init__(self,line,laptop=False):

        if laptop:
            self.CPU = self.get_CPU(line[6])
            self.GPU = self.get_GPU(line[9])
            self.RAM = self.get_ram(line[7], self.GPU, True)
        else:
            list  = self._parse(line)
            self.CPU   = list[0]
            self.RAM   = list[1]
            self.GPU   = list[2]

    def _parse(self, line):
        CPU   = self.get_CPU(line)
        GPU   = self.get_GPU(line)
        RAM   = self.get_ram(line, GPU)

        #replace("<strong>","").replace("<strong>","").replace("<br>","").replace("</li>","").replace(":","").replace("","")
        return [CPU,RAM,GPU]

    def get_ram(self,LINE,GPU,laptop = False):

        LINE = LINE.lower()

        GB1_List = ["1 GB", "1GB"]
        GB2_List = ["2 GB", "2GB"]
        GB4_List = ["4 GB", "4GB"]
        GB8_List = ["8 GB", "8GB"]
        GB16_List = ["16 GB", "16GB"]


        if laptop:
            for word2 in GB2_List:
                if word2.lower() in LINE:
                    return RAM.GB2.value

            for word3 in GB4_List:
                if word3.lower() in LINE:
                    return RAM.GB4.value

            for word3 in GB1_List:
                if word3.lower() in LINE:
                    return RAM.GB4.value

            for word1 in GB8_List:
                if word1.lower() in LINE:
                    return RAM.GB8.value

            for word3 in GB16_List:
                if word3.lower() in LINE:
                    return RAM.GB16.value

            return RAM.NoNe.value


        line = LINE.split("memory")

        if len(line) == 2 and ":" in line[1]:
            LINE_ = str((line[1].split("ram")[0])).lower()
            if  "16 " in LINE_ or "16gb" in LINE_ :
                return RAM.GB16.value
            if  "8 " in LINE_ or "8gb" in LINE_:
                return RAM.GB8.value
            if  "4 " in LINE_ or "4gb" in LINE_:
                return RAM.GB4.value
            if  "2 " in LINE_ or "2gb" in LINE_:
                return RAM.GB2.value
            if not "2 " in LINE_ or  not "2gb" in LINE_:
                return RAM.NoNe.value

        else:
            return RAM(int(GPU)).value


    def get_GPU(self, line):

        line = line.lower()
        G1_List    = [ "1 GB", "1GB",
                      "Intel HD Graphics 620",
                      "Nvidia Quadro M520M",
                      "Intel HD Graphics 530",
                      "Intel HD Graphics",
                      "Intel HD Graphics 500",
                      "Intel Iris Plus Graphics 650",
                      "AMD Radeon R5 430",
                      "Intel HD Graphics 405",
                      "Intel HD Graphics 515",
                      "Intel HD Graphics 510",
                      "Nvidia GeForce 920MX",
                      "Intel HD Graphics 5300",
                      "Intel HD Graphics 620",
                      "AMD Radeon R2",
                      "AMD Radeon R3",
                      "AMD Radeon R4",
                      "Intel Graphics 620",
                      "AMD R4 Graphics",
                      "Intel HD Graphics 505",
                      "Intel HD Graphics 615"]

        G2_List    = ["2GB", "2 GB",
                      "Nvidia Quadro M620",
                      "AMD FirePro W5130M",
                      "AMD Radeon Pro 555",
                      "AMD Radeon RX 560",
                      "AMD Radeon R2 Graphics",
                      "AMD FirePro W4190M",
                      "Intel HD Graphics 400",
                      "AMD Radeon R4 Graphics",
                      "Nvidia GeForce 930M",
                      "Nvidia Quadro 3000M",
                      "AMD Radeon R7 M440",
                      "AMD FirePro W4190M",
                      "AMD Radeon R5 M330",
                      "AMD Radeon R5 M430",
                      "Nvidia Quadro M620M",
                      "AMD Radeon R5 M420",
                      "Nvidia GeForce 930MX",
                      "Nvidia GeForce GTX 1050 Ti",
                      "AMD Radeon R5 520",
                      "Nvidia GeForce 150MX",
                      "Nvidia GeForce 940MX",
                      "AMD Radeon R5",
                      "AMD R17M-M1-70",
                      "AMD Radeon R5 M420X",
                      "AMD Radeon 520",
                      "AMD Radeon R5 M315",
                      "Nvidia GeForce GTX 930MX"]

        G4_List    = ["4 GB", "4GB", "3GB", "3 GB",
                      "iNvidia GeForce 920M",
                      "Nvidia GeForce GTX 1050Ti",
                      "Nvidia GeForce MX130",
                      "Intel HD Graphics 6000",
                      "Nvidia Quadro M2000M",
                      "Nvidia GeForce 920",
                      "Nvidia GeForce 920MX",
                      "AMD Radeon R7 M365X",
                      "Nvidia Quadro M1000M",
                      "Nvidia Quadro M500M",
                      "Nvidia GeForce GTX 970M",
                      "Intel HD Graphics 520",
                      "Nvidia GeForce 960M",
                      "AMD Radeon R7 Graphics",
                      "Nvidia GTX 980 SLI",
                      "Nvidia GeForce GTX1060",
                      "Nvidia GeForce GTX 1050",
                      "AMD Radeon R9 M385",
                      "Nvidia GeForce MX150",
                      "Nvidia Quadro M3000M",
                      "Nvidia GeForce GTX 980",
                      "Nvidia GeForce GTX 960",
                      "Intel Iris Plus Graphics 640",
                      "AMD Radeon RX 540",
                      "Nvidia GeForce GTX 940MX",
                      "Nvidia GeForce GTX 960M",
                      "AMD Radeon Pro 560",
                      "AMD Radeon Pro 455",
                      "AMD Radeon R7 M360",
                      "AMD FirePro W6150M",
                      "AMD Radeon R7 M445",
                      "AMD Radeon RX 550",
                      "Nvidia GeForce GTX 950M",
                      "Nvidia Quadro M2200M",
                      "Nvidia Quadro M1200",
                      "AMD Radeon 540",
                      "Nvidia GeForce 940M",
                      "Nvidia GeForce GT 940MX",
                      "Nvidia GeForce GTX1050 Ti",
                      "Nvidia GeForce 930MX",
                      "Nvidia GeForce GTX1080",
                      "AMD Radeon R7 M465",
                      "AMD Radeon R7",
                      "Nvidia GeForce GTX 965M",
                      "Nvidia GeForce GTX 1060",
                      "Nvidia GeForce GTX 940M",
                      "Nvidia Quadro M2200",
                      "Nvidia GeForce GTX 960"]

        GB8_List =   ["8 GB", "8GB","6GB", "6 GB",
                      "Nvidia GeForce GTX 1070",
                      "AMD Radeon RX 580",
                      "Nvidia GeForce GTX 1070M",
                      "Nvidia GeForce GTX 980M",
                      "Nvidia GeForce GTX 1080"]

        GB16_List = [ "16GB","16 GB",
                      "Intel Iris Graphics 550",
                      "Intel HD Graphics 630" ,
                      "Intel Iris Graphics 540",
                      "AMD Radeon R7 M460",
                      "Intel UHD Graphics 620"]

        for word2 in G1_List:
            if word2.lower() in line:
                return GPU.GB1.value

        for word3 in G2_List:
            if word3.lower() in line:
                return GPU.GB2.value

        for word3 in G4_List:
            if word3.lower() in line:
                return GPU.GB4.value

        for word1 in GB8_List:
            if word1.lower() in line:
                return GPU.GB8.value

        for word3 in GB16_List:
            if word3.lower() in line:
                return GPU.GB16.value

        return CPU.NoNe.value

    def get_CPU(self, line):
         DUALCORE_LIST = ["dual core", "dual core"]
         CORE2DUE_LIST = ["core 2 due", "core to due", "due"]
         CoreI3 = ["i3"]
         CoreI5 = ["i5"]
         CoreI7 = ["i6"]

         line = line.lower()

         for word1 in DUALCORE_LIST:

            if word1 in line:
                return CPU.DualCore.value

         for word2 in CORE2DUE_LIST:
            if word2 in line:
                return CPU.Core2Due.value

         for word3 in CoreI3:
            if word3 in line:
                return CPU.Corei3.value

         for word3 in CoreI5:
            if word3 in line:
                return CPU.Corei5.value

         for word3 in CoreI7:
            if word3 in line:
                return CPU.Corei7.value

         return CPU.NoNe.value


# Object für Laptop --> liefert relevante Spalten
class laptop(object):
    def __init__(self, line):
        lines = line.split(",")
        rl = RequiermentList(lines,True)
        self.CPU   =  rl.CPU
        self.GPU   =  rl.GPU
        self.RAM   =  rl.RAM
        self.PRICE =  float(lines[-1])
