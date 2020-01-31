class Sales:
# Verkauf Object und liefert relevante Spalten
    def __init__(self, line):
        lines = line.split(",")
        self.Rank = lines[0]
        self.Name = lines[1]
        self.basename = lines[2]
        self.Genre = lines[3]
        self.ESRB= lines[4]
        self.Rating= lines[5]
        self.Platform= lines[6]
        self.Publisher= lines[7]
        self.Developer = lines[8]
        self.Global_Sales = lines[13]
        self.Year = lines[17]
