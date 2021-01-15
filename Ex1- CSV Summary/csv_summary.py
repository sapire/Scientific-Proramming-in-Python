import csv
import json


class Group:
    """This class represents a group, its 'name' attribute holds the value of the groupby feature for that group"""

    def __init__(self, name):
        """This is an initializer for a Group object. It receives the name of the groupby feature."""

        self.name = name
        self.dictFeatures = {}
        self.index = 0

    def addfeature(self, feature, aggregatefName, aggregateValue):
        """addfeature method get feature(string), aggregatefName- the aggragate name, and the aggregateValue. it will add aggregated feature to the group and save it
        at the feature dictionaty so feature and aggregatefName is the key, and aggregateVal is the value . """
        self.dictFeatures[f"{feature} ({aggregatefName})"] = aggregateValue

    def __getitem__(self, item):
        """getitem method return feature value. It can be accessed by index or feature name with [] operator. negative index refer to the end of the list"""
        if type(item) == int:
            return self.dictFeatures[list(self.dictFeatures.keys())[item]]
        else:
            for i in self.dictFeatures:
                if i.split()[0] == item:
                    return self.dictFeatures[i]

    def __str__(self):
        """Convert group object to string showing group name and features"""
        strRep = self.name+" - "
        for idx, i in enumerate(self.dictFeatures):
            if idx == len(self.dictFeatures)-1:
                strRep += i+":"+f"{self.dictFeatures[i]}"
            else:
                strRep += i+":"+f"{self.dictFeatures[i]}, "

        return strRep

    def __iter__(self):
        """ iter return feature iterator"""
        return self

    def __next__(self):
        """Return the next group in the iterator"""
        if self.index == len(self.dictFeatures):
            raise StopIteration

        key = list(self.dictFeatures.keys())[self.index]
        self.index = self.index+1
        return (key.split()[0], self.dictFeatures[key])


class Summary:
    """This class represents a summary of the CSV file it receives, according to the instructions in the JSON file."""

    def __init__(self, CSV, JSON):
        """Initializes a new summary object. Receives a CSV file and a JSON file"""
        self.csvList = self.convertToDIct(CSV)
        self.jsonList = self.convertJsonToDict(JSON)
        if self.csvList == [] or self.jsonList == {}:
            self.groupList = {}
            self.featureOrder = []
            return
        self.groupList = self.groupby()
        self.featureOrder = list(self.csvList[0].keys())

    def __getitem__(self, key):
        """given a group name as key, returns the group matching it, implement [] operator """
        for i in self.groupList:
            if i.name == key:
                return i

    def __str__(self):
        """ str convert the Summary object to string containing the group string for each group in a new line"""
        strRep = ""
        for idx, i in enumerate(self.groupList):
            if idx == len(self.groupList)-1:
                strRep += f"{i}"
            else:
                strRep += f"{i}\n"
        return strRep

    def __iter__(self):
        """ iter return iterator to the groupList, which allows to iterate over groups in summary"""
        return self.groupList.__iter__()

    def saveSummary(self, filename, delimiter=','):
        """ saveSummary method get the filename(destination file) and delimiter(char) as an optional parameter (the default will be a comma ',').
        the method save the summary as a CSV file in the order of the colums is the same as the source file used to create the Summary object,
         except that the groupby object feature will be first. the order of the groups will be ascending of the group name.
         The header row format will be: "group_name (groupby), feature_name1 (aggregate1), feature_name2 (aggregate2)..."
         """
        with open(filename, 'w', newline='') as csvfile:
            if self.jsonList == {}:
                return
            if self.featureOrder == []:
                return

            headerRow = [self.jsonList['groupby']+" (groupby)"]

            spec = self.getSpec()
            writer = csv.writer(csvfile, delimiter=delimiter)
            for item in self.featureOrder:
                if not item == self.jsonList['groupby'] and not spec.get(item) == None:
                    headerRow += [f"{item} ({spec[item]})"]
            writer.writerow(headerRow)
            for group in self.groupList:
                data = [group.name]
                for value in self.featureOrder:
                    if not value == self.jsonList['groupby'] and value in self.getSpec():
                        data.append(group[value])
                writer.writerow(data)

    def getGroups(self):
        """getGroup method return a list of all groups created."""
        return self.groupList

    def getSpec(self):
        """getSpec method return a dictionary with the following format:
        {feature1:aggregate1, feature2:aggregate2,...,featureN:aggregateN} """
        dictSpec = {}
        featureList = self.jsonList['features']
        for f in featureList:
            feature = list(f.keys())[0]
            if f[feature]["type"] == "textual":
                continue
            aggregate = f[feature]["aggregate"]
            dictSpec[feature] = aggregate
        return dictSpec

    def groupby(self):
        """groupby create the groups and using the specification in the JSON file,
        it calls the appropriate aggregation function and add it`s value to the group.
        the return value is the list of group object   """
        groupByFeature = self.jsonList['groupby']
        table = self.splitToGroups(groupByFeature)
        groupList = []
        for groupName in table:
            row = Group(groupName)
            groupList.append(row)
            featureList = self.jsonList['features']
            for f in featureList:
                feature = list(f.keys())[0]
                ftype = f[feature]["type"]
                if ftype == "textual":
                    continue
                aggregatefunc = f[feature]["aggregate"]
                aggregateVal = None
                vals = [idx[feature] for idx in table[groupName]]

                if ftype == "categorical":
                    for idx in range(0, len(vals)):
                        if vals[idx] == '':
                            vals[idx] = 'NA'
                    if aggregatefunc == "mode":
                        aggregateVal = self.Mode(sorted(vals, key=str.lower))
                    if aggregatefunc == "union":
                        aggregateVal = self.categoricalUnion(vals)
                    if aggregatefunc == "unique":
                        aggregateVal = self.categoricalUniqe(vals)
                    if aggregatefunc == "union":
                        aggregateVal = self.categoricalUnion(vals)
                    if aggregatefunc == "count":
                        aggregateVal = self.Count(vals)
                if ftype == "numerical":
                    for idx in range(0, len(vals)):
                        if vals[idx] == '':
                            vals[idx] = 0
                        else:
                            vals[idx] = int(vals[idx])
                    if aggregatefunc == "min":
                        aggregateVal = self.min(vals)
                    if aggregatefunc == "max":
                        aggregateVal = self.max(vals)
                    if aggregatefunc == "median":
                        aggregateVal = self.median(vals)
                    if aggregatefunc == "mean":
                        aggregateVal = self.mean(vals)
                    if aggregatefunc == "sum":
                        aggregateVal = self.sum(vals)
                    if aggregatefunc == "mode":
                        aggregateVal = self.Mode(sorted(vals))
                    if aggregatefunc == "Count":
                        aggregateVal = self.Count(vals)

                row.addfeature(feature, aggregatefunc, aggregateVal)
        return groupList

    def convertJsonToDict(self, JSON):
        """This function get JSON file in order to read it and convert in to List of dictionary and return it."""

        try:
            with open(JSON, newline='')as jsonfile:
                reader = json.load(jsonfile)
            return reader
        except:
            return {}

    def convertToDIct(self, CSV):
        """This function get CSV file in order to read it and convert in to List of dictionary and return it."""
        try:
            dict = []
            with open(CSV, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    dict.append(row)
            return dict
        except:
            return []

    def splitToGroups(self, feature):
        """splitToGroups get a feature(string) and returns a dictionary that each unique value of the original feature is a key and it's value is the list of rows containing it"""
        vals = sorted(set([idx[feature]for idx in self.csvList]))
        groupsDict = {}
        for v in vals:
            groupsDict[v] = [row for row in self.csvList if row[feature] == v]
        return groupsDict

    def Mode(self, vals):
        """ categoricalMode get vals(list) of feature value all belong to cetain feature in a group and
        return the value which appears most. ties broken by first in alpha-betical order. return value is dependant
        on the value type in vals, if vals is a list of strings, a string will be returned, else a number will be returned"""
        dictCount = {}
        maxval = 0
        maxkey = None
        for k in vals:
            if not k in dictCount:
                dictCount[k] = 1
            else:
                dictCount[k] += 1
            if dictCount[k] > maxval:
                maxval = dictCount[k]
                maxkey = k

        return maxkey

    def categoricalUnion(self, vals):
        """ categoricalUnion get vals(list) of feature value all belong to cetain feature in a group and
        return string containing all unique entitios in category separated by semicolon(;)."""

        return ";".join(set(vals))

    def categoricalUniqe(self, vals):
        """ categoricalUnique get vals(list) of feature value all belong to cetain feature in a group and
        return the number of unique entities in category (int)."""

        return len(set(vals))

    def Count(self,  vals):
        """ count get vals(list) of feature value all belong to cetain feature in a group and
        return total number of entities in category (int)."""
        res = len(vals)
        return res

    def min(self,  vals):
        """min get vals(list) of feature value all belong to cetain feature in a group and
        return the minimum value in category (int) """
        return min(vals)

    def max(self,  vals):
        """max get vals(list) of feature value all belong to cetain feature in a group and
        return the maximum value in category (int) """
        return max(vals)

    def median(self,  vals):
        """median get vals(list) of feature value all belong to cetain feature in a group and
        return the median value in category (int) """
        vals = sorted(vals)
        length = len(vals)
        res = 0

        if length % 2 == 0:
            res = (vals[length//2] + vals[(length//2)-1])//2

        else:
            res = vals[length//2]

        return res

    def mean(self, vals):
        """mean get vals(list) of feature value all belong to cetain feature in a group and
        return the mean values in category (int) """
        vals = sorted(vals)

        return sum(vals)/len(vals)

    def sum(self,  vals):
        """sum get vals(list) of feature value all belong to cetain feature in a group and
        return the sum values in category (int) """
        return sum(vals)


if __name__ == "__main__":
    s = Summary('cars.csv', 'features.json')
    s.saveSummary("new.txt")
