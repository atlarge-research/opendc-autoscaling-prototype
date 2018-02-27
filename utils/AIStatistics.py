"""
A module with commonly used statistical tools.
"""

__author__ = 'Alexandru Iosup'
__email__ = 'A.Iosup at tudelft.nl'
__file__ = 'AIStatistics.py'
__version__ = '$Revision: 0.1$'
__date__ = "$Date: 2006/03/21 09:51:01 $"
__copyright__ = "Copyright (c) 2006 Alexandru IOSUP"
__license__ = "Python"

# ---------------------------------------------------
# Log:
# 05/01/2007 A.I. 0.2  Added class CWeightedStats for adding stats where
#                      series items are associated different weights (besides value)
# 21/03/2006 A.I. 0.1  Started this package
# ---------------------------------------------------

import math


# import sys

class CStats:
    """ a class for quickly summarizing comparable (e.g., numeric) data """

    def __init__(self, bIsNumeric=True, bKeepValues=True, bAutoComputeStats=True):
        self.Min = None
        self.Max = None
        self.Sum = 0
        self.SumOfSquares = 0
        self.Avg = None  # arithmetic mean
        self.StdDev = None  # standard deviation
        self.COV = None  # coefficient of variation
        self.NItems = 0
        self.Values = []
        self.bIsNumeric = bIsNumeric
        self.bKeepValues = bKeepValues
        self.bAutoComputeStats = bAutoComputeStats

    def addValue(self, Value):
        self.NItems += 1
        if self.bKeepValues: self.Values.append(Value)
        if self.Min is None or self.Min > Value: self.Min = Value
        if self.Max is None or self.Max < Value: self.Max = Value
        if self.bIsNumeric:
            self.Sum += Value
            self.SumOfSquares += Value * Value
            if self.bAutoComputeStats:
                self.Avg = float(self.Sum) / self.NItems
                if self.NItems - 1 > 0:
                    self.StdDev = math.sqrt(
                        (self.NItems * self.SumOfSquares - self.Sum * self.Sum) / (self.NItems * (self.NItems - 1)))
                else:
                    self.StdDev = 0.0
                if abs(self.Avg) > 0.0001:
                    self.COV = self.StdDev / self.Avg
                else:
                    self.COV = 0.0

    def doComputeStats(self):
        if self.NItems > 0:
            self.Avg = float(self.Sum) / self.NItems
            # sys.stdout.write("-------------------\n")
            # sys.stdout.write("NItems =%30.3f\n" % (float(self.NItems)))
            # sys.stdout.write("Sum    =%30.3f\n" % (float(self.Sum)))
            # sys.stdout.write("Sum^2  =%30.3f\n" % (float(self.Sum*self.Sum)))
            # sys.stdout.write("SumSq  =%30.3f\n" % (float(self.SumOfSquares)))
            # sys.stdout.write("N*SumSq=%30.3f\n" % (float(self.NItems * self.SumOfSquares)))
            # sys.stdout.write("Diff   =%30.3f\n" % (float(self.NItems * self.SumOfSquares - self.Sum * self.Sum)))
            # sys.stdout.flush()
            if self.NItems - 1 > 0:
                self.StdDev = math.sqrt(
                    (self.NItems * self.SumOfSquares - self.Sum * self.Sum) / (self.NItems * (self.NItems - 1)))
            else:
                self.StdDev = 0.0
            if abs(self.Avg) > 0.0001:
                self.COV = self.StdDev / self.Avg
            else:
                self.COV = 0.0


class CWeightedStats(CStats):
    """ a class for quickly summarizing comparable (e.g., numeric) data """

    def __init__(self, bIsNumeric=True, bKeepValues=True, bAutoComputeStats=True):
        CStats.__init__(self, bIsNumeric, bKeepValues, bAutoComputeStats)
        self.WSum = 0  # sum of weighted values
        self.WSumOfSquares = 0  # sum of squares of weighted values
        self.WAvg = None  # weighted average (Yahyapour,Lifka,...)
        self.AvgDev = None  # weighted average deviation (Oliker)
        self.TotalWeight = 0  # overall weight
        self.WValues = []  # list of weighted values
        self.WMin = None  # weighted min
        self.WMax = None  # weighted max

    def addValue(self, Value, Weight):
        CStats.addValue(self, Value)
        self.TotalWeight += Weight
        if self.bKeepValues: self.Values.append(WeightedValue)
        if self.bIsNumeric:
            WeightedValue = Value * Weight
            if self.WMin is None or self.WMin > WeightedValue: self.WMin = WeightedValue
            if self.WMax is None or self.WMax < WeightedValue: self.WMax = WeightedValue
            self.WSum += WeightedValue
            self.WSumOfSquares += WeightedValue * WeightedValue
            if self.bAutoComputeStats:
                if self.TotalWeight > 0:
                    self.WAvg = float(self.WSum) / self.TotalWeight
                else:
                    self.WAvg = 0.0
                if self.NItems > 0:
                    self.AvgDev = math.sqrt(self.SumOfSquares - self.Avg * self.Avg) / self.NItems
                else:
                    self.AvgDev = 0.0

    def doComputeStats(self):
        CStats.doComputeStats(self)
        if self.TotalWeight > 0:
            self.WAvg = float(self.WSum) / self.TotalWeight
        else:
            self.WAvg = 0.0
        if self.NItems > 0:
            self.AvgDev = math.sqrt(self.SumOfSquares - self.Avg * self.Avg) / self.NItems
        else:
            self.AvgDev = 0.0


class CHistogram:
    """ a class for creating histograms of comparable (e.g., numeric) data """

    def __init__(self, bIsNumeric=True, bKeepValues=True):
        self.Stats = CStats(bIsNumeric, bKeepValues, bAutoComputeStats=False)
        self.NItems = 0
        self.Values = {}
        self.MaxHeight = 0
        self.CDF = None

    def addValue(self, Value):
        self.NItems += 1
        # -- map value to histogram
        if Value not in self.Values: self.Values[Value] = 0
        self.Values[Value] += 1
        if self.MaxHeight < self.Values[Value]: self.MaxHeight = self.Values[Value]
        # -- create value statistics
        self.Stats.addValue(Value)

    def getMinValue(self):
        return self.Stats.Min

    def getMaxValue(self):
        return self.Stats.Max

    def computeCDF(self, StepSize=1, bMustRecompute=True):
        if bMustRecompute or self.CDF is None:
            self.Stats.doComputeStats()
            self.CDF = {}
            if self.NItems > 0:
                Counter = 0
                for Value in xrange(self.Stats.Min, self.Stats.Max + 1, StepSize):
                    if Value in self.Values:
                        Counter += self.Values[Value]
                    self.CDF[Value] = float(Counter) / self.NItems
        return self.CDF
