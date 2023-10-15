import enum
import numbers

from enum import unique

@unique
class Operator(enum.Enum):
    FileScan = 1
    Filter = 2
    BroadcastHashJoin = 3
    BroadcastExchange = 4
    HashAggregate = 5
    Project = 6
    Sort = 7
    SortMergeJoin = 8
    Subquery = 9
    Union = 10
    Expand = 11
    ExchangeHashpartitioning = 12
    ExchangeSinglePartition = 13
    ReusedExchange = 14
    Empty = 15
    TakeOrderedAndProject = 16
    CollectLimit = 17
    Error = 18

    def __eq__(self, other):
        # if self.__class__ is other.__class__:
        #     return self == other
        try:
            return self.value == other.value
        except:
            pass
        try:
            if isinstance(other, numbers.Real):
                return self.value == other
            if isinstance(other, str):
                return self.name == other
        except:
            pass
        return NotImplemented


if __name__ == '__main__':
    op = Operator.Filter
    if op == Operator.Filter:
        print("equal")
