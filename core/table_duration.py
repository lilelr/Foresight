# 考虑 Node 对象
class Table:
    def __init__(self,name,duration,actual_size,output_columns):
            self.name = name
            self.duration = duration
            self.actual_size = actual_size
            self.output_columns = output_columns
            self.cost_per_column=duration/output_columns # in seconds

    def __repr__(self):
            return repr((self.name,"actual_size:" +str( self.actual_size),"cost_per_column (sec):" +str(self.cost_per_column) ))