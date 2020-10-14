class Fargate(object):
    def __init__(self, taskARN, publicIP, eniID):
        self.taskARN = taskARN
        self.publicIP = publicIP
        self.eniID = eniID
    
    def __eq__(self, other):
        if other is None:
            return False 
        if type(other) is Fargate:
            return self.taskARN == other.taskARN
        return False 