import os
import numpy as np
from datetime import datetime, timedelta

class Segment:
  def __init__(self,subjectid,fileid,channel,start_time,end_time,signals):
    self.__doc = {
      "subjectid": subjectid,
      "fileid": fileid,
      "channel":channel,
      "start_time":start_time,
      "end_time":end_time,
      "signals":signals
    }
  
  def get_doc(self):
    return self.__doc