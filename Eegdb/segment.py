import os
import numpy as np
from datetime import datetime, timedelta

class Segment:
  def __init__(self,subjectid,fileid,channel_index,channel_label,sample_rate,start_datetime,end_datetime,signals):
    self.__doc = {
      "subjectid": subjectid,
      "fileid": fileid,
      "channel_index":channel_index,
      "channel_label":channel_label,
      "sample_rate":sample_rate,
      "start_time":start_datetime,
      "end_time":end_datetime,
      "signals":signals
    }
  
  def get_doc(self):
    return self.__doc