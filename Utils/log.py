import datetime
import math

class LogFile:
  def __init__(self,path):
    self.start_time = datetime.datetime.now()
    self.file = open(path, "w")
    self.file.write("LOG START "+ str(self.start_time))

    self.section = ""
    self.section_i = 0
  
  def close(self):
    end_time = datetime.datetime.now()
    self.file.write("LOG END "+ str(end_time))

  def start_section(self,section):
    self.section_i+=1
    self.section = section
    self.file.write('*'*20 + self.section + " " + str(datetime.datetime.now()) + '*'*20)
  
  def end_section(self):
    self.file.write('*'*20 + self.section + " " + str(datetime.datetime.now()) + '*'*20)
    self.section = ""

  def writeline(self, newlog):
    self.file.write(str(datetime.datetime.now()) + ' ' + newlog +'\n')
  
  def writelines(self, newlogs):
    for newlog in newlogs:
      self.file.write(str(datetime.datetime.now()) + ' ' + newlog +'\n')
