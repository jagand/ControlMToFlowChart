import base64
from ftplib import FTP
import os
import datetime
import pandas as pd
import re
from graphviz import Digraph

#Code References
#   https://stackoverflow.com/questions/273192/how-can-i-create-a-directory-if-it-does-not-exist
#   https://stackoverflow.com/questions/1450393/how-do-you-read-from-stdin-in-python

u = 'racfUser'                          
p = 'racfPass'                
todayDate = str(datetime.datetime.now())[:10];
rawCtlmScheduleDataframe = pd.DataFrame(columns=['Schedule'])
ctlmSchedulesDataframe = pd.DataFrame(columns=['PredecessorScheduleName', 'PredecessorScheduleJob', 'SuccessorScheduleName', 'SuccessorScheduleJob'])
validWordsInSchedule = re.compile('[^-_@a-zA-Z0-9]')               # Organization and developer dependent

#Function that defines writeline. Without this, all lines are concatenated
def writeline(line):
    file.write(line + "\n")

#Create directory with today's date
if not os.path.exists(todayDate + '/CTLM_FLOW'):
    os.makedirs(todayDate + '/CTLM_FLOW')
    
#Read list of jobs users want to read
ctlmSchedule = input("Provide CTLM Schedule location: ")

# Connect to Mainframe and get files in directory created above   
ftp = FTP('mainframe.ip.address')                                
ftp.login(u, p)  
filename = todayDate + '/CTLM_FLOW/' + ctlmSchedule[ctlmSchedule.find('(') + 1:-1] + '.txt'
scheduleName = ctlmSchedule[ctlmSchedule.find('(') + 1:-1]
file = open(filename, 'w')             
member = "'" + ctlmSchedule + "'"

try:
    ftp.retrlines('RETR ' + member, writeline)
    print ('Downloaded CTLM schedule to ' + filename)
except Exception as e:
    print ('Unable to get CTLM schedule ' + member + " reason is " + str(e))
    
#Read downloaded CTLM schedule file. Important assumptions below
#   1. Read only lines starting with I or O. If it starts with I, its IN condition. If line starts with O, its out condition  
#   2. Put all these schedules in a dataframe    

previousLine = ''
    
with open(filename) as ctlmLines:
    inputOutputCondToggleFlag = 'false'
    for ctlmLine in ctlmLines:
        if (ctlmLine[:1] == 'I' or ctlmLine[:1] == 'O'):
            if (previousLine[:1] == ctlmLine[:1]):
                previousLine = previousLine + ctlmLine[1:]
            else:
                rawCtlmScheduleDataframe.loc[len(rawCtlmScheduleDataframe)] = previousLine
                previousLine = ctlmLine

# 1. Read schedules dataframe and split each row by space. 
# 2. Check each splitted item to see if it has Schedule name in it
#       a. If splitted item is space, ignore it               
#       b. If splitted item has schedule name, its relavant and hence store it
#       c. If previous splitted item has the word CONT in it and current splitted item is not space, append current word to previous
#       d. Add words extracted from step b or step b concatenated with step c in dataframe for further analysis after replacing the word CONT with space             
          
for iteration in range(0, len(rawCtlmScheduleDataframe)):
    ctlmSchedulesInLine = rawCtlmScheduleDataframe.at[iteration, "Schedule"].split(' ')
    appendFlag = 'N'
    scheduleCondition = ''
    predecessorScheduleName = '' 
    predecessorScheduleJob = ''
    successorScheduleName = ''
    successorScheduleJob = ''
    for iteration2 in range(0, len(ctlmSchedulesInLine)):
        if ctlmSchedulesInLine[iteration2] == '':
            continue
        
        if scheduleName in ctlmSchedulesInLine[iteration2]:
            scheduleCondition = ctlmSchedulesInLine[iteration2]
            if 'CONT' in scheduleCondition:
                appendFlag = 'Y'
        else:
            if appendFlag == 'Y':
                scheduleCondition = scheduleCondition + ctlmSchedulesInLine[iteration2]
                appendFlag = 'N'
    
#Refine extracted schedule by removing the words CONT, ODAT and any characters other than as defined in validWordsInSchedule regex 
#CAUTION!!! Assuming that job names wont have the words CONT, ODAT and all follow the pattern mentioned in validWordsInSchedule                
    scheduleCondition = scheduleCondition.replace('CONT','')
#    scheduleCondition = scheduleCondition.replace('ODAT','')    #Commenting this line based on some expected values in output
    scheduleCondition = validWordsInSchedule.sub('',scheduleCondition)       
    
#Once we get schedule, split it into predecessorScheduleName, predecessorScheduleJob, successorScheduleName, successorScheduleJob 
#    if '_' in scheduleCondition:
    if (scheduleCondition.count('_') == 1): #Assuming '_' splits pred and successor
        predecessorSchedule, successorSchedule = scheduleCondition.split('_')
        if (predecessorSchedule.count('-') == 1):   #Assuming '-' is used to split schedule table from schedule job
            predecessorScheduleList = predecessorSchedule.split('-') #Assuming '-' is used to split schedule table from schedule job
            predecessorScheduleName, predecessorScheduleJob = \
                predecessorScheduleList[len(predecessorScheduleList)-2], predecessorScheduleList[len(predecessorScheduleList)-1]
            #To generate flowchart, need to remove any chars until @ in schedule. Else 001@SCHEDNAME will be seperate from SCHEDNAME and that cause duplicate flows in flowchart
#            if '@' in predecessorScheduleName:    #Commenting this line based on some expected values in output
#                predecessorScheduleName = predecessorScheduleName[predecessorScheduleName.find('@') + 1:]   #Commenting this line based on some expected values in output
        else:
            predecessorScheduleName, predecessorScheduleJob = '', predecessorSchedule
            
        if (successorSchedule.count('-') == 1): #Assuming '-' is used to split schedule table from schedule job
            successorScheduleName, successorScheduleJob = successorSchedule.split('-') #Assuming '-' is used to split schedule table from schedule job
        else:
            successorScheduleName, successorScheduleJob = predecessorScheduleName, successorSchedule
                    
    else:
        predecessorSchedule = scheduleCondition
        successorSchedule = predecessorSchedule
    
    ctlmSchedulesDataframe.loc[len(ctlmSchedulesDataframe)] = \
            [predecessorScheduleName.replace('ODAT','').replace('PREV','').replace('DLY',''), \    #Assuming '-' is used to split schedule table from schedule job
             predecessorScheduleJob.replace('ODAT',''), successorScheduleName.replace('ODAT',''), \
             successorScheduleJob.replace('ODAT','')]          
    ctlmSchedulesDataframe = ctlmSchedulesDataframe.drop_duplicates(keep='first') #Removing duplicate schedules


flowChart = Digraph(scheduleName + 'Schedule Flow', filename=scheduleName + '.gv')

flowChart.attr('node', shape='rectangle')
for iteration3 in range(0, len(ctlmSchedulesDataframe)):
    predecessorJobTable = ctlmSchedulesDataframe.at[iteration3, "PredecessorScheduleName"]
    predecessorJobName = ctlmSchedulesDataframe.at[iteration3, "PredecessorScheduleJob"]
    successorJobTable = ctlmSchedulesDataframe.at[iteration3, "SuccessorScheduleName"]
    successorJobName = ctlmSchedulesDataframe.at[iteration3, "SuccessorScheduleJob"]
    
    if ((predecessorJobName != '') and (successorJobName != '')):
        flowChart.edge(ctlmSchedulesDataframe.at[iteration3, "PredecessorScheduleJob"], \
                       ctlmSchedulesDataframe.at[iteration3, "SuccessorScheduleJob"])

flowChart.view()