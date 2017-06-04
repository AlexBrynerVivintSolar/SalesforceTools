# coding: utf-8
import numpy as np
import pandas as pd
import time
from datetime import datetime, timedelta, date
from time import sleep, gmtime, strftime
from pandas import DataFrame, Series, read_csv

from salesforce_bulk_api import SalesforceBulkJob
from SalesforceBulkQuery import *
from simple_salesforce import *

###################################################################################################

# Salesforce Credentials

# Creates SimpleSalesforce Login Instance
sf = Salesforce(username='', password='', security_token='', sandbox=, client_id='')

###################################################################################################

def getBlankDF():
    return pd.DataFrame(np.nan, index=[], columns=[])

def NameCaseAsTitles(x):
    if (str(x).isupper() or str(x).islower()) and '@' not in str(x):
        return str(x).title()
    else:
        return x
    
def getDate(days):
    return(datetime.today() - timedelta(days=days)).strftime('%Y-%m-%dT00:00:00z') # YYYY-MM-DDThh:mm:ssz


def SFNulls(df, FillWith='#N/A'):
    """
        Description: Fills 0's and NAN's with "#N/A" which is the value that the Salesforce Bulk API recognizes as Null.
        Parameters:
            df = Pandas.DataFrame
        Recognizes 'float64', 'int64', and 'int32' data types.
    """
    df.apply(lambda s: pd.to_numeric(s, errors='ignore'))
    NumCol = df.columns.values.tolist()
    for col in NumCol:
        df[col] = df[col].replace(0, np.NAN).fillna('%s' % FillWith)


def SFQuery(SOQL: str, InList=None, LowerHeaders=False, CheckParentChild=True, KeepAttributes=False):
    """
        Description: Queries Salesforce returning all results in a pandas dataframe.  This also sets all possible data types to numbers and sets column headers to lower case. If using InList, this functionality is built with pandas dataframe columns in mind to help simplify filtering from other SOQL results.
        Parameters:
            SOQL = Salesforce SOQL Statement
            InList* = List of items for an "IN" filter. Apex SOQL - "SELECT Id, Name FROM Account Where Id IN :ids"
                InList filters dups and formats the list for SOQL, and splits up the list into smaller chuncks if needed. 
                SOQL parameter must be written out to the point where the : would be set in a SOQL query in Apex.
                EX: SFQuery("SELECT Id, Name FROM Account WHERE Id IN", IdsList)
                Can also write out additional WHERE statements preceding the InList.
                EX: SFQuery("SELECT Id, Name From Account WHERE Name = 'Alex' and Id IN", IdsList)
                InList format - ['id1', 'id2', 'id2', 'id3', 'id3', 'id4', 'id5'] becomes ('id1', 'id2', 'id3', 'id4', 'id5')
            LowerHeader = Returns Dataframe with column headers lowercase
            CheckParentChild = This checks for the relationships by looking for the ordered dictionaries returned by Salesforce.  It loops through to ensure reached the end of the line if stepping through multiple parent relationships.  Turn off if queries need to run slighly faster.
            
            
            InList* - This is not an efficent use of api calls.  Nested Select statements in the where clause is a more efficent use for api calls but there are always tradeoffs.  At some point it would make more sense to utilize tuples, but unfortunately salesforce did not like the last comma.  
    """
    def basicSOQL(SOQLstr : str):
        # formats the Salesforce ordered dictionary into a pandas dataframe
        try:
            od = sf.query_all("%s" % SOQLstr)
            items = {val: dict(od['records'][val]) for val in range(len(od['records'])) } 
            res = DataFrame.from_dict(items, orient='index')
            if LowerHeaders == True:
                res.columns = map(str.lower, res.columns)
            return res.apply(lambda s: pd.to_numeric(s, errors='ignore'))
        except ValueError:
            pass
    def CreateFilterStr(ListToStr):
        # creates a string from a list 
        # ['id1', 'id2', 'id3', 'id4', 'id5'] -> ('id1', 'id2', 'id3', 'id4', 'id5')
        resStr = "("
        r = 0
        for rl in ListToStr:
            if rl is not None:
                if r == 0:
                    resStr += "'"+str(rl)+"'"
                    r = 1
                elif r == 1:
                    resStr += ",'"+str(rl)+"'"
        resStr += ")"
        return resStr
    def BatchQueryList(toBatchList):
        # filters the list of duplicates then batches the lists in groups
        # [('id1', 'id2', 'id3', id4', 'id5'),('id6', 'id7', 'id8', 'id9', 'id10')]
        batchSize = 300
        newList = list(set(toBatchList))
        listSize = len(newList)
        startPoint = 0
        endPoint = batchSize
        res = []
        while startPoint < listSize:
            tempStr = CreateFilterStr(newList[startPoint:endPoint])
            res.append([tempStr])
            startPoint = endPoint
            endPoint += batchSize
        return res
    def InListQuery(SOQL, InList):
        # runs a query for each list from the batched lists and stacks the results
        filterLists = BatchQueryList(InList)

        resDF = None
        i = 0
        for i in range(0,len(filterLists)):
            tempDF = basicSOQL(SOQLstr = "%s %s" % (SOQL, filterLists[i][0]))
            try: resDF = resDF.append(tempDF, ignore_index=True)
            except AttributeError: resDF = tempDF
            i += 1
        return resDF
    def getChildRecords(obj, row):
        if row == None:
            return None

        size = row.get('totalSize')
        records = row.get('records')
        tempDic = {}

        for i in range(0,size):
            tempDic[i] = {}
            for field in records[i].keys():
                try:
                    records[i].get(field).keys()
                    continue
                except AttributeError:
                    pass
                tempDic[i][obj + '.' + field] = records[i].get(field)
        return tempDic
    def getParentRecords(field, row):
        if row == None:
            return None
        else:
            return row.get(field)
    
    rs = None
    if InList == None:
        rs = basicSOQL(SOQL)
    else:
        InList = list(InList)
        rs = InListQuery(SOQL, InList)
    
    # Drops the attributes column passed through by Salesforce
    if CheckParentChild == False and KeepAttributes == False:
        rs = rs.drop(['attributes'], axis=1)
        
    while CheckParentChild:
        CheckParentChild = False

        indexCols = []
        for col in rs:
            obj = None
            relationship = None
            for i in range(len(rs[col])):
                # scans down each column until finding an ordered dict to parse
                if rs[col][i] == None:
                    continue
                try:
                    if rs[col][i].get('type') != None and col == 'attributes':
                        if KeepAttributes == False:
                            rs = rs.drop([col], axis=1)
                        break
                except AttributeError:
                    indexCols.append(col)
                    break

                # Determines whether parent or child query and the object type
                try:
                    obj = rs[col][i].get('attributes').get('type')
                except:
                    pass
                try:
                    obj = rs[col][i].get('records')[0].get('attributes').get('type')
                except:
                    pass
                relationship = 'Child' if rs[col][i].get('totalSize') != None else 'Parent'
                break


            if relationship == 'Child' and obj != None:
                rs[col] = rs.apply(lambda row: getChildRecords(obj, row[col]), axis=1)

            elif relationship == 'Parent' and obj != None:
                fields = []
                for i in range(len(rs[col])):
                    if rs[col][i] != None:
                        fields.extend(list(rs[col][i].keys()))
                        fields = list(set(fields))
                        
                if KeepAttributes == False:
                    try:
                        fields.remove('attributes')
                    except ValueError:
                        pass
                for field in fields:
                    rs[obj + '.' + field] = rs.apply(lambda row: getParentRecords(field, row[col]), axis=1)
                rs = rs.drop([col], axis=1)

                CheckParentChild = True

        # next I'd like to setup an option for child relationship queries to return a multi indexed dataframe
        # print(indexCols)
    return rs
        
           
def SFFormat(df, SObject, EnforceNulls=False):
    """
        Description: Looks up data types and dynamically formats columns to a correct format for the Bulk Api. Returns error messages for invalid data types or column headers.  If EnforceNulls is true fills all blanks with #N/A, if false will set blanks to ''.
        Parameters:
            df = Pandas.DataFrame
            SObject = Type of object for the upload. Ex: 'Account'
            EnforceNulls = If true will fill blanks with #N/A to set as null in Salesforce
            
        *Currently only formats dates and datetimes
    """
    NoFieldError = ''
    InvalidDataError = ''
    
    df.columns = map(str.lower, df.columns)
    
    fieldDict = getattr(sf, '%s' % SObject).describe()["fields"]
    
    numFields = len(fieldDict)
    
    NumCol = df.columns.values.tolist()
    for col in NumCol:
        i = 0
        for x in fieldDict:
            if x['name'].lower() == col:
                dtype = x['type']
                length = x['length']
                
                try:
                    if dtype == 'date':
                        df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d').replace(to_replace='NaT', value='#N/A') 
                    elif dtype == 'datetime':
                        df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%dT%H:%M:%S').replace(to_replace='NaT', value='#N/A')
                except ValueError: 
                    InvalidDataError += ("Invalid "+dtype+" : "+col+"\n")
                    
                break
                
            i += 1
            if i >= numFields:
                NoFieldError += (SObject+" does not contain : "+col+"\n")
                
    SFNulls(df)
    
    if EnforceNulls == False:
        for col in NumCol:
            df[col] = df[col].replace('#N/A','')
    
    errors = NoFieldError+InvalidDataError
    if len(errors) > 0:
        return(errors)
    else:
        return('No Errors')

    
def SFUpload(df, UploadType, Sobject, batchSize=49995, hangtime=0):
    """
        Description: Upload a pandas dataframe through the Salesforce Bulk API in batches of 49995 (5 batches of 9999). Can run either an insert or update to the listed Sobject.  Sobject and UploadType must be listed as a string. ex: 'Update', 'Account'  
        Parameters:
            df         = Pandas.DataFrame
            UploadType = Update or Insert
            Sobject    = Salesforce object in the upload. Ex - Accounts, Contact
            batchSize  = Number of rows that the upload will run before submitting the next group of rows in the dataset. Defaults to 49,995 (5 batches of 9999)
            hangtime   = Number of seconds to wait before uploading a new batch. Defaults to 0.
    """

    if len(df) == 0:
        return
    
    startRow = 0
    endRow = batchSize

    while startRow < len(df):
        upload = df[startRow:endRow]

        Headers = upload.columns.tolist()
        Data = upload.to_records(index=False)
        job = SalesforceBulkJob(UploadType, Sobject, salesforce=sf)
        job.upload(Headers,Data)

        startRow = endRow
        endRow = startRow + batchSize
        time.sleep(hangtime)
        

def SFBulkQuery(SObject, SOQL):
    """
        Description: Runs a query through the bulk api.  Creates, Tracks, and Closes the Request and returns the results as a Pandas Dataframe.  Currently there are lots of slighly obnoxious messages to help with tracking the current status.
        Parameters:
            SObject = Salesforce Object, ex: Account, Contact
            SOQL    = Salesforce SOQL Statement for bulk query
    """
    sfbulk = SalesforceBulk(sessionId=sf.session_id, host=sf.sf_instance)
    job = sfbulk.create_query_job(SObject, contentType='CSV')
    batch = sfbulk.query(job, SOQL)
    while not sfbulk.is_batch_done(job, batch):
        time.sleep(10)
    sfbulk.close_job(job)
    res = sfbulk.get_batch_result_iter(job, batch)
    return res
