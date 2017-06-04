import xml.etree.ElementTree as ET
import io
from httplib2 import Http
import re
import csv
import pandas as pd
import time
from time import sleep
from pandas import DataFrame
import csv
import requests
import bulk_states

class BulkApiError(Exception):

    def __init__(self, message, status_code=None):
        super(BulkApiError, self).__init__(message)
        self.status_code = status_code


class BulkJobAborted(BulkApiError):

    def __init__(self, job_id):
        self.job_id = job_id

        message = 'Job {0} aborted'.format(job_id)
        super(BulkJobAborted, self).__init__(message)


class BulkBatchFailed(BulkApiError):

    def __init__(self, job_id, batch_id, state_message):
        self.job_id = job_id
        self.batch_id = batch_id
        self.state_message = state_message

        message = 'Batch {0} of job {1} failed: {2}'.format(batch_id, job_id,
                                                            state_message)
        super(BulkBatchFailed, self).__init__(message)

class SalesforceBulk(object):

    def __init__(self, sessionId=None, host=None, API_version="39.0",
                 exception_class=BulkApiError):
        self.endpoint = "https://" + host + "/services/async/%s" % API_version
        self.sessionId = sessionId
        self.jobNS = 'http://www.force.com/2009/06/asyncapi/dataload'
        self.jobs = {}  # dict of job_id => job_id
        self.batches = {}  # dict of batch_id => job_id
        self.batch_statuses = {}
        self.exception_class = exception_class
        
    def headers(self, values={}):
        default = {"X-SFDC-Session": self.sessionId,
                   "Content-Type": "application/xml; charset=UTF-8"}
        for k, val in values.items():
            default[k] = val
        return default
        
    def create_job(self, object_name=None, operation=None, contentType='CSV',
                   concurrency=None, external_id_name=None):
        assert(object_name is not None)
        assert(operation is not None)

        doc = self.create_job_doc(object_name=object_name,
                                  operation=operation,
                                  contentType=contentType,
                                  concurrency=concurrency,
                                  external_id_name=external_id_name)

        http = Http()
        resp, content = http.request(self.endpoint + "/job",
                                     "POST",
                                     headers=self.headers(),
                                     body=doc)

        self.check_status(resp, content)

        tree = ET.fromstring(content)
        job_id = tree.findtext("{%s}id" % self.jobNS)
        self.jobs[job_id] = job_id

        return job_id
    
    def close_job(self, job_id):
        doc = self.create_close_job_doc()
        http = Http()
        url = self.endpoint + "/job/%s" % job_id
        resp, content = http.request(url, "POST", headers=self.headers(),
                                     body=doc)
        self.check_status(resp, content)
    
    def create_job_doc(self, object_name=None, operation=None,
                       contentType='CSV', concurrency=None, external_id_name=None):
        root = ET.Element("jobInfo")
        root.set("xmlns", self.jobNS)
        op = ET.SubElement(root, "operation")
        op.text = operation
        obj = ET.SubElement(root, "object")
        obj.text = object_name
        if external_id_name:
            ext = ET.SubElement(root, 'externalIdFieldName')
            ext.text = external_id_name

        if concurrency:
            con = ET.SubElement(root, "concurrencyMode")
            con.text = concurrency
        ct = ET.SubElement(root, "contentType")
        ct.text = contentType
        
        buf = io.BytesIO()
        tree = ET.ElementTree(root)
        tree.write(buf, encoding="UTF-8")
        return buf.getvalue().decode('UTF-8')
    
    def create_close_job_doc(self):
        root = ET.Element("jobInfo")
        root.set("xmlns", self.jobNS)
        state = ET.SubElement(root, "state")
        state.text = "Closed"

        buf = io.BytesIO()
        tree = ET.ElementTree(root)
        tree.write(buf, encoding="UTF-8")
        return buf.getvalue().decode('UTF-8')
        
    def is_batch_done(self, job_id, batch_id):
        batch_state = self.batch_state(job_id, batch_id, reload=True)
        if batch_state in bulk_states.ERROR_STATES:
            status = self.batch_status(job_id, batch_id)
            raise BulkBatchFailed(job_id, batch_id, status['stateMessage'])
        return batch_state == bulk_states.COMPLETED
    
    def batch_status(self, job_id=None, batch_id=None, reload=False):
        if not reload and batch_id in self.batch_statuses:
            return self.batch_statuses[batch_id]

        job_id = job_id or self.lookup_job_id(batch_id)

        http = Http()
        uri = self.endpoint + \
            "/job/%s/batch/%s" % (job_id, batch_id)
        resp, content = http.request(uri, headers=self.headers())
        self.check_status(resp, content)

        tree = ET.fromstring(content)
        result = {}
        for child in tree:
            result[re.sub("{.*?}", "", child.tag)] = child.text

        self.batch_statuses[batch_id] = result
        return result
    
    def check_status(self, resp, content):
        if resp.status >= 400:
            msg = "Bulk API HTTP Error result: {0}".format(content)
            self.raise_error(msg, resp.status)

    def batch_state(self, job_id, batch_id, reload=False):
        status = self.batch_status(job_id, batch_id, reload=reload)
        if 'state' in status:
            return status['state']
        else:
            return None
        
    # Register a new Bulk API job - returns the job id
    def create_query_job(self, object_name, **kwargs):
        return self.create_job(object_name, "query", **kwargs)
    
    # Add a BulkQuery to the job - returns the batch id
    def query(self, job_id, soql):
        if job_id is None:
            job_id = self.create_job(
                re.search(re.compile("from (\w+)", re.I), soql).group(1),
                "query")
        http = Http()
        uri = self.endpoint + "/job/%s/batch" % job_id
        headers = self.headers({"Content-Type": "text/csv"})
        resp, content = http.request(uri, method="POST", body=soql,
                                     headers=headers)

        self.check_status(resp, content)

        tree = ET.fromstring(content)
        batch_id = tree.findtext("{%s}id" % self.jobNS)

        self.batches[batch_id] = job_id

        return batch_id
    
    def get_batch_result_iter(self, job_id, batch_id, parse_csv=False,
                              logger=None):
        """
        Return a line interator over the contents of a batch result document. If
        csv=True then parses the first line as the csv header and the iterator
        returns dicts.
        """
        status = self.batch_status(job_id, batch_id)
        if status['state'] != 'Completed':
            return None
        elif logger:
            if 'numberRecordsProcessed' in status:
                logger("Bulk batch %d processed %s records" %
                       (batch_id, status['numberRecordsProcessed']))
            if 'numberRecordsFailed' in status:
                failed = int(status['numberRecordsFailed'])
                if failed > 0:
                    logger("Bulk batch %d had %d failed records" %
                           (batch_id, failed))

        uri = self.endpoint + \
            "/job/%s/batch/%s/result" % (job_id, batch_id)
        r = requests.get(uri, headers=self.headers(), stream=True)
        print(r.text)
        
        result_id = r.text.split("<result>")[1].split("</result>")[0]

        uri = self.endpoint + \
            "/job/%s/batch/%s/result/%s" % (job_id, batch_id, result_id)
        download = requests.get(uri, headers=self.headers(), stream=True)
        print('Downloaded Results')
        
        decoded_content = download.content.decode('utf-8')
        print('Decoded Results')
        
        dictR = csv.DictReader(decoded_content.splitlines(), delimiter=',')
        i = 0
        items = {}
        for row in dictR:
            items[i] = row
            i += 1
            print('\rRow : %s' % i, end="")
        print('\nCreated Dictionary')
            
        res = DataFrame.from_dict(items, orient='index')
        print('Created Dataframe')
        return res