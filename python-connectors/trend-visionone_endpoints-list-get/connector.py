# This file is the actual code for the custom Python dataset trend-visionone_endpoints-list-get

# import the base class for the custom dataset
import requests
from six.moves import xrange
from dataiku.connector import Connector

class GetEndpointList(Connector):

  def __init__(self, config, plugin_config):
    Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
    self.fqdn = self.plugin_config.get("trendVisionOneFqdn")
    self.authToken = self.plugin_config.get("trendVisionOneApiToken")
    self.TMV1Filter = config.get("TMV1 Filter")

  def get_read_schema(self):
    return {
      "columns" : [
        { "name" : "endpointName", "type" : "string" },
        { "name" : "agentGuid", "type" : "string" },
        { "name" : "displayName", "type" : "string" },
        { "name" : "osName", "type" : "string" },
        { "name" : "osVersion", "type" : "string" },
        { "name" : "osKernelVersion", "type" : "string" },
        { "name" : "osArchitecture", "type" : "string" },
        { "name" : "lastUsedIp", "type" : "string" },
        { "name" : "cpuArchitecture", "type" : "string" },
        { "name" : "lastLoggedOnUser", "type" : "string" },
        { "name" : "isolationStatus", "type" : "string" },
        { "name" : "ipAddresses", "type" : "array" },
        { "name" : "serialNumber", "type" : "string" },
        { "name" : "eppAgent", "type" : "object" },
        { "name" : "edrSensor", "type" : "object" }
      ]
    }

  def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit = -1):
    url_base = 'https://' + self.fqdn
    url_path = '/v3.0/endpointSecurity/endpoints'
    url = url_base + url_path

    query_params = {
      'top': '1000'
    }

    # Add a TMV1-Filter header only if "TMV1 Filter" dataset parameter is not empty
    if self.TMV1Filter is None or self.TMV1Filter.strip() == "":
      headers = {
        'Authorization': 'Bearer ' + self.authToken
      }
    else:
      headers = {
        'Authorization': 'Bearer ' + self.authToken,
        'TMV1-Filter': self.TMV1Filter
      }

    while True:    
      r = requests.get(url, params=query_params, headers=headers)

      if r.status_code == 200 and 'application/json' in r.headers.get('Content-Type', '') and len(r.content):
        data = r.json()

        # Stream data
        items = data['items']
        for i in items:
          yield i

        # Process next page
        if 'nextLink' in data:
          url = data['nextLink']
        else:
          break
      else:
        raise Exception('Error when calling Trend Vision One API. Error code:' + str(r.status_code) + ". Error message: " + r.text)

  def get_writer(self, dataset_schema=None, dataset_partitioning=None, partition_id=None):
    raise NotImplementedError


  def get_partitioning(self):
    raise NotImplementedError


  def list_partitions(self, partitioning):
    return []


  def partition_exists(self, partitioning, partition_id):
    raise NotImplementedError


  def get_records_count(self, partitioning=None, partition_id=None):
    raise NotImplementedError

