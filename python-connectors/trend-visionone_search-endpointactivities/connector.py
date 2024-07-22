# This file is the actual code for the custom Python dataset trend-visionone_endpoints-list-get

# import the base class for the custom dataset
import requests
from six.moves import xrange
from dataiku.connector import Connector

class SearchEndpointActivity(Connector):

  def __init__(self, config, plugin_config):
    Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
    self.fqdn = self.plugin_config.get("trendVisionOneFqdn")
    self.authToken = self.plugin_config.get("trendVisionOneApiToken")
    self.startDateTime = config.get("startDateTime")
    self.endDateTime = config.get("endDateTime")
    self.TMV1Query = config.get("TMV1Query")

  def get_read_schema(self):
    return None

  def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit = -1):
    url_base = 'https://' + self.fqdn
    url_path = '/v3.0/search/endpointActivities'
    url = url_base + url_path

    query_params = {
      'startDateTime': self.startDateTime,
      'endDateTime': self.endDateTime,
      'top': '5000'
    }

    headers = {
      'Authorization': 'Bearer ' + self.authToken,
      'TMV1-Query': self.TMV1Query
    }

    # Set a counter to only send query parameters once
    i = 0 

    while True:    
      if i== 0:
        r = requests.get(url, params=query_params, headers=headers)
        i = i + 1
      else:
        # Do not send query parameters again when calling nextLink url.
        r = requests.get(url, headers=headers)

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

