# This file is the actual code for the custom Python dataset trend-visionone_endpoints-list-get

# import the base class for the custom dataset
import requests
from six.moves import xrange
from dataiku.connector import Connector
from helpers import raise_if_apitoken_missing, streamTrendVisionOneData

class SearchDetections(Connector):

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
    # Raise an error if API auth token is missing
    raise_if_apitoken_missing(self.authToken)

    url_base = 'https://' + self.fqdn
    url_path = '/v3.0/search/detections'
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

    yield from streamTrendVisionOneData(url, query_params, headers)

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

