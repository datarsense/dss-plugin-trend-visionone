# -*- coding: utf-8 -*-
import datetime
import pandas as pd
import requests

from dataiku.customrecipe import get_plugin_config, get_recipe_config, get_input_names_for_role, get_output_names_for_role
from helpers import raise_if_apitoken_missing, getTrendVisionOneData

import logging
import time

def get_input_dataset(role):
    names = get_input_names_for_role(role)
    return dataiku.Dataset(names[0]) if len(names) > 0 else None


def get_output_dataset(role):
    names = get_output_names_for_role(role)
    return dataiku.Dataset(names[0]) if len(names) > 0 else None


def remove_endpoint(fqdn, token, agentGuid):
  url_base = 'https://' + fqdn
  url_path = '/v3.0/endpointSecurity/endpoints/delete'
  url = url_base + url_path

  headers = {
    'Authorization': 'Bearer ' + token,
    'Content-Type': 'application/json;charset=utf-8'
  }

  body = [
    {
      'agentGuid': agentGuid
    }
  ]

  logging.info("Removing endpoint - " + agentGuid)

  i = 0
  while i < 5:
    r = requests.post(url, headers=headers, json=body)

    if r.status_code >= 200 and r.status_code < 300 and 'application/json' in r.headers.get('Content-Type', '') and len(r.content):
      return True
    elif r.status_code == 429 or r.status_code == 504 or r.status_code == 599:
      time.sleep(15)
      i = i + 1 
    else:
      raise Exception('Error when calling Trend Vision One API. Error code:' + str(r.status_code) + ". Error message: " + r.text)


# Read plugin parameters
# Raise an error if API key is not defined in plugin parameters
plugin_config = get_plugin_config()
fqdn = plugin_config.get("trendVisionOneFqdn")
authToken = plugin_config.get("trendVisionOneApiToken")

raise_if_apitoken_missing(authToken)


# Read recipe config
input_dataset = get_input_dataset('Input Dataset')
output_dataset = get_output_dataset('Output Dataset')

recipe_config = get_recipe_config()
endpointGuid = recipe_config['endpointGuid']

# Recipe input
input_df = input_dataset.get_dataframe()
logging.info("Remove endpoints - Dataset loaded")

# Create output dataframe
input_df.apply(lambda x: remove_endpoint(fqdn, authToken, x[endpointGuid]), axis=1)

logging.info("Remove endpoints - end")

# Write output dataframe
output_df = input_df
output_dataset.write_with_schema(output_df)


