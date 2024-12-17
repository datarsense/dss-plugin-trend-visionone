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

def enrich_with_edr_telemetry(fqdn, token, event_date, hoursOffset, src_ip, src_port, dst_ip, dst_port, proto):
  url_base = 'https://' + fqdn
  url_path = '/v3.0/search/endpointActivities'
  url = url_base + url_path

  TMV1Query = "src:" + str(src_ip) + " and spt:" + str(src_port) + " and dst:" + str(dst_ip) + " and dpt:" + str(dst_port)
  logging.info("EDR TMV1Query: " + TMV1Query)

  if hoursOffset != 0:
    time_offset = datetime.timedelta(hours = hoursOffset)
    base_date = event_date + time_offset
  else:
    base_date = event_date
  
  eventIsoDateTime = event_date.replace(microsecond=0).replace(tzinfo=None).isoformat()
  time_change = datetime.timedelta(minutes=10)
  startIsoDateTime = (base_date - time_change).replace(microsecond=0).replace(tzinfo=None).isoformat() + 'Z'
  endIsoDateTime = (base_date + time_change).replace(microsecond=0).replace(tzinfo=None).isoformat() + 'Z'

  logging.info("Start date: " + str(startIsoDateTime))
  logging.info("Event date: " + str(eventIsoDateTime))
  logging.info("End date: " + str(endIsoDateTime))


  query_params = {
    'startDateTime': startIsoDateTime, #Format: "2024-12-11T00:00:00Z"
    'endDateTime': endIsoDateTime, #Format: "2024-12-12T06:10:00Z"
    'top': '5'
  }

  headers = {
    'Authorization': 'Bearer ' + token,
    'TMV1-Query': TMV1Query
  }

  edr_data = getTrendVisionOneData(url, query_params, headers)
  logging.info("EDR data: " + str(edr_data))
  return edr_data

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
srcIp = recipe_config['srcIp']
srcPort = recipe_config['srcPort']
dstIp = recipe_config['dstIp']
dstPort = recipe_config['dstPort']
ipProto = recipe_config['ipProto']
eventDate = recipe_config['eventDate']
edrFirewallTimedelta = recipe_config['edrFirewallTimedelta']

# Recipe input
input_df = input_dataset.get_dataframe()
logging.info("Enrich with EDR telemetry data - Dataset loaded")

# Create output dataframe
output_df = input_df
output_df["EDR"] = output_df.apply(lambda x: enrich_with_edr_telemetry(fqdn, authToken, x[eventDate], edrFirewallTimedelta, x[srcIp], x[srcPort], x[dstIp], x[dstPort], x[ipProto]), axis=1)

logging.info("Enrich with EDR telemetry data - Enrichment end")

# Write output dataframe
output_dataset.write_with_schema(output_df)


