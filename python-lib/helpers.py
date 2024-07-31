import requests
import time

# Raise an error if API authentication token is null or contains only blank chars
def raise_if_apitoken_missing(token):
    if token is None or token.strip() == "":
      raise Exception('Error : API token missing')
    
def streamTrendVisionOneData(url, query_params, headers):
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
      elif r.status_code == 429 or r.status_code == 504:
        time.sleep(15)
      else:
        raise Exception('Error when calling Trend Vision One API. Error code:' + str(r.status_code) + ". Error message: " + r.text)