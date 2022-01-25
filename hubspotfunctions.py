"""
Helper functions to extract data from HubSpot via API
"""

import json
import time
from datetime import datetime
from datetime import timedelta

import pandas as pd
import requests
from hubspot import HubSpot

json_file = open(r"hubspot-key.json", encoding='utf-8')
api_key = json.load(json_file)["hubspot-api"]
api_client = HubSpot(api_key=api_key)

# Create a function to download every contact that has either been created or submitted an application
# since the beginning of 2021


def get_engaged_contacts(contact_properties, _utc_time):
    """
    Return all HubSpot users who've engaged since _utc_time
    :param contact_properties: List[str] list of strings naming contact properties
    :param _utc_time: (datetime) datetime in UTC zulu time
    :return: (df) Returns a dataframe of all contacts and their contact_properties
    """
    # Define variables needed for the HubSpot API
    _url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    _headers = {"Content-Type": "application/json"}
    _querystring = {"hapikey": api_key}
    _df = pd.DataFrame()

    # Define date ranges and convert them to UTC time
    if _utc_time == "":
        _utc_time = datetime.strptime(
            "2021-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"
        )

    _epoch_start = int((_utc_time - datetime(1970, 1, 1)).total_seconds() * 1000)
    _current_time = timedelta(days=-1) + datetime.now().date()
    _epoch_end = int((_current_time - datetime(1970, 1, 1)).total_seconds() * 1000)
    _loops = int(
        ((datetime.now().year - datetime(2021, 1, 1).year) * 12) + datetime.now().month
    )
    _epoch_step = int((_epoch_end - _epoch_start) / _loops)

    for _loop in range(0, _loops):
        print(f"Executing loop {_loop} of {_loops}")
        _epoch_end = _epoch_start + _epoch_step
        _paging_str = "0"

        while _paging_str != "":
            time.sleep(0.067)
            _payload = {
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "last_application_submit_date",
                                "operator": "GT",
                                "value": _epoch_start,
                            },
                            {
                                "propertyName": "last_application_submit_date",
                                "operator": "LTE",
                                "value": _epoch_end,
                            },
                        ]
                    },
                    {
                        "filters": [
                            {
                                "propertyName": "createdate",
                                "operator": "GT",
                                "value": _epoch_start,
                            },
                            {
                                "propertyName": "createdate",
                                "operator": "LTE",
                                "value": _epoch_end,
                            },
                        ]
                    },
                ],
                "properties": contact_properties,
                "limit": 100,
                "after": _paging_str,
            }

            _response = requests.post(
                _url, headers=_headers, params=_querystring, data=json.dumps(_payload)
            )
            _df = pd.concat([_df, pd.json_normalize(_response.json(), "results")])
            _total_str = _response.json().get("total")

            try:
                _paging_str = _response.json().get("paging").get("next").get("after")
                print(f"On  {int(_paging_str):,}  of  {int(_total_str):,}")
            except Exception:
                _paging_str = ""
                print("End of list")

        _epoch_start += _epoch_step

    _df.reset_index(inplace=True, drop=True)
    _df.drop_duplicates("id", inplace=True)
    return _df


# Create a function to download every deal created in 2021
def get_deals(deal_properties):
    """
    Return the properties given by deal_properties for all HubSpot deals post-2021
    :param deal_properties: (List[str]) A list of strings defining the deal properties to return.
    :return: (df) A dataframe of all deals and their properties defined by deal_properties
    """
    # Define variables needed for the HubSpot API
    _url = "https://api.hubapi.com/crm/v3/objects/deals/search"
    _headers = {"Content-Type": "application/json"}
    _querystring = {"hapikey": api_key, "includePropertyVersions": True}
    _df = pd.DataFrame()

    # Define date ranges and convert them to UTC time
    _utc_time = datetime.strptime("2021-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
    _epoch_start = int((_utc_time - datetime(1970, 1, 1)).total_seconds() * 1000)
    _current_time = timedelta(days=-1) + datetime(
        datetime.now().year, datetime.now().month, datetime.now().day
    )
    _epoch_end = int((_current_time - datetime(1970, 1, 1)).total_seconds() * 1000)
    _loops = int(datetime.now().month)
    _epoch_step = int((_epoch_end - _epoch_start) / _loops)

    for _loop in range(0, _loops):
        print(f"Executing loop {_loop} of {_loops}")
        _epoch_end = _epoch_start + _epoch_step
        _paging_str = "0"

        while _paging_str != "":
            time.sleep(0.067)
            _payload = {
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "createdate",
                                "operator": "GT",
                                "value": _epoch_start,
                            },
                            {
                                "propertyName": "createdate",
                                "operator": "LTE",
                                "value": _epoch_end,
                            },
                        ]
                    }
                ],
                "properties": deal_properties,
                "limit": 100,
                "after": _paging_str,
            }

            _response = requests.post(
                _url, headers=_headers, params=_querystring, data=json.dumps(_payload)
            )
            _df = pd.concat([_df, pd.json_normalize(_response.json(), "results")])
            _total_str = _response.json().get("total")

            try:
                _paging_str = _response.json().get("paging").get("next").get("after")
                print(f"On  {int(_paging_str):,}  of  {int(_total_str):,}")
            except Exception:
                _paging_str = ""
                print("End of list")

        _epoch_start += _epoch_step

    _df.reset_index(inplace=True, drop=True)
    _df.drop_duplicates("id", inplace=True)
    return _df


# Get all deals associated with a contact
def get_associated_contacts(df, column):
    """
    Return the contacts associated with HubSpot deal IDs in a dataframe column.
    :param df: (df) The dataframe containing the HubSpot deal IDs
    :param column: (str) The name of the column in df that contains the HubSpot deal IDs
    :return: (df) Returns a dataframe with all HubSpot deal IDs and their associated contact IDs
    """
    _df = pd.DataFrame(columns=["deal.id", "contact.id"])

    for i in df.index:
        _deal_id = df.loc[i, column]
        _df.loc[i, "deal.id"] = _deal_id
        try:
            api_response = api_client.crm.deals.associations_api.get_all(
                deal_id=_deal_id, to_object_type="contacts", limit=500
            )
            _df.loc[i, "contact.id"] = api_response.results[0].id
        except Exception:
            print(f"Deal {_deal_id} has no associated contacts.")

    return _df


# Get all paying customers
def get_customers(contact_properties):
    """
    Return a dataframe of all paying customers.
    :param contact_properties: (List[str]) A list of strings defining the HubSpot contact properties to return.
    :return: (df) A dataframe with all customers and their HubSpot properties defined in contact_properties.
    """
    # Define variables needed for the HubSpot API
    _url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    _headers = {"Content-Type": "application/json"}
    _querystring = {"hapikey": api_key}
    _paging_str = 0
    _df = pd.DataFrame()

    while _paging_str != "":
        time.sleep(0.067)
        _payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "programs_confirmed",
                            "operator": "HAS_PROPERTY",
                        }
                    ]
                }
            ],
            "properties": contact_properties,
            "limit": 100,
            "after": _paging_str,
        }

        _response = requests.post(
            _url, headers=_headers, params=_querystring, data=json.dumps(_payload)
        )
        _df = pd.concat([_df, pd.json_normalize(_response.json(), "results")])
        _total_str = _response.json().get("total")

        try:
            _paging_str = _response.json().get("paging").get("next").get("after")
            print(f"On  {int(_paging_str):,}  of  {int(_total_str):,}")
        except Exception:
            _paging_str = ""
            print("End of list")

    _df.reset_index(inplace=True, drop=True)
    _df.drop_duplicates("id", inplace=True)
    return _df


# Get all email events for a particular email recipient
def get_email_events(recipient_list):
    """
    Return a dataframe of all email events associated with HubSpot emails.
    :param recipient_list: (List) List of emails that HubSpot has sent to.
    :return: (df) Dataframe of all emails sent to given contacts and the actions those contacts took.
    """
    url = "https://api.hubapi.com/email/public/v1/events"
    headers = {"accept": "application/json"}
    _df = pd.DataFrame()

    for _recipient in recipient_list:
        print(
            f"Checking email history for {_recipient} -- Record {recipient_list.index(_recipient)} of {len(recipient_list)}."
        )
        _querystring = {"hapikey": api_key, "recipient": _recipient}
        _more_state = True
        try:
            while _more_state is True:
                time.sleep(0.067)
                _response = requests.request(
                    "GET", url, headers=headers, params=_querystring
                )
                _df = pd.concat([_df, pd.json_normalize(_response.json(), "events")])

                _more_state = _response.json().get("hasMore")

                if _more_state is True:
                    _offset_str = _response.json().get("offset")
                    _querystring = {
                        "hapikey": api_key,
                        "recipient": _recipient,
                        "offset": _offset_str,
                    }
        except Exception as e:
            print(e)

    _df.reset_index(inplace=True, drop=True)
    _df["created"] = datetime.utcfromtimestamp(_df.loc[0, "created"] / 1000)
    _df["sentBy.created"] = datetime.utcfromtimestamp(
        _df.loc[0, "sentBy.created"] / 1000
    )
    return _df
