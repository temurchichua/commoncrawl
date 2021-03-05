#!/usr/bin/env python
import json
import requests

credentials = 'tools/credentials.json'
message = 'My first automated slack post'


def get_credentials(credentials=credentials):
    """
    Read credentials from JSON file.
    """
    with open(credentials, 'r') as f:
        creds = json.load(f)
    return creds['slack_webhook']


def post_to_slack(message=message, credentials=credentials):
    data = {'text': message}
    url = get_credentials(credentials)
    requests.post(url, json=data, verify=False)


if __name__ == '__main__':
    post_to_slack(message, credentials)
