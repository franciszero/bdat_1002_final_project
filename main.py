import requests
import base64

# Twitter API credentials
CONSUMER_KEY = '4duXKJl8KeqacDBWtlO9dPMmu'
CONSUMER_SECRET = 'AYsUnr7Ps6sV78dERBUkoIHtlHiki5ER1VfkpUelNEVFhBEDW8'


def obtain_bearer_token(consumer_key, consumer_secret):
    # Encode consumer key and secret
    key_secret = '{}:{}'.format(consumer_key, consumer_secret).encode('ascii')
    b64_encoded_key = base64.b64encode(key_secret)
    b64_encoded_key = b64_encoded_key.decode('ascii')

    # Endpoint URL
    auth_url = 'https://api.twitter.com/oauth2/token'

    # Headers
    auth_headers = {
        'Authorization': 'Basic {}'.format(b64_encoded_key),
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
    }

    # Body
    auth_data = {
        'grant_type': 'client_credentials'
    }

    # Post request
    response = requests.post(auth_url, headers=auth_headers, data=auth_data)

    if response.status_code == 200:
        token_data = response.json()
        return token_data['access_token']
    else:
        print("Error:", response.status_code, response.text)
        return None


# AAAAAAAAAAAAAAAAAAAAAKcEpQEAAAAAVgRtgbvidkVFwJZ5nEndv9vQ2HU%3DInBBHzg4bnfqTA557Lq8ZUMRDhEPomWrkq4hTHuAlQEv9SiMPI
bearer_token = obtain_bearer_token(CONSUMER_KEY, CONSUMER_SECRET)
if bearer_token:
    print("Bearer Token:", bearer_token)
else:
    print("Failed to obtain bearer token!")
