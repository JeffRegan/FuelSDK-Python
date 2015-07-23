import os
import logging
import time
import json

import jwt
import requests
import suds.client
import suds.wsse
from suds.sax.element import Element

from .config import exact_target_config
from .objects import ETDataExtension, ETSubscriber

TIMEOUT = 600  # make the timeout 10 minutes


class ETClient(object):
    """
    Setup web service connectivity by getting need config data, security tokens etc.
    """

    debug = False
    client_id = None
    client_secret = None
    app_signature = None
    wsdl_file_url = None
    auth_token = None
    internal_auth_token = None
    auth_token_expiration = None  # seconds since epoch that the current jwt token will expire
    refresh_key = None
    endpoint = None
    auth_obj = None
    soap_client = None
    auth_url = None

    # get_server_wsdl - if True and a newer WSDL is on the server than the local filesystem retrieve it
    def __init__(self, get_server_wsdl=False, debug=False, params=None):
        self.debug = debug
        if debug:
            logging.basicConfig(level=logging.INFO)
            logging.getLogger('suds.client').setLevel(logging.DEBUG)
            logging.getLogger('suds.transport').setLevel(logging.DEBUG)
            logging.getLogger('suds.xsd.schema').setLevel(logging.DEBUG)
            logging.getLogger('suds.wsdl').setLevel(logging.DEBUG)
        else:
            logging.getLogger('suds').setLevel(logging.INFO)

        # Read the config information out of config.python
        config = exact_target_config

        # client id
        if params is not None and 'clientid' in params:
            self.client_id = params['clientid']
        else:
            self.client_id = config.client_id

        # client secret
        if params is not None and 'clientsecret' in params:
            self.client_secret = params['clientsecret']
        else:
            self.client_secret = config.client_secret

        # app signature
        if params is not None and 'appsignature' in params:
            self.app_signature = params['appsignature']
        else:
            self.app_signature = config.app_signature

        # default wsdl
        if params is not None and 'defaultwsdl' in params:
            wsdl_server_url = params['defaultwsdl']
        else:
            wsdl_server_url = config.default_wsdl

        # authentication url
        if params is not None and 'authenticationurl' in params:
            self.auth_url = params['authenticationurl']
        else:
            self.auth_url = config.authentication_url

        # wsdl file location
        if params is not None and 'wsdl_file_local_loc' in params:
            wsdl_file_local_location = params['wsdl_file_local_loc']
        else:
            wsdl_file_local_location = config.wsdl_file_local_loc

        self.wsdl_file_url = self.load_wsdl(wsdl_server_url, wsdl_file_local_location, get_server_wsdl)

        # get the JWT from the params if passed in...or go to the server to get it
        if params is not None and 'jwt' in params:
            decoded_jwt = jwt.decode(params['jwt'], self.app_signature)
            self.auth_token = decoded_jwt['request']['user']['oauthToken']
            self.auth_token_expiration = time.time() + decoded_jwt['request']['user']['expiresIn']
            self.internal_auth_token = decoded_jwt['request']['user']['internalOauthToken']
            if 'refreshToken' in decoded_jwt:
                self.refresh_key = decoded_jwt['request']['user']['refreshToken']
            self.build_soap_client()
        else:
            self.refresh_token()

    def load_wsdl(self, wsdl_url, wsdl_file_local_location, get_server_wsdl=False):
        """
        retrieve the url of the ExactTarget wsdl...either file: or http:
        depending on if it already exists locally or server flag is set and
        server has a newer copy
        """
        if wsdl_file_local_location is not None:
            file_location = wsdl_file_local_location
        else:
            path = os.path.dirname(os.path.abspath(__file__))
            file_location = os.path.join(path, 'ExactTargetWSDL.xml')
        file_url = 'file:///' + file_location

        # if there is no local copy or local copy is empty then go get it
        if not os.path.exists(file_location) or os.path.getsize(file_location) == 0:
            self.retrieve_server_wsdl(wsdl_url, file_location)
        elif get_server_wsdl:
            r = requests.head(wsdl_url)
            if r is not None and 'last-modified' in r.headers:
                server_wsdl_updated = time.strptime(r.headers['last-modified'], '%a, %d %b %Y %H:%M:%S %Z')
                file_wsdl_updated = time.gmtime(os.path.getmtime(file_location))
                if server_wsdl_updated > file_wsdl_updated:
                    self.retrieve_server_wsdl(wsdl_url, file_location)

        return file_url

    def retrieve_server_wsdl(self, wsdl_url, file_location):
        """
        get the WSDL from the server and save it locally
        """
        r = requests.get(wsdl_url, timeout=TIMEOUT)
        f = open(file_location, 'w')
        f.write(r.text)

    def build_soap_client(self):
        if self.endpoint is None:
            self.endpoint = self.determine_stack()

        self.auth_obj = {'oAuth': {'oAuthToken': self.internal_auth_token},
                         'attributes': {'oAuth': {'xmlns': 'http://exacttarget.com'}}}

        self.soap_client = suds.client.Client(self.wsdl_file_url, faults=False, cachingpolicy=1)
        self.soap_client.set_options(location=self.endpoint)

        element_oauth = Element('oAuth', ns=('etns', 'http://exacttarget.com'))
        element_oauth_token = Element('oAuthToken').setText(self.internal_auth_token)
        element_oauth.append(element_oauth_token)
        self.soap_client.set_options(soapheaders=element_oauth, timeout=TIMEOUT)

        security = suds.wsse.Security()
        token = suds.wsse.UsernameToken('*', '*')
        security.tokens.append(token)
        self.soap_client.set_options(wsse=security)

    def refresh_token(self, force_refresh=False):
        """
        Called from many different places right before executing a SOAP call
        """
        # If we don't already have a token or the token expires within 10 min(600 seconds), get one
        token_has_expired = self.auth_token_expiration is None or time.time() + 600 > self.auth_token_expiration

        if force_refresh or self.auth_token is None or token_has_expired:
            logging.log(level=logging.INFO, msg='Refreshing Exact Target Token')
            headers = {'content-type': 'application/json'}
            payload = {
                'clientId': self.client_id,
                'clientSecret': self.client_secret,
            }

            r = requests.post(self.auth_url, data=json.dumps(payload), headers=headers, timeout=TIMEOUT)
            token_response = r.json()

            if 'accessToken' not in token_response:
                payload['clientSecret'] = '******'  # don't log the secret
                raise Exception('Unable authorize with: {0} response: {1}'.format(payload, token_response))

            self.auth_token = token_response['accessToken']
            self.auth_token_expiration = time.time() + token_response['expiresIn']
            self.internal_auth_token = token_response['legacyToken']
            if 'refreshToken' in token_response:
                self.refresh_key = token_response['refreshToken']

            self.build_soap_client()

    def determine_stack(self):
        """
        find the correct url that data request web calls should go against for the token we have.
        """
        try:
            url = 'https://www.exacttargetapis.com/platform/v1/endpoints/soap?access_token={0}'.format(self.auth_token)
            r = requests.get(url, timeout=TIMEOUT)
            context_response = r.json()
            if 'url' in context_response:
                return str(context_response['url'])

        except Exception as e:
            raise Exception('Unable to determine stack using /platform/v1/tokenContext: ' + e.message)

    def add_subscriber_to_list(self, email_address, list_ids, subscriber_key = None):
        """
        add or update a subscriber with a list
        """
        new_sub = ETSubscriber()
        new_sub.auth_stub = self
        lists = []

        for p in list_ids:
            lists.append({'ID': p})

        new_sub.props = {'EmailAddress': email_address, 'Lists': lists}
        if subscriber_key is not None:
            new_sub.props['SubscriberKey'] = subscriber_key

        # Try to add the subscriber
        post_response = new_sub.post()

        if not post_response.status:
            # If the subscriber already exists in the account then we need to do an update.
            # Update Subscriber On List
            if post_response.results[0]['ErrorCode'] == 12014:
                patch_response = new_sub.patch()
                return patch_response

        return post_response

    def create_data_extensions(self, data_extension_definitions):
        """
        write the data extension props to the web service
        """
        new_des = ETDataExtension()
        new_des.auth_stub = self

        new_des.props = data_extension_definitions
        post_response = new_des.post()

        return post_response
