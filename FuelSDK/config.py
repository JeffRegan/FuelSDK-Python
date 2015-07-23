import os


class ExactTargetConfig(object):
    app_signature = None
    client_id = os.environ['FUELSDK_CLIENT_ID']
    client_secret = os.environ['FUELSDK_CLIENT_SECRET']
    default_wsdl = 'https://webservice.exacttarget.com/etframework.wsdl'
    authentication_url = 'https://auth.exacttargetapis.com/v1/requestToken?legacy=1'
    wsdl_file_local_loc = '/tmp/ExactTargetWSDL.s6.xml'


exact_target_config = ExactTargetConfig()
