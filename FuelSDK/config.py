import os


class ExactTargetConfig(object):
    '''
    client_id and client_secret are required
    other params have defaults and are therefore optional
    '''
    client_id = os.environ['FUELSDK_CLIENT_ID']
    client_secret = os.environ['FUELSDK_CLIENT_SECRET']
    app_signature = os.environ.get('FUELSDK_APP_SIGNATURE', None)
    default_wsdl = os.environ.get('FUELSDK_DEFAULT_WSDL', 'https://webservice.exacttarget.com/etframework.wsdl')
    authentication_url = os.environ.get('FUELSDK_AUTH_URL', 'https://auth.exacttargetapis.com/v1/requestToken?legacy=1')
    wsdl_file_local_loc = os.environ.get('FUELSDK_WSDL_FILE_LOCAL_LOC', '/tmp/ExactTargetWSDL.s6.xml')


exact_target_config = ExactTargetConfig()
