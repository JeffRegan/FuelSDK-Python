import requests
import json
import copy
import logging
from utility import prune_dict

TIMEOUT = 600  # make the timeout 10 minutes


class ETCreateOptions(object):
    '''
    Only needed for async requests
    Async requests can be scheduled
    Sync requests cannot be scheduled
    '''

    def __init__(self, scheduled_time=None):
        super(ETCreateOptions, self).__init__()
        self.scheduled_time = scheduled_time

    def build(self):
        dictionary = {
            'RequestType': 'Synchronous',  # Asynchronous
            'ScheduledTime': self.scheduled_time
        }
        dictionary = prune_dict(dictionary=dictionary)
        return dictionary


class ETConstructor(object):
    '''
    Parent class used to determine what status we are in depending on web service call results
    '''

    results = []
    code = None
    status = False
    message = None
    more_results = False
    request_id = None

    def __init__(self, response=None, rest=False):

        if response is not None:  # if a response was returned from the web service call
            if rest:    # result is from a REST web service call...
                self.code = response.status_code
                if self.code == 200:
                    self.status = True
                else:
                    self.status = False

                try:
                    self.results = response.json()
                except Exception, e:
                    logging.log(level=logging.ERROR, msg='response: {0}'.format(response))
                    logging.exception(e)
                    # TODO what? This will cause another exception
                    self.message = response.json()

                # additional parsing will happen in the child object that called in to here.

            else:  # soap call
                self.code = response[0]  # suds puts the code in tuple position 0
                body = response[1]  # and the result in tuple position 1

                # Store the Last Request ID for use with continue
                if body and 'RequestID' in body:
                    self.request_id = body['RequestID']

                if self.code == 200:
                    self.status = True

                    if 'OverallStatus' in body:
                        self.message = body['OverallStatus']
                        if body['OverallStatus'] == 'MoreDataAvailable':
                            self.more_results = True
                        elif body['OverallStatus'] != 'OK':
                            self.status = False

                    body_container_tag = None
                    if 'Results' in body:  # most SOAP responses are wrapped in 'Results'
                        body_container_tag = 'Results'
                    elif 'ObjectDefinition' in body:  # Describe SOAP response is in 'ObjectDefinition'
                        body_container_tag = 'ObjectDefinition'

                    if body_container_tag is not None:
                        self.results = body[body_container_tag]

                else:
                    self.status = False

    def parse_props_dict_into_ws_object(self, obj_type, ws_object, props_dict):
        for k, v in props_dict.iteritems():
            if k in ws_object:
                ws_object[k] = v
            else:
                message = '{0} is not a property of {1}'.format(k, obj_type)
                raise Exception(message)
        return ws_object

    def parse_props_into_ws_object(self, auth_stub, obj_type, props):
        empty_obj = auth_stub.soap_client.factory.create(obj_type)
        if props is not None and type(props) is dict:
            ws_create = copy.copy(empty_obj)
            ws_create = self.parse_props_dict_into_ws_object(obj_type, ws_create, props)
            return ws_create
        elif props is not None and type(props) is list:
            ws_create_list = []
            for prop_dict in props:
                ws_create = copy.copy(empty_obj)
                ws_create = self.parse_props_dict_into_ws_object(obj_type, ws_create, prop_dict)
                ws_create_list.append(ws_create)

            return ws_create_list
        else:
            message = 'Can not post properties to {0} without a dict or list of properties'.format(obj_type)
            raise Exception(message)


class ETDescribe(ETConstructor):
    '''
    Used to Describe Objects via web service call
    '''

    def __init__(self, auth_stub, obj_type):
        auth_stub.refresh_token()

        ws_describe_request = auth_stub.soap_client.factory.create('ArrayOfObjectDefinitionRequest')

        object_definition_request = {'ObjectType': obj_type}
        ws_describe_request.ObjectDefinitionRequest = [object_definition_request]

        response = auth_stub.soap_client.service.Describe(ws_describe_request)

        if response is not None:
            self.message = 'Describe: ' + obj_type
            super(ETDescribe, self).__init__(response)


class ETConfigure(ETConstructor):
    '''
    Used to Configure Objects via web service call
    '''

    def __init__(self, auth_stub, obj_type, props=None, update=False, delete=False):
        auth_stub.refresh_token()

        ws_configure_request = auth_stub.soap_client.factory.create('ConfigureRequestMsg')
        action = 'create'
        if delete:
            action = 'delete'
        elif update:
            action = 'update'
        ws_configure_request.Action = action
        ws_configure_request.Configurations = {'Configuration': self.parse_props_into_ws_object(auth_stub, obj_type, props)}

        response = auth_stub.soap_client.service.Configure(None, ws_configure_request)

        if response is not None:
            super(ETConfigure, self).__init__(response)


class ETGet(ETConstructor):
    '''
    Get call to a web service
    '''

    def __init__(self, auth_stub, obj_type, props=None, search_filter=None, options=None, client_ids=[], query_all_accounts=False):
        auth_stub.refresh_token()

        if props is None:  # if there are no properties to retrieve for the obj_type then return a Description of obj_type
            describe = ETDescribe(auth_stub, obj_type)
            props = []
            for prop in describe.results[0].Properties:
                if prop.IsRetrievable:
                    props.append(prop.Name)

        ws_retrieve_request = auth_stub.soap_client.factory.create('RetrieveRequest')
        ws_retrieve_request.QueryAllAccounts = query_all_accounts

        if client_ids:
            formatted_client_ids = []
            for client_id in client_ids:
                formatted_client_ids.append({'CustomerKey': client_id})
                ws_retrieve_request.ClientIDs = formatted_client_ids

        if props is not None:
            if type(props) is dict:  # If the properties is a hash, then we just want to use the keys
                ws_retrieve_request.Properties = props.keys()
            else:
                ws_retrieve_request.Properties = props

        if search_filter is not None:
            if 'LogicalOperator' in search_filter:
                ws_simple_filter_part_left = auth_stub.soap_client.factory.create('SimpleFilterPart')
                for prop in ws_simple_filter_part_left:
                    if prop[0] in search_filter['LeftOperand']:
                        ws_simple_filter_part_left[prop[0]] = search_filter['LeftOperand'][prop[0]]

                ws_simple_filter_part_right = auth_stub.soap_client.factory.create('SimpleFilterPart')
                for prop in ws_simple_filter_part_right:
                    if prop[0] in search_filter['RightOperand']:
                        ws_simple_filter_part_right[prop[0]] = search_filter['RightOperand'][prop[0]]

                ws_complex_filter_part = auth_stub.soap_client.factory.create('ComplexFilterPart')
                ws_complex_filter_part.LeftOperand = ws_simple_filter_part_left
                ws_complex_filter_part.RightOperand = ws_simple_filter_part_right
                ws_complex_filter_part.LogicalOperator = search_filter['LogicalOperator']
                for additional_operand in search_filter.get('AdditionalOperands', []):
                    ws_simple_filter_part = auth_stub.soap_client.factory.create('SimpleFilterPart')
                    for k, v in additional_operand.items():
                        ws_simple_filter_part[k] = v
                    ws_complex_filter_part.AdditionalOperands.Operand.append(ws_simple_filter_part)

                ws_retrieve_request.Filter = ws_complex_filter_part
            else:
                ws_simple_filter_part = auth_stub.soap_client.factory.create('SimpleFilterPart')
                for prop in ws_simple_filter_part:
                    if prop[0] in search_filter:
                        ws_simple_filter_part[prop[0]] = search_filter[prop[0]]
                ws_retrieve_request.Filter = ws_simple_filter_part

        if options is not None:
            for key, value in options.iteritems():
                if isinstance(value, dict):
                    for k, v in value.iteritems():
                        ws_retrieve_request.Options[key][k] = v
                else:
                    ws_retrieve_request.Options[key] = value

        ws_retrieve_request.ObjectType = obj_type

        response = auth_stub.soap_client.service.Retrieve(ws_retrieve_request)

        if response is not None:
            super(ETGet, self).__init__(response)


class ETPost(ETConstructor):
    '''
    Call the Exact Target web service Create method
    '''

    def __init__(self, auth_stub, obj_type, props=None, create_options=None):
        auth_stub.refresh_token()

        if create_options is not None:
            empty_obj = auth_stub.soap_client.factory.create('CreateOptions')
            create_options = self.parse_props_dict_into_ws_object(
                obj_type='CreateOptions',
                ws_object=empty_obj,
                props_dict=create_options
            )

        obj = self.parse_props_into_ws_object(auth_stub, obj_type, props)
        response = auth_stub.soap_client.service.Create(create_options, obj)

        if response is not None:
            super(ETPost, self).__init__(response)


class ETPatch(ETConstructor):
    '''
    Call the Exact Target web service Update method
    '''

    def __init__(self, auth_stub, obj_type, props=None):
        auth_stub.refresh_token()

        response = auth_stub.soap_client.service.Update(None, self.parse_props_into_ws_object(auth_stub, obj_type, props))

        if response is not None:
            super(ETPatch, self).__init__(response)


class ETDelete(ETConstructor):
    '''
    Call the Exact Target web service Delete method
    '''

    def __init__(self, auth_stub, obj_type, props=None):
        auth_stub.refresh_token()

        response = auth_stub.soap_client.service.Delete(None, self.parse_props_into_ws_object(auth_stub, obj_type, props))

        if response is not None:
            super(ETDelete, self).__init__(response)


class ETContinue(ETConstructor):
    '''
    Call the Exact Target web service RetrieveRequest passing in ContinueRequest param
    '''

    def __init__(self, auth_stub, request_id):
        auth_stub.refresh_token()

        ws_continue_request = auth_stub.soap_client.factory.create('RetrieveRequest')
        ws_continue_request.ContinueRequest = request_id
        response = auth_stub.soap_client.service.Retrieve(ws_continue_request)

        if response is not None:
            super(ETContinue, self).__init__(response)


class ETBaseObject(object):
    '''
    set up variables for children objects to share
    '''

    auth_stub = None
    obj = None
    last_request_id = None
    endpoint = None
    props = None
    extProps = None
    search_filter = None
    options = None


class ETGetSupport(ETBaseObject):
    '''
    make sure needed information is available and then make the call to ET_Get to call the webservice
    '''

    obj_type = 'ET_GetSupport'   # should be overwritten by inherited class

    def get(self, m_props=None, m_filter=None, m_options=None, client_ids=[], query_all_accounts=False):
        props = self.props
        search_filter = self.search_filter
        options = self.options

        if m_props is not None and type(m_props) is list:
            props = m_props
        elif self.props is not None and type(self.props) is dict:
            props = self.props.keys()

        if m_filter is not None and type(m_filter) is dict:
            search_filter = m_filter

        if m_options is not None and type(m_filter) is dict:
            options = m_options

        obj = ETGet(
            self.auth_stub,
            self.obj_type, props,
            search_filter,
            options,
            client_ids=client_ids,
            query_all_accounts=query_all_accounts
        )

        if obj is not None:
            self.last_request_id = obj.request_id
        return obj

    def info(self):
        obj = ETDescribe(self.auth_stub, self.obj_type)
        if obj is not None:
            self.last_request_id = obj.request_id
        return obj

    def get_more_results(self):
        obj = ETContinue(self.auth_stub, self.last_request_id)
        if obj is not None:
            self.last_request_id = obj.request_id
        return obj


class ETGetRest(ETConstructor):
    '''
    Restful webservice to Get data
    '''

    def __init__(self, auth_stub, endpoint, qs=None):
        auth_stub.refresh_token()
        full_endpoint = '{0}?access_token={1}'.format(endpoint, auth_stub.authToken)
        for qs_value in qs:
            full_endpoint = '{0}&{1}={2}'.format(full_endpoint, qs_value, str(qs[qs_value]))

        r = requests.get(full_endpoint, timeout=TIMEOUT)
        self.more_results = False
        super(ETGetRest, self).__init__(r, True)


class ETPostRest(ETConstructor):
    '''
    Restful webservice to Get data
    '''

    def __init__(self, auth_stub, endpoint, payload):
        auth_stub.refresh_token()

        headers = {'content-type': 'application/json'}
        full_endpoint = '{0}?access_token={1}'.format(endpoint, auth_stub.authToken)
        r = requests.post(full_endpoint, data=json.dumps(payload), headers=headers, timeout=TIMEOUT)
        super(ETPostRest, self).__init__(r, True)


class ETPatchRest(ETConstructor):
    '''
    Restful webservice to Get data
    '''

    def __init__(self, auth_stub, endpoint, payload):
        auth_stub.refresh_token()

        headers = {'content-type': 'application/json'}
        full_endpoint = '{0}?access_token={1}'.format(endpoint, auth_stub.authToken)
        r = requests.patch(full_endpoint, data=json.dumps(payload), headers=headers, timeout=TIMEOUT)
        super(ETPatchRest, self).__init__(r, True)


class ETDeleteRest(ETConstructor):
    '''
    Restful webservice to Get data
    '''
    def __init__(self, auth_stub, endpoint):
        auth_stub.refresh_token()

        full_endpoint = '{0}?access_token={1}'.format(endpoint, auth_stub.authToken)
        r = requests.delete(full_endpoint, timeout=TIMEOUT)
        super(ETDeleteRest, self).__init__(r, True)


class ETCUDSupport(ETGetSupport):
    '''
    Get data
    '''

    def post(self, create_options=None):
        if self.extProps is not None:
            for k, v in self.extProps.iteritems():
                self.props[k.capitalize] = v

        obj = ETPost(self.auth_stub, self.obj_type, self.props)
        if obj is not None:
            self.last_request_id = obj.request_id
        return obj

    def patch(self):
        obj = ETPatch(self.auth_stub, self.obj_type, self.props)
        if obj is not None:
            self.last_request_id = obj.request_id
        return obj

    def delete(self):
        obj = ETDelete(self.auth_stub, self.obj_type, self.props)
        if obj is not None:
            self.last_request_id = obj.request_id
        return obj


class ETGetSupportRest(ETBaseObject):
    '''
    Get data using a REST call
    '''

    url_props = None
    url_props_required = None
    last_page_number = None

    def get(self, props = None):
        if props is not None and type(props) is dict:
            self.props = props

        complete_url = self.endpoint
        additional_qs = {}

        if self.props is not None and type(self.props) is dict:
            for k, v in self.props.iteritems():
                if k in self.url_props:
                    complete_url = complete_url.replace('{{{0}}}'.format(k), v)
                else:
                    additional_qs[k] = v

        for value in self.url_props_required:
            if self.props is None or value not in self.props:
                raise Exception('Unable to process request due to missing required prop: #{value}')

        for value in self.url_props:
            complete_url = complete_url.replace('/{{{0}}}'.format(value), '')

        obj = ETGetRest(self.auth_stub, complete_url, additional_qs)

        results = obj.results
        count = None
        if 'page' in results:
            self.last_page_number = results['page']
            page_size = results['pageSize']
            if 'count' in results:
                count = results['count']
            elif 'totalCount' in results:
                count = results['totalCount']

            if count is not None and count > (self.last_page_number * page_size):
                obj.more_results = True
            else:
                obj.more_results = False
        return obj

    def get_more_results(self):
        props = None    # where should it come from?
        if props is not None and type(props) is dict:
            self.props = props

        original_page_value = '1'
        remove_page_from_props = False

        if self.props is not None and '$page' in self.props:
            original_page_value = self.props['page']
        else:
            remove_page_from_props = True

        if self.props is None:
            self.props = {}

        self.props['$page'] = self.last_page_number + 1

        obj = self.get()

        if remove_page_from_props:
            del self.props['$page']
        else:
            self.props['$page'] = original_page_value

        return obj


class ETCUDSupportRest(ETGetSupportRest):
    '''
    Create, Update and Delete using a REST call
    '''

    endpoint = None
    url_props = None
    url_props_required = None

    def post(self):
        complete_url = self.endpoint

        if self.props is not None and type(self.props) is dict:
            for k, v in self.props.iteritems():
                if k in self.url_props:
                    complete_url = complete_url.replace('{{{0}}}'.format(k), v)

        for value in self.url_props_required:
            if self.props is None or value not in self.props:
                raise Exception('Unable to process request due to missing required prop: #{value}')

        # Clean Optional Parameters from Endpoint URL first
        for value in self.url_props:
            complete_url = complete_url.replace('/{{{0}}}'.format(value), '')

        obj = ETPostRest(self.auth_stub, complete_url, self.props)
        return obj

    def patch(self):
        complete_url = self.endpoint
        # All URL Props are required when doing Patch
        for value in self.url_props:
            if self.props is None or value not in self.props:
                raise Exception('Unable to process request due to missing required prop: #{value}')

        if self.props is not None and type(self.props) is dict:
            for k, v in self.props.iteritems():
                if k in self.url_props:
                    complete_url = complete_url.replace('{{{0}}}'.format(k), v)

        obj = ETPatchRest(self.auth_stub, complete_url, self.props)
        return obj

    def delete(self):
        complete_url = self.endpoint
        # All URL Props are required when doing Patch
        for value in self.url_props:
            if self.props is None or value not in self.props:
                raise Exception('Unable to process request due to missing required prop: #{value}')

        if self.props is not None and type(self.props) is dict:
            for k, v in self.props.iteritems():
                if k in self.url_props:
                    complete_url = complete_url.replace('{{{0}}}'.format(k), v)

        obj = ETDeleteRest(self.auth_stub, complete_url)
        return obj
