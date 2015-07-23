from .rest import ETCUDSupport, ETCUDSupportRest, ETGetSupport, ETGet, ETPatch, ETPost, ETDelete, ETConfigure, \
    ETCreateOptions
from .utility import prune_dict


class ETDeliveryProfile(ETCUDSupport):

    def __init__(self):
        super(ETDeliveryProfile, self).__init__()
        self.obj_type = 'DeliveryProfile'
        self.key = None
        self.id = None
        self.name = None
        self.description = None

    def map_properties(self):
        self.props = {
            'ID': self.id,
            'CustomerKey': self.key,
            'Name': self.name,
            'Description': self.description,
            'SourceAddressType': 'DefaultPrivateIPAddress'
        }

        # remove all None values
        self.props = prune_dict(self.props)

    def post(self, create_options=None):
        self.map_properties()
        return super(ETDeliveryProfile, self).post(create_options=create_options)

    def get(self, m_props=None, m_filter=None, m_options=None, client_ids=[], query_all_accounts=False):
        self.props = [
            'ID',
            'CustomerKey',
            'Name',
            'Description',
            'CustomerKey',
        ]

        return super(ETDeliveryProfile, self).get(client_ids=client_ids, query_all_accounts=query_all_accounts)


class ETSenderProfile(ETCUDSupport):

    def __init__(self):
        super(ETSenderProfile, self).__init__()
        self.obj_type = 'SenderProfile'
        self.key = None
        self.id = None
        self.description = None
        self.from_name = None
        self.from_address = None
        self.name = None

    def map_properties(self):
        self.props = {
            'ID': self.id,
            'CustomerKey': self.key,
            'Name': self.name,
            'Description': self.description,
            'FromName': self.from_name,
            'FromAddress': self.from_address
        }

        # remove all None values
        self.props = prune_dict(self.props)

    def post(self, create_options=None):
        self.map_properties()
        return super(ETSenderProfile, self).post(create_options=create_options)

    def get(self, m_props=None, m_filter=None, m_options=None, client_ids=[], query_all_accounts=False):
        self.props = [
            'ID',
            'CustomerKey',
            'Name',
            'Description',
            'CustomerKey',
            'FromName',
            'FromAddress'
        ]

        return super(ETSenderProfile, self).get(client_ids=client_ids, query_all_accounts=query_all_accounts)


class ETSendClassification(ETCUDSupport):

    def __init__(self):
        super(ETSendClassification, self).__init__()
        self.obj_type = 'SendClassification'
        self.key = None
        self.id = None
        self.name = None
        self.description = None
        self.sender_profile_id = None
        self.delivery_profile_id = None

    def map_properties(self):
        self.props = {
            'ID': self.id,
            'CustomerKey': self.key,
            'Name': self.name,
            'Description': self.description,
            'SenderProfile': {'ObjectID': self.sender_profile_id},
            'DeliveryProfile': {'ObjectID': self.delivery_profile_id},
            'SendPriority': 'Low',
            'SendClassificationType': 'Marketing'
        }

        # remove all None values
        self.props = prune_dict(self.props)

    def post(self, create_options=None):
        self.map_properties()
        return super(ETSendClassification, self).post(create_options=create_options)

    def get(self, m_props=None, m_filter=None, m_options=None, client_ids=[], query_all_accounts=False):
        self.props = [
            'ID',
            'CustomerKey',
            'Name',
            'Description',
            'CustomerKey',
            'SenderProfile',
            'DeliveryProfile',
            'SendPriority',
            'SendClassificationType'
        ]

        return super(ETSendClassification, self).get(client_ids=client_ids, query_all_accounts=query_all_accounts)


class ETSuppressionListDefinition(ETCUDSupport):
    '''
    wrap an Exact Target Suppression List Definition
    '''

    def __init__(self):
        super(ETSuppressionListDefinition, self).__init__()
        self.obj_type = 'SuppressionListDefinition'


class SuppressionListContext(ETGetSupport):
    '''
    wrap an Exact Target Suppression List Context
    '''

    def __init__(self):
        super(SuppressionListContext, self).__init__()
        self.obj_type = 'SuppressionListContext'


class ETContentArea(ETCUDSupport):
    '''
    wrap an Exact Target Content Area
    '''

    def __init__(self):
        super(ETContentArea, self).__init__()
        self.obj_type = 'ContentArea'


class ETFolder(ETCUDSupport):
    '''
    wrap an Exact Target DataFolder
    '''

    def __init__(self):
        super(ETFolder, self).__init__()
        self.obj_type = 'DataFolder'


class ETProfileAttribute(ETCUDSupport):
    '''
    wrap an Exact Target PropertyDefinition
    '''

    def __init__(self):
        self.obj_type = 'PropertyDefinition'
        self.update = False
        self.delete = False

    def post(self, create_options=None):
        obj = ETConfigure(self.auth_stub, self.obj_type, self.props, self.update, self.delete)
        if obj is not None:
            self.last_request_id = obj.request_id
        return obj


class ETBounceEvent(ETGetSupport):
    '''
    wrap an Exact Target Bounce Event
    '''

    def __init__(self):
        self.obj_type = 'BounceEvent'


class ETCampaign(ETCUDSupportRest):
    '''
    wrap an Exact Target Campaign and associated Assets
    '''

    def __init__(self):
        super(ETCampaign, self).__init__()
        self.endpoint = 'https://www.exacttargetapis.com/hub/v1/campaigns/{id}'
        self.url_props = ['id']
        self.url_props_required = []

    # the patch rest service is not implemented for campaigns yet.  use post instead and remove this when patch is implemented on the back end
    def patch(self):
        self.endpoint = 'https://www.exacttargetapis.com/hub/v1/campaigns'  # don't put the id on the url when patching via post
        obj = super(ETCampaign, self).post()
        self.endpoint = 'https://www.exacttargetapis.com/hub/v1/campaigns/{id}' # but set it back to the url with id for other operations to continue working
        return obj


class ETCampaignAsset(ETCUDSupportRest):
    def __init__(self):
        super(ETCampaignAsset, self).__init__()
        self.endpoint = 'https://www.exacttargetapis.com/hub/v1/campaigns/{id}/assets/{assetId}'
        self.url_props = ['id', 'assetId']
        self.url_props_required = ['id']


class ETClickEvent(ETGetSupport):
    '''
    wrap an Exact Target Click Event
    '''

    def __init__(self):
        super(ETClickEvent, self).__init__()
        self.obj_type = 'ClickEvent'


class ETList(ETCUDSupport):
    '''
    wrap an Exact Target List and List Subscriber
    '''

    def __init__(self):
        super(ETList, self).__init__()
        self.obj_type = 'List'

class ETListSubscriber(ETGetSupport):
    def __init__(self):
        super(ETListSubscriber, self).__init__()
        self.obj_type = 'ListSubscriber'


class ETSentEvent(ETGetSupport):
    def __init__(self):
        super(ETSentEvent, self).__init__()
        self.obj_type = 'SentEvent'


class ETOpenEvent(ETGetSupport):
    def __init__(self):
        super(ETOpenEvent, self).__init__()
        self.obj_type = 'OpenEvent'


class ETUnsubEvent(ETGetSupport):
    def __init__(self):
        super(ETUnsubEvent, self).__init__()
        self.obj_type = 'UnsubEvent'


class ETEmail(ETCUDSupport):
    def __init__(self):
        super(ETEmail, self).__init__()
        self.obj_type = 'Email'


class ETTriggeredSend(ETCUDSupport):
    subscribers = None

    def __init__(self):
        super(ETTriggeredSend, self).__init__()
        self.obj_type = 'TriggeredSendDefinition'
        self.attributes = None

    def send(self, scheduled_time=None):
        create_options = None
        if scheduled_time is not None:
            create_options = ETCreateOptions(scheduled_time=scheduled_time).build()

        ts_call = {'TriggeredSendDefinition': self.props, 'Subscribers': self.subscribers, 'Attributes': self.attributes}
        self.obj = ETPost(self.auth_stub, 'TriggeredSend', props=ts_call, create_options=create_options)
        return self.obj


class ETSubscriber(ETCUDSupport):
    def __init__(self):
        super(ETSubscriber, self).__init__()
        self.obj_type = 'Subscriber'


class ETDataExtension(ETCUDSupport):
    columns = None

    def __init__(self):
        super(ETDataExtension, self).__init__()
        self.obj_type = 'DataExtension'

    def post(self, create_options=None):
        original_props = self.props

        if type(self.props) is list:
            multi_de = []
            for currentDE in self.props:
                currentDE['Fields'] = {}
                currentDE['Fields']['Field'] = []
                for key in currentDE['columns']:
                    currentDE['Fields']['Field'].append(key)
                del currentDE['columns']
                multi_de.append(currentDE.copy())

            self.props = multi_de
        else:
            self.props['Fields'] = {}
            self.props['Fields']['Field'] = []

            for key in self.columns:
                self.props['Fields']['Field'].append(key)

        obj = super(ETDataExtension, self).post(create_options=create_options)
        self.props = original_props
        return obj

    def patch(self):
        self.props['Fields'] = {}
        self.props['Fields']['Field'] = []
        for key in self.columns:
            self.props['Fields']['Field'].append(key)
        obj = super(ETDataExtension, self).patch()
        del self.props['Fields']
        return obj


class ETDataExtensionColumn(ETGetSupport):
    def __init__(self):
        super(ETDataExtensionColumn, self).__init__()
        self.obj = 'DataExtensionField'

    def get(self, m_props=None, m_filter=None, m_options=None, client_ids=[], query_all_accounts=False):
        '''
        if props and props.is_a? Array then
            @props = props
        end
        '''

        if self.props is not None and type(self.props) is dict:
            self.props = self.props.keys()

        '''
        if filter and filter.is_a? Hash then
            @filter = filter
        end
        '''

        '''
        fixCustomerKey = False
        if filter and filter.is_a? Hash then
            @filter = filter
            if @filter.has_key?("Property") && @filter["Property"] == "CustomerKey" then
                @filter["Property"]  = "DataExtension.CustomerKey"
                fixCustomerKey = true
            end
        end
        '''

        obj = ETGet(self.auth_stub, self.obj, self.props, self.search_filter)
        self.last_request_id = obj.request_id

        '''
        if fixCustomerKey then
            @filter["Property"] = "CustomerKey"
        end
        '''

        return obj


class ETDataExtensionRow(ETCUDSupport):
    name = None
    customer_key = None

    def __init__(self):
        super(ETDataExtensionRow, self).__init__()
        self.obj_type = 'DataExtensionObject'

    def get(self, m_props=None, m_filter=None, m_options=None, client_ids=[], query_all_accounts=False):
        self.get_name()
        '''
        if props and props.is_a? Array then
            @props = props
        end
        '''

        if self.props is not None and type(self.props) is dict:
            self.props = self.props.keys()

        '''
        if filter and filter.is_a? Hash then
            @filter = filter
        end
        '''

        obj = ETGet(self.auth_stub, 'DataExtensionObject[{0}]'.format(self.name), self.props, self.search_filter)
        self.last_request_id = obj.request_id

        return obj

    def post(self, create_options=None):
        self.get_customer_key()
        original_props = self.props

        if type(self.props) is list:
            current_prop_list = []
            for rec in self.props:
                current_fields = []
                current_prop = {}

                for key, value in rec.iteritems():
                    current_fields.append({'Name' : key, 'Value' : value})

                current_prop['CustomerKey'] = self.customer_key
                current_prop['Properties'] = {}
                current_prop['Properties']['Property'] = current_fields

                current_prop_list.append(current_prop)

            current_prop = current_prop_list

        else:
            current_fields = []
            current_prop = {}

            for key, value in self.props.iteritems():
                current_fields.append({'Name' : key, 'Value' : value})

            current_prop['CustomerKey'] = self.customer_key
            current_prop['Properties'] = {}
            current_prop['Properties']['Property'] = current_fields

        obj = ETPost(self.auth_stub, self.obj_type, current_prop, create_options=create_options)
        self.props = original_props
        return obj

    def patch(self):
        self.get_customer_key()

        if type(self.props) is list:
            current_prop_list = []
            for rec in self.props:
                current_fields = []
                current_prop = {}

                for key, value in rec.iteritems():
                    current_fields.append({'Name' : key, 'Value' : value})

                current_prop['CustomerKey'] = self.customer_key
                current_prop['Properties'] = {}
                current_prop['Properties']['Property'] = current_fields

                current_prop_list.append(current_prop)

            current_prop = current_prop_list
        else:
            current_fields = []
            current_prop = {}

            for key, value in self.props.iteritems():
                current_fields.append({'Name' : key, 'Value' : value})

            current_prop['CustomerKey'] = self.customer_key
            current_prop['Properties'] = {}
            current_prop['Properties']['Property'] = current_fields

        obj = ETPatch(self.auth_stub, self.obj_type, current_prop)
        return obj

    def delete(self):
        self.get_customer_key()

        if type(self.props) is list:
            current_prop_list = []
            for rec in self.props:
                current_fields = []
                current_prop = {}

                for key, value in rec.iteritems():
                    current_fields.append({'Name' : key, 'Value' : value})

                current_prop['CustomerKey'] = self.customer_key
                current_prop['Keys'] = {}
                current_prop['Keys']['Key'] = current_fields

                current_prop_list.append(current_prop)

            current_prop = current_prop_list
        else:
            current_fields = []
            current_prop = {}

            for key, value in self.props.iteritems():
                current_fields.append({'Name' : key, 'Value' : value})

            current_prop['CustomerKey'] = self.customer_key
            current_prop['Keys'] = {}
            current_prop['Keys']['Key'] = current_fields

        obj = ETDelete(self.auth_stub, self.obj_type, current_prop)
        return obj

    def get_customer_key(self):
        if self.customer_key is None:
            if self.name is None:
                raise Exception('Unable to process DataExtension::Row request due to CustomerKey and Name not being defined on ET_DatExtension::row')
            else:
                de = ETDataExtension()
                de.auth_stub = self.auth_stub
                de.props = ['Name','CustomerKey']
                de.search_filter = {'Property' : 'CustomerKey','SimpleOperator' : 'equals','Value' : self.name}
                get_response = de.get()
                if get_response.status and len(get_response.results) == 1 and 'CustomerKey' in get_response.results[0]:
                    self.customer_key = get_response.results[0]['CustomerKey']
                else:
                    raise Exception('Unable to process DataExtension::Row request due to unable to find DataExtension based on Name')

    def get_name(self):
        if self.name is None:
            if self.customer_key is None:
                raise Exception('Unable to process DataExtension::Row request due to CustomerKey and Name not being defined on ET_DatExtension::row')
            else:
                de = ETDataExtension()
                de.auth_stub = self.auth_stub
                de.props = ['Name','CustomerKey']
                de.search_filter = {'Property' : 'CustomerKey','SimpleOperator' : 'equals','Value' : self.customer_key}
                get_response = de.get()
                if get_response.status and len(get_response.results) == 1 and 'Name' in get_response.results[0]:
                    self.name = get_response.results[0]['Name']
                else:
                    raise Exception('Unable to process DataExtension::Row request due to unable to find DataExtension based on CustomerKey')
