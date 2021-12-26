#from config import prod_server_address,prod_server_user,prod_server_password

from zeep import Client
from requests import Session
from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
from zeep.transports import Transport
import logging



class WsdlClient:
    def __init__(self, wsdl_config):
        logging.warning('Определение wsdl типов')
        self.session = Session()
        self.session.auth = HTTPBasicAuth(wsdl_config['server_user'], wsdl_config['server_password'])
        self.client = Client(wsdl=wsdl_config['server_address'], transport=Transport(session=self.session))
        name_hdb = ''
        name_doc = ''
        name_nom = ''
        for namespace in self.client.namespaces:
            current_name = self.client.namespaces[namespace]
            logging.warning([namespace, current_name])
            if 'hdb' in current_name:
                name_hdb = namespace
            elif 'documentsklad' in current_name:
                name_doc = namespace
            elif 'nomenklatura' in current_name:
                name_nom = namespace
        logging.warning(name_doc)
        self.hdb_type = self.client.get_type(name_hdb + ':hdb_element')
        self.hdb_array_type = self.client.get_type(name_hdb + ':hdb_array_element')

        self.header_type=self.client.get_type(name_doc + ':document_header')
        self.row_type=self.client.get_type(name_doc + ':document_row')
        self.rows_type=self.client.get_type(name_doc + ':document_rows')
        self.document_type=self.client.get_type(name_doc + ':document')
        self.arrayn_type = self.client.get_type(name_nom + ':arrayn_elements')
        self.nomenklatura_type = self.client.get_type(name_nom + ':nomenklatura_element')

        self.row_partii_type=self.client.get_type(name_doc + ':partii_row')
        self.rows_partii_type=self.client.get_type(name_doc + ':partii_rows')
        self.document_partii_type=self.client.get_type(name_doc + ':document_partii')



