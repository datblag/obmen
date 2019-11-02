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

        self.hdb_type = self.client.get_type('ns3:hdb_element')
        self.hdb_array_type = self.client.get_type('ns3:hdb_array_element')

        self.header_type=self.client.get_type('ns2:document_header')
        self.row_type=self.client.get_type('ns2:document_row')
        self.rows_type=self.client.get_type('ns2:document_rows')
        self.document_type=self.client.get_type('ns2:document')
        self.arrayn_type = self.client.get_type('ns0:arrayn_elements')
        self.nomenklatura_type = self.client.get_type('ns0:nomenklatura_element')

        self.row_partii_type=self.client.get_type('ns2:partii_row')
        self.rows_partii_type=self.client.get_type('ns2:partii_rows')
        self.document_partii_type=self.client.get_type('ns2:document_partii')



