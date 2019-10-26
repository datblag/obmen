from config import prod_server_address,prod_server_user,prod_server_password

from zeep import Client
from requests import Session
from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
from zeep.transports import Transport


session = Session()
session.auth = HTTPBasicAuth(prod_server_user, prod_server_password)
client = Client(wsdl=prod_server_address,transport=Transport(session=session))


hdb_type = client.get_type('ns3:hdb_element')
hdb_array_type = client.get_type('ns3:hdb_array_element')

header_type=client.get_type('ns2:document_header')
row_type=client.get_type('ns2:document_row')
rows_type=client.get_type('ns2:document_rows')
document_type=client.get_type('ns2:document')
arrayn_type = client.get_type('ns0:arrayn_elements')
nomenklatura_type = client.get_type('ns0:nomenklatura_element')

row_partii_type=client.get_type('ns2:partii_row')
rows_partii_type=client.get_type('ns2:partii_rows')
document_partii_type=client.get_type('ns2:document_partii')


