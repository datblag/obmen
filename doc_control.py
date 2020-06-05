import logging
import utils
import tqdm
import nomenklatura
from hdb import get_client_groups_filial, get_client_groups
from config import cb_firma_id
from utils import check_client, check_firma
from rashod import load_rashod

def check_rashod(cursor, wsdl_client):
    check_list = []
    # check_list.append({'doctype': 410, 'idartmarket': 'SP6060'})
    # check_list.append({'doctype': 469, 'idartmarket': 'SP6072'})
    # check_list.append({'doctype': 3716, 'idartmarket': 'SP6071'})
    # check_list.append({'doctype': 297, 'idartmarket': 'SP6076'})  # списание
    # check_list.append({'doctype': 434, 'idartmarket': 'SP6059'})  # приход
    check_list.append({'doctype': 239, 'idartmarket': 'SP6079'})  # перемещение
    check_list.append({'doctype': 310, 'idartmarket': 'SP6077'})  # ввод остатков
    logging.warning('rashod control start')
    for check_item in check_list:
        logging.warning(check_item)
        cursor.execute('''
        SELECT closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc, docno, ''' +
                       check_item['idartmarket']+''' as idartmarket, _1sjourn.iddoc,
        sc13.sp4805 as firma, iddocdef FROM DH'''+str(check_item['doctype'])+''' as dh WITH (NOLOCK)
        left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc and _1sjourn.iddocdef='''+str(check_item['doctype'])+
                       ''' left join sc13 WITH (NOLOCK) on SP1005=sc13.id
        where (CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) between '2019-01-01' and '2020-12-31')
                            ''')

        rows = cursor.fetchall()
        counter = 0
        for row in rows:
            counter = counter+1
            if counter == 1000:
                print(counter)
                counter = 0
            #print(row)
            if row['firma'] != '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
                continue
            res = wsdl_client.client.service.doc_check(row['idartmarket'], check_item['doctype'])
            isclosed = utils.is_process_doc(row['closed'])
            if res == -100 and isclosed == 1 or res == 100 and isclosed == 1:
                logging.warning([res, row['datedoc'], row['docno'], isclosed, row['iddoc']])
                # load_rashod(cursor, wsdl_client, {'TYPEID': check_item['doctype'], 'OBJID': row['iddoc']})
            elif res == 200 and not isclosed == 1:
                logging.warning(["Ошибка проведен", res, row['datedoc'], row['docno'], isclosed, row['iddoc']])
        logging.warning('rashod control compete')
