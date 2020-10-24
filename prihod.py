import logging
from hdb import get_client_groups, get_client_groups_filial
import datetime
import nomenklatura
from config import cb_firma_id
from utils import is_process_doc
from hdb import unload_production_date

def get_prihod_rows_filial(prm_cursor, prm_obj_id):
    prm_cursor.execute('''
            select closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
            SC4014.SP5011 as firma, SC172.SP573 as client, sc55.SP8452 as sklad, SP9324 as idartmarket,
            _1sjourn.iddoc,0 as zatr_nashi,0 as zatr_post,0 as naedinicu,
            '0' as isreturn, _1sjourn.iddoc as OBJID
            from DH1582 as dh WITH (NOLOCK)
            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc
            left join SC4014 WITH (NOLOCK) on SP4056=SC4014.id
            left join SC172 WITH (NOLOCK) on SP1555 = SC172.id
            left join SC55 WITH (NOLOCK) on SP1565 = SC55.id
            where _1sjourn.iddoc=%s
    ''', prm_obj_id)
    return prm_cursor.fetchall()


def get_vozvrat_rows_filial(prm_cursor, prm_obj_id):
    prm_cursor.execute('''
            select closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
            SC4014.SP5011 as firma, SC172.SP573 as client, sc55.SP8452 as sklad, SP9329 as idartmarket,
            _1sjourn.iddoc,0 as zatr_nashi,0 as zatr_post,0 as naedinicu,
            '0' as isreturn, _1sjourn.iddoc as OBJID
            from DH1656 as dh WITH (NOLOCK)
            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc
            left join SC4014 WITH (NOLOCK) on SP4056=SC4014.id
            left join SC172 WITH (NOLOCK) on SP1629 = SC172.id
            left join SC55 WITH (NOLOCK) on SP1639 = SC55.id
            where _1sjourn.iddoc=%s
    ''', prm_obj_id)
    return prm_cursor.fetchall()


def load_vozvrat_filial(cursor, wsdl_client, prm_row_delta):
    logging.info('Выборка приходов заголовки')
    cursor.execute('''
            select  closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
            SC4014.SP5011 as firma, SC172.SP573 as client, sc55.SP8452 as sklad, SP9329 as idartmarket,
            _1sjourn.iddoc,0 as zatr_nashi,0 as zatr_post,0 as naedinicu,
            '1' as isreturn
            from DH1656 as dh WITH (NOLOCK)
            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
            left join SC4014 WITH (NOLOCK) on SP4056=SC4014.id
            left join SC172 WITH (NOLOCK) on SP1629 = SC172.id
            left join SC55 WITH (NOLOCK) on SP1639 = SC55.id
            where _1sjourn.iddoc=%s
    ''', prm_row_delta['OBJID'])

    logging.info('Выборка приходов заголовки завершена')
    rows_header = cursor.fetchall()

    for row_header in rows_header:
        is_return = 0
        if row_header['idartmarket'] is None or row_header['idartmarket'].strip() == '':
            logging.error(';'.join(['Пустой ид', row_header['docno']]))
            continue
        if row_header['sklad'] is None or row_header['sklad'].strip() == '':
            logging.error(';'.join(['Пустой склад', row_header['docno']]))
            continue
        if row_header['client'] is None or row_header['client'].strip() == '':
            logging.error(';'.join(['Пустой клиент', row_header['docno']]))
            continue

        header = wsdl_client.header_type(document_type=2, firma=cb_firma_id, sklad=row_header['sklad'].strip(),
                             client=row_header['client'].strip(), idartmarket=row_header['idartmarket'].strip(),
                                         document_date=row_header['datedoc'], nomerartmarket=row_header['docno'],
                             zatr_nashi=row_header['zatr_nashi'],
                             zatr_post=row_header['zatr_post'],
                             naedinicu=row_header['naedinicu'],
                             vozvrat=is_return)

        isclosed = is_process_doc(row_header['closed'])

        client_list = []
        if not "'" + row_header['client'] + "'" in client_list:
            client_list.append("'" + row_header['client'] + "'")
        if not client_list:
            continue
        str_id = ",".join(client_list)
        get_client_groups_filial(wsdl_client=wsdl_client, prm_cursor=cursor, prm_id_list=str_id)
        logging.info(';'.join(['Выборка строк прихода', row_header['docno']]))

        prm_datedoc = datetime.datetime.strftime(row_header['datedoc'], '%Y-%m-%d')
        logging.info(prm_datedoc)

        cursor.execute('''
                        select  SC84.code as idtovar, SC84.SP8450 as idtovarfil, SP1645 as kolvo, SP1647 as koef,
                        SP1648 as price, SP1649 as sum, SP4245 as pricepriobr  from Dt1656
                        left join SC84 on SP1644 = SC84.id  where iddoc=%s ''', (prm_row_delta['OBJID']))

        logging.info(';'.join(['Выборка строк прихода завершена', row_header['docno']]))
        rows_table = cursor.fetchall()
        logging.info(rows_table)
        row_list = []
        tovar_list = []

        for row_table in rows_table:
            if not row_table['idtovar'].strip().isdigit():
                logging.error(["Некорректный код товара", row_table['idtovar']])
                row_nom = wsdl_client.row_type(tovar=0, quantity=row_table['kolvo'], price=row_table['price'],
                                    koef=row_table['koef'], sum=row_table['sum'], pricepriobr=row_table['pricepriobr'],
                                    tovar_filial=row_table['idtovarfil'])
            else:
                row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'],
                                               price=row_table['price'], koef=row_table['koef'], sum=row_table['sum'],
                                               pricepriobr=row_table['pricepriobr'],
                                               tovar_filial=row_table['idtovarfil'])
            if row_table['idtovar'] is None:
                pass
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        logging.warning(str_id)
        nomenklatura.load_nomenklatura(cursor, str_id, prm_id_mode=3, prm_with_parent=0, prm_update_mode=0,
                                       wsdl_client=wsdl_client, is_filial=1)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(['Загрузка документа прихода', row_header['docno'], row_header['datedoc']])
        n = wsdl_client.client.service.load_prihod_tovar(document, isclosed, 1)
        logging.info(['Загрузка документа прихода завершена', row_header['docno'], row_header['datedoc'], n])


def load_prihod_filial(cursor, wsdl_client, prm_row_delta):
    logging.info('Выборка приходов заголовки')
    cursor.execute('''
            select  closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
            SC4014.SP5011 as firma, SC172.SP573 as client, sc55.SP8452 as sklad, SP9324 as idartmarket,
            _1sjourn.iddoc,0 as zatr_nashi,0 as zatr_post,0 as naedinicu,
            '0' as isreturn, SP3778 as number_in, SP3779 as date_in
            from DH1582 as dh WITH (NOLOCK)
            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
            left join SC4014 WITH (NOLOCK) on SP4056=SC4014.id
            left join SC172 WITH (NOLOCK) on SP1555 = SC172.id
            left join SC55 WITH (NOLOCK) on SP1565 = SC55.id
            where _1sjourn.iddoc=%s
    ''', prm_row_delta['OBJID'])

    logging.info('Выборка приходов заголовки завершена')
    rows_header = cursor.fetchall()

    for row_header in rows_header:
        is_return = 0
        if row_header['idartmarket'] is None or row_header['idartmarket'].strip() == '':
            logging.error(';'.join(['Пустой ид', row_header['docno']]))
            continue
        if row_header['sklad'] is None or row_header['sklad'].strip() == '':
            logging.error(';'.join(['Пустой склад', row_header['docno']]))
            continue
        if row_header['client'] is None or row_header['client'].strip() == '':
            logging.error(';'.join(['Пустой клиент', row_header['docno']]))
            continue

        header = wsdl_client.header_type(document_type=2, firma=cb_firma_id, sklad=row_header['sklad'].strip(),
                                         client=row_header['client'].strip(),
                                         idartmarket=row_header['idartmarket'].strip(),
                                         document_date=row_header['datedoc'], nomerartmarket=row_header['docno'],
                                         zatr_nashi=row_header['zatr_nashi'], zatr_post=row_header['zatr_post'],
                                         naedinicu=row_header['naedinicu'], vozvrat=is_return)

        isclosed = is_process_doc(row_header['closed'])

        client_list = []
        if not "'" + row_header['client'] + "'" in client_list:
            client_list.append("'" + row_header['client'] + "'")
        if not client_list:
            continue
        str_id = ",".join(client_list)
        get_client_groups_filial(wsdl_client=wsdl_client, prm_cursor=cursor, prm_id_list=str_id)
        logging.info(';'.join(['Выборка строк прихода', row_header['docno']]))

        prm_datedoc = datetime.datetime.strftime(row_header['datedoc'], '%Y-%m-%d')
        logging.info(prm_datedoc)

        cursor.execute('''
                        select  SC84.code as idtovar, SC84.SP8450 as idtovarfil, SP1570 as kolvo, SP1572 as koef,
                        SP1573 as price, SP1574 as sum, SP1573 as pricepriobr  from Dt1582
                        left join SC84 on SP1569 = SC84.id  where iddoc=%s ''', (prm_row_delta['OBJID']))

        logging.info(';'.join(['Выборка строк прихода завершена', row_header['docno']]))
        rows_table = cursor.fetchall()
        logging.info(rows_table)
        row_list = []
        tovar_list = []

        for row_table in rows_table:
            if not row_table['idtovar'].strip().isdigit():
                logging.error(["Некорректный код товара", row_table['idtovar']])
                row_nom = wsdl_client.row_type(tovar=0, quantity=row_table['kolvo'], price=row_table['price'],
                                               koef=row_table['koef'], sum=row_table['sum'],
                                               pricepriobr=row_table['pricepriobr'],
                                               tovar_filial=row_table['idtovarfil'])
            else:
                row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'],
                                               price=row_table['price'], koef=row_table['koef'], sum=row_table['sum'],
                                               pricepriobr=row_table['pricepriobr'],
                                               tovar_filial=row_table['idtovarfil'])
            if row_table['idtovar'] is None:
                 #continue
                pass
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        logging.warning(str_id)
        nomenklatura.load_nomenklatura(cursor, str_id, prm_id_mode=3, prm_with_parent=0, prm_update_mode=0,
                                       wsdl_client=wsdl_client, is_filial=1)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(['Загрузка документа прихода', row_header['docno'], row_header['datedoc']])
        n = wsdl_client.client.service.load_prihod_tovar(document, isclosed, 1)
        logging.info(['Загрузка документа прихода завершена', row_header['docno'], row_header['datedoc'], n])


def load_prihod(cursor, wsdl_client, prm_row_delta):
    logging.info('Выборка приходов заголовки')
    cursor.execute('''

                        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                        sc13.sp4805 as firma,sc46.sp4807 as client,sc31.SP5639 as sklad,SP6059 as idartmarket,
                        _1sjourn.iddoc,SP4172 as zatr_nashi,SP4176 as zatr_post,SP4173 as naedinicu,
                        sp446 as isreturn, SP5591 as number_in, SP5592 as date_in  FROM dh434 as dh WITH (NOLOCK)
                        left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
                        left join sc46 WITH (NOLOCK) on SP437 = sc46.id
                        left join sc31 WITH (NOLOCK) on SP436 = sc31.id
                        left join sc13 WITH (NOLOCK) on SP1005=sc13.id
                        where _1sjourn.iddoc=%s
                        ''', prm_row_delta['OBJID'])

    #SP446 pr_nakl

    logging.info('Выборка приходов заголовки завершена')
    rows_header = cursor.fetchall()

    for row_header in rows_header:
        is_return = 0
        if row_header['isreturn'] == '    3J   ':
            is_return = 1
        if row_header['firma'] != '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
            continue
        if row_header['idartmarket'] is None or row_header['idartmarket'].strip() == '':
            logging.error(';'.join(['Пустой ид', row_header['docno']]))
            continue

        if row_header['sklad'] is None or row_header['sklad'].strip() == '':
            logging.error(';'.join(['Пустой склад', row_header['docno']]))
            continue

        if row_header['client'] == None or row_header['client'].strip() == '':
            logging.error(';'.join(['Пустой клиент', row_header['docno']]))
            continue

        if row_header['date_in'] is None or row_header['date_in'] == datetime.datetime(1753, 1, 1, 0, 0):
            date_in = datetime.datetime(1, 1, 1, 0, 0)
        else:
            date_in = row_header['date_in']



        header = wsdl_client.header_type(document_type=2, firma=row_header['firma'].strip(), sklad=row_header['sklad'].strip(),
                             client=row_header['client'].strip(), idartmarket=row_header['idartmarket'].strip()
                             , document_date=row_header['datedoc'], nomerartmarket=row_header['docno'],
                             zatr_nashi=row_header['zatr_nashi'],
                             zatr_post=row_header['zatr_post'],
                             naedinicu=row_header['naedinicu'],
                             vozvrat=is_return, bdid=row_header['number_in'], field_date=date_in)

        isclosed = is_process_doc(row_header['closed'])

        client_list = []
        if not "'" + row_header['client'] + "'" in client_list:
            client_list.append("'" + row_header['client'] + "'")
        if not client_list:
            continue
        str_id = ",".join(client_list)
        get_client_groups(wsdl_client, cursor, str_id)

        logging.info(';'.join(['Выборка строк прихода', row_header['docno']]))

        prm_datedoc = datetime.datetime.strftime(row_header['datedoc'], '%Y-%m-%d')
        logging.info(prm_datedoc)

        cursor.execute('''
                   			        select  sc33.sp4802 as idtovar,SP449 as kolvo,SP452 as koef,SP451 as price,
                                    SP453 as sum, value as pricepriobr, SP5641 as id_pdate, SC5196.id as bdid_pdate 
                                    from dt434
                                    left join sc33 on sp448=sc33.id
                                    left join SC5196 on SP5201=SC5196.id

    	                            left join 
                                    (select a.objid as idtovar,ISNULL(cast(value as numeric(14,2)),0) as value,date from (
                                    select objid,id,

                                    max(substring(convert(varchar,date,120),1,10)
                                    +right('0000000000'+cast(time as varchar),10)
                                    +docid
                                    +right('0000000000'+cast(row_id as varchar),10))  md

                                    from _1SCONST where _1SCONST.id=38 and
                                    date<=%s group by objid,id) a
                                    inner join (select objid,id,date,value,

                                    substring(convert(varchar,date,120),1,10)
                                    +right('0000000000'+cast(time as varchar),10)
                                    +docid
                                    +right('0000000000'+cast(row_id as varchar),10)  ld


                                    from _1SCONST where _1SCONST.id=38 ) b
                                    on a.objid=b.objid and md=ld ) cpriobr on sc33.id= cpriobr.idtovar

                                    where iddoc=%s
                            ''', (prm_datedoc, prm_row_delta['OBJID']))

        logging.info(';'.join(['Выборка строк прихода завершена', row_header['docno']]))
        rows_table = cursor.fetchall()
        row_list = []
        tovar_list = []

        for row_table in rows_table:
            if row_table['id_pdate']:
                unload_production_date(cursor, wsdl_client.client, row_table['bdid_pdate'])
            row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'], price=row_table['price'],
                               koef=row_table['koef'], sum=row_table['sum'], pricepriobr=row_table['pricepriobr'],
                                           pdate=row_table['id_pdate'])
            if row_table['idtovar'] is None:
                continue
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        nomenklatura.load_nomenklatura(cursor,prm_id_str=str_id, prm_id_mode=2, prm_with_parent=0, prm_update_mode=0,wsdl_client=wsdl_client)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа прихода', row_header['docno']]))
        n = wsdl_client.client.service.load_prihod_tovar(document, isclosed, 0)
        logging.info(';'.join(['Загрузка документа прихода', row_header['docno'], n,
                               datetime.datetime.strftime(date_in, '%Y-%m-%d'), row_header['number_in']]))
