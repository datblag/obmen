import logging
from hdb import get_client_groups
import datetime
import nomenklatura

def load_prihod(cursor, wsdl_client, prm_row_delta):
    logging.info('Выборка приходов заголовки')
    cursor.execute('''

                        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                        sc13.sp4805 as firma,sc46.sp4807 as client,sc31.SP5639 as sklad,SP446 pr_nakl,SP6059 as idartmarket,
                        _1sjourn.iddoc,SP4172 as zatr_nashi,SP4176 as zatr_post,SP4173 as naedinicu,
                        sp446 as isreturn  FROM dh434 as dh WITH (NOLOCK)
                        left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
                        left join sc46 WITH (NOLOCK) on SP437 = sc46.id
                        left join sc31 WITH (NOLOCK) on SP436 = sc31.id
                        left join sc13 WITH (NOLOCK) on SP1005=sc13.id
                        where _1sjourn.iddoc=%s


                        ''', prm_row_delta['OBJID'])
    logging.info('Выборка приходов заголовки завершена')
    rows_header = cursor.fetchall()

    for row_header in rows_header:
        is_return = 0
        if row_header['isreturn'] == '    3J   ':
            is_return = 1
        if row_header['firma'] != '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
            continue
        if row_header['idartmarket'] == None or row_header['idartmarket'].strip() == '':
            logging.error(';'.join(['Пустой ид', row_header['docno']]))
            continue

        if row_header['sklad'] == None or row_header['sklad'].strip() == '':
            logging.error(';'.join(['Пустой склад', row_header['docno']]))
            continue

        if row_header['client'] == None or row_header['client'].strip() == '':
            logging.error(';'.join(['Пустой клиент', row_header['docno']]))
            continue

        header = wsdl_client.header_type(document_type=2, firma=row_header['firma'].strip(), sklad=row_header['sklad'].strip(),
                             client=row_header['client'].strip(), idartmarket=row_header['idartmarket'].strip()
                             , document_date=row_header['datedoc'], nomerartmarket=row_header['docno'],
                             zatr_nashi=row_header['zatr_nashi'],
                             zatr_post=row_header['zatr_post'],
                             naedinicu=row_header['naedinicu'],
                             vozvrat=is_return)

        isclosed = row_header['closed'] and 1

        client_list = []
        if not "'" + row_header['client'] + "'" in client_list:
            client_list.append("'" + row_header['client'] + "'")
        if client_list == []:
            continue
        str_id = ",".join(client_list)
        get_client_groups(wsdl_client,cursor, str_id)

        logging.info(';'.join(['Выборка строк прихода', row_header['docno']]))

        prm_datedoc = datetime.datetime.strftime(row_header['datedoc'], '%Y-%m-%d')
        logging.info(prm_datedoc)

        cursor.execute('''
                   			        select  sc33.sp4802 as idtovar,SP449 as kolvo,SP452 as koef,SP451 as price,
                                    SP453 as sum, value as pricepriobr from dt434
                                    left join sc33 on sp448=sc33.id

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
        print(rows_table)
        row_list = []
        tovar_list = []

        for row_table in rows_table:
            row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'], price=row_table['price'],
                               koef=row_table['koef'], sum=row_table['sum'], pricepriobr=row_table['pricepriobr'])
            if row_table['idtovar'] == None:
                continue
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        nomenklatura.load_nomenklatura(cursor,prm_id_str=str_id, prm_id_mode=2, prm_with_parent=0, prm_update_mode=0,wsdl_client=wsdl_client)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа прихода', row_header['docno']]))
        n = wsdl_client.client.service.load_prihod_tovar(document, isclosed)
        logging.info(';'.join(['Загрузка документа прихода', row_header['docno'], n]))
