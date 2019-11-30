import logging
import nomenklatura
from utils import check_firma, check_docid, check_sklad

def get_move_header(cursor, prm_isfilial, prm_row_delta):
    if prm_isfilial == 0:
        logging.info('Выборка перемещение заголовки')


        cursor.execute('''
                        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                        sc13.sp4805 as firma,sc31_1.SP5639 as sklad,SP6079 as idartmarket,
                        sc31_2.SP5639 as sklad_in,
                        _1sjourn.iddoc FROM dh239 as dh WITH (NOLOCK)
                        left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
                        left join sc31   as sc31_1 WITH (NOLOCK) on SP241 = sc31_1.id
                        left join sc31   as sc31_2 WITH (NOLOCK) on SP242 = sc31_2.id
                        left join sc13 WITH (NOLOCK)  on SP1005=sc13.id
                        where _1sjourn.iddoc=%s
    
    
                        ''', prm_row_delta['OBJID'])
        logging.info('Выборка перемещение заголовки завершена')

        return cursor.fetchall()


def get_move_rows(cursor, prm_isfilial, prm_row):
    if prm_isfilial == 0:
        logging.info(';'.join(['Выборка строк перемещение', prm_row['docno']]))
        cursor.execute('''
                            select  sc33.sp4802 as idtovar,SP250 as kolvo,SP1034 as koef,0 as price,0 as sum from dt239
                            left join sc33 on SP249=sc33.id
                            where iddoc=%s
                            ''', prm_row['iddoc'])
        logging.info(';'.join(['Выборка строк перемещение завершена', prm_row['docno']]))
        return cursor.fetchall()


def move_tovar(cursor, wsdl_client, prm_row_delta):
    rows_header = get_move_header(cursor, 0, prm_row_delta)

    for row_header in rows_header:

        if not check_firma(row_header, 0) or not check_docid(row_header, 0) or not check_sklad(row_header, 0):
            continue

        header = wsdl_client.header_type(document_type=2, firma=row_header['firma'].strip(),
                                         sklad=row_header['sklad'].strip(), client=row_header['sklad_in'].strip(),
                                         idartmarket=row_header['idartmarket'].strip()
                                         , document_date=row_header['datedoc'], nomerartmarket=row_header['docno'])

        isclosed = row_header['closed'] and 1

        rows_table = get_move_rows(cursor, 0, row_header)
        row_list = []
        tovar_list = []

        for row_table in rows_table:
            row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'],
                                           price=row_table['price'], koef=row_table['koef'], sum=row_table['sum'])
            if row_table['idtovar'] == None:
                continue
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        nomenklatura.load_nomenklatura(cursor, prm_id_str=str_id, prm_id_mode=2, prm_with_parent=0,
                                       prm_update_mode=0, wsdl_client=wsdl_client)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа перемещение', row_header['docno']]))
        n = wsdl_client.client.service.load_peremesh(document, isclosed)
        logging.info(';'.join(['Загрузка документа перемещение', row_header['docno'], n]))
