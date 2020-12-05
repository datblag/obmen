import logging
import nomenklatura
from utils import check_firma, check_docid, check_sklad, is_process_doc
from config import cb_firma_id, filial_sklad_white_list
from hdb import unload_production_date


def get_move_header(cursor, prm_isfilial, prm_row_delta):
    logging.info('Выборка перемещение заголовки')
    if prm_isfilial == 0:
        logging.info('Выборка перемещение заголовки завершена')
        cursor.execute('''
                        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                        sc13.sp4805 as firma,sc31_1.SP5639 as sklad,SP6079 as idartmarket,
                        sc31_2.SP5639 as sklad_in,
                        _1sjourn.iddoc FROM dh239 as dh WITH (NOLOCK)
                        left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
                        left join sc13 WITH (NOLOCK)  on SP1005=sc13.id
                        left join sc31   as sc31_1 WITH (NOLOCK) on SP241 = sc31_1.id
                        left join sc31   as sc31_2 WITH (NOLOCK) on SP242 = sc31_2.id
                        where _1sjourn.iddoc=%s
                        ''', prm_row_delta['OBJID'])
    if prm_isfilial == 1:
        cursor.execute('''
                        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                        SC4014.SP5011 as firma,sc55_1.SP8452 as sklad, SP9326 as idartmarket,
                        sc55_2.SP8452 as sklad_in,
                        _1sjourn.iddoc FROM DH1628 as dh WITH (NOLOCK)
                        left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
                        left join SC4014 WITH (NOLOCK) on SP4056=SC4014.id
                        left join sc55 as sc55_1 WITH (NOLOCK) on SP3078 = sc55_1.id
                        left join sc55 as sc55_2 WITH (NOLOCK) on SP1615 = sc55_2.id
                        where _1sjourn.iddoc=%s
                        ''', prm_row_delta['OBJID'])
        logging.info('Выборка перемещение заголовки завершена')

    return cursor.fetchall()


def get_move_rows(cursor, prm_isfilial, prm_row):
    if prm_isfilial == 0:
        logging.info(';'.join(['Выборка строк перемещение', prm_row['docno']]))
        cursor.execute('''
                            select  sc33.sp4802 as idtovar,SP250 as kolvo,SP1034 as koef,0 as price,0 as sum,
                             SP5641 as id_pdate, SC5196.id as bdid_pdate from dt239
                            left join sc33 WITH (NOLOCK) on SP249=sc33.id
                            left join SC5196 WITH (NOLOCK) on SP5199=SC5196.id
                            where iddoc=%s
                            ''', prm_row['iddoc'])
    if prm_isfilial == 1:
        logging.info(';'.join(['Выборка строк перемещение', prm_row['docno']]))
        cursor.execute('''
                            select  SC84.code as idtovar, SC84.SP8450 as idtovarfil, SP1621 as kolvo,SP1623 as koef,
                            0 as price,0 as sum from dt1628
                            left join SC84 WITH (NOLOCK) on SP1620=SC84.id
                            where iddoc=%s
                            ''', prm_row['iddoc'])
    logging.info(';'.join(['Выборка строк перемещение завершена', prm_row['docno']]))
    return cursor.fetchall()


def move_tovar_filial(cursor, wsdl_client, prm_row_delta):
    rows_header = get_move_header(cursor, 1, prm_row_delta)

    for row_header in rows_header:
        logging.warning([check_firma(row_header, 1), check_docid(row_header, 1), check_sklad(row_header, 1)])
        if not check_firma(row_header, 1) or not check_docid(row_header, 1) or not check_sklad(row_header, 1):
            logging.warning(row_header['sklad_in'])
            if check_firma(row_header, 1) and check_docid(row_header, 1) and \
                    row_header['sklad_in'].strip() in filial_sklad_white_list:
                logging.warning('Загружаем оприходованием')
                header = wsdl_client.header_type(document_type=2, firma=cb_firma_id,
                                                 sklad=row_header['sklad_in'].strip(), client='',
                                                 idartmarket=row_header['idartmarket'].strip()
                                                 , document_date=row_header['datedoc'],
                                                 nomerartmarket=row_header['docno'])

                isclosed = is_process_doc(row_header['closed'])
                rows_table = get_move_rows(cursor, 1, row_header)
                row_list = []
                tovar_list = []

                for row_table in rows_table:
                    if not row_table['idtovar'].strip().isdigit():
                        logging.error(["Некорректный код товара", row_table['idtovar']])
                        row_nom = wsdl_client.row_type(tovar=0, quantity=row_table['kolvo'],
                                                       price=row_table['price'], koef=row_table['koef'],
                                                       sum=row_table['sum'],
                                                       tovar_filial=row_table['idtovarfil'])
                    else:
                        row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'],
                                                       price=row_table['price'], koef=row_table['koef'],
                                                       sum=row_table['sum'],
                                                       tovar_filial=row_table['idtovarfil'])

                    if row_table['idtovar'] is None:
                        continue
                    if not "'" + row_table['idtovar'] + "'" in tovar_list:
                        tovar_list.append("'" + row_table['idtovar'] + "'")
                    row_list.append(row_nom)

                rows = wsdl_client.rows_type(rows=row_list)
                str_id = ",".join(tovar_list)
                nomenklatura.load_nomenklatura(cursor, str_id, prm_id_mode=3, prm_with_parent=0, prm_update_mode=0,
                                               wsdl_client=wsdl_client, is_filial=1)

                document = wsdl_client.document_type(header=header, rowslist=rows)
                logging.info(';'.join(['Загрузка документа ввод остатка', row_header['docno']]))
                n = wsdl_client.client.service.load_vvodostatka_tovar(document, isclosed, 1)
                logging.info(';'.join(['Загрузка документа ввод остатка', row_header['docno'], n]))

            continue
        logging.warning(row_header)

        header = wsdl_client.header_type(document_type=2, firma=cb_firma_id,
                                         sklad=row_header['sklad'].strip(), client=row_header['sklad_in'].strip(),
                                         idartmarket=row_header['idartmarket'].strip()
                                         , document_date=row_header['datedoc'], nomerartmarket=row_header['docno'])

        isclosed = is_process_doc(row_header['closed'])

        rows_table = get_move_rows(cursor, 1, row_header)
        row_list = []
        tovar_list = []

        for row_table in rows_table:
            if not row_table['idtovar'].strip().isdigit():
                logging.error(["Некорректный код товара", row_table['idtovar']])
                row_nom = wsdl_client.row_type(tovar=0, quantity=row_table['kolvo'],
                                                price=row_table['price'], koef=row_table['koef'], sum=row_table['sum'],
                                               tovar_filial=row_table['idtovarfil'])
            else:
                row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'],
                                               price=row_table['price'], koef=row_table['koef'], sum=row_table['sum'],
                                               tovar_filial=row_table['idtovarfil'])
        #     if row_table['idtovar'] == None:
        #         continue
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        nomenklatura.load_nomenklatura(cursor, str_id, prm_id_mode=3, prm_with_parent=0, prm_update_mode=0,
                                       wsdl_client=wsdl_client, is_filial=1)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа перемещение', row_header['docno']]))
        n = wsdl_client.client.service.load_peremesh(document, isclosed, 1)
        logging.info(['Загрузка документа перемещение', row_header['docno'], n])


def get_spisanie_rows(cursor, prm_isfilial, prm_row):
    if prm_isfilial == 0:
        logging.info(';'.join(['Выборка строк списание', prm_row['docno']]))
        cursor.execute('''
                        select  sc33.sp4802 as idtovar,SP304 as kolvo,SP306 as koef,0 as price,0 as sum,
                        SP5641 as id_pdate, SC5196.id as bdid_pdate
                        from dt297
                        left join sc33 on SP303=sc33.id
                        left join SC5196 on SP5209=SC5196.id  
                        where iddoc=%s
                            ''', prm_row['iddoc'])
    if prm_isfilial == 1:
        logging.info(';'.join(['Выборка строк списание', prm_row['docno']]))
        cursor.execute('''
                select  SC84.code as idtovar, SC84.SP8450 as idtovarfil, SP1783 as kolvo, SP1785 as koef,
                0 as price,0 as sum from dt1790
                left join SC84 WITH (NOLOCK) on SP1782=SC84.id
                        where iddoc=%s
                            ''', prm_row['iddoc'])
    logging.info(';'.join(['Выборка строк списание завершена', prm_row['docno']]))
    return cursor.fetchall()



def get_spisanie_header(cursor, prm_isfilial, prm_row_delta):
    logging.info('Выборка списание заголовки')
    if prm_isfilial == 0:
        logging.info('Выборка списание заголовки завершена')
        cursor.execute('''
                    SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc, docno,
                    sc13.sp4805 as firma,sc31.SP5639 as sklad, SP6076 as idartmarket,
                    _1sjourn.iddoc FROM dh297   as dh WITH (NOLOCK)
                    left join _1sjourn WITH (NOLOCK)  on dh.iddoc =_1sjourn.iddoc 
                    left join sc31 WITH (NOLOCK)  on SP300 = sc31.id
                    left join sc13 WITH (NOLOCK)  on SP1005=sc13.id
                    where _1sjourn.iddoc=%s
                        ''', prm_row_delta['OBJID'])
    if prm_isfilial == 1:
        cursor.execute('''
                        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc, docno,
                        SC4014.SP5011 as firma, sc55.SP8452 as sklad, SP9328 as idartmarket,
                        _1sjourn.iddoc FROM DH1790 as dh WITH (NOLOCK)
                        left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
                        left join SC4014 WITH (NOLOCK) on SP4056=SC4014.id
                        left join sc55 WITH (NOLOCK) on SP1777 = sc55.id
                        where _1sjourn.iddoc=%s
                        ''', prm_row_delta['OBJID'])
        logging.info('Выборка списание заголовки завершена')

    return cursor.fetchall()


def spisanie_filial(cursor, wsdl_client, prm_row_delta):
    rows_header = get_spisanie_header(cursor, 1, prm_row_delta)

    for row_header in rows_header:
        logging.warning([check_firma(row_header, 1), check_docid(row_header, 1), check_sklad(row_header, 1)])
        if not check_firma(row_header, 1) or not check_docid(row_header, 1) or not check_sklad(row_header, 1):
            continue
        logging.warning('Загружаем списание')
        header = wsdl_client.header_type(document_type=2, firma=cb_firma_id,
                                         sklad=row_header['sklad'].strip(), client='',
                                         idartmarket=row_header['idartmarket'].strip()
                                         , document_date=row_header['datedoc'],
                                         nomerartmarket=row_header['docno'])

        isclosed = is_process_doc(row_header['closed'])
        rows_table = get_spisanie_rows(cursor, 1, row_header)
        row_list = []
        tovar_list = []

        for row_table in rows_table:
            if not row_table['idtovar'].strip().isdigit():
                logging.error(["Некорректный код товара", row_table['idtovar']])
                row_nom = wsdl_client.row_type(tovar=0, quantity=row_table['kolvo'],
                                               price=row_table['price'], koef=row_table['koef'],
                                               sum=row_table['sum'],
                                               tovar_filial=row_table['idtovarfil'])
            else:
                row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'],
                                               price=row_table['price'], koef=row_table['koef'],
                                               sum=row_table['sum'],
                                               tovar_filial=row_table['idtovarfil'])

            if row_table['idtovar'] is None:
                continue
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        nomenklatura.load_nomenklatura(cursor, str_id, prm_id_mode=3, prm_with_parent=0, prm_update_mode=0,
                                       wsdl_client=wsdl_client, is_filial=1)

        list_partii = []
        if isclosed == 1:
            logging.info('Выборка партий расхода')
            cursor.execute('''
                                select 
                                SC84.code as idtovar_artmarket, SC84.SP8450 as idtovarfil,
                                ltrim(rtrim(_1sjourn.iddoc)) as prihodid, _1sjourn.iddocdef as prihodtype,
                                docno as prihodno,CAST(LEFT(_1sjourn.Date_Time_IDDoc, 8) as DateTime) as prihoddate,
                                SP342 as ostatok, SP343 as stoimost, SP6818 as prodstoimost, 1 as prodaga
                                from RA328
                                left join SC84 WITH (NOLOCK) on RA328.SP331=SC84.id
                                left join SC214 WITH (NOLOCK) on RA328.SP341=SC214.id
                                left join _1sjourn WITH (NOLOCK) on ltrim(rtrim(substring(ltrim(rtrim(SP216)),
                                charindex(' ',ltrim(rtrim(SP216))),100)))=ltrim(rtrim(_1sjourn.iddoc))
                                where RA328.iddoc=%s
                                ''', row_header['iddoc'])
            logging.info('Выборка партий расхода завершена')
            rows_table_partii = cursor.fetchall()
            for row_partii in rows_table_partii:
                if not row_partii['idtovar_artmarket'].strip().isdigit():
                    row_nom_partii = wsdl_client.row_partii_type(tovar=0,
                                                                 prihod_id=row_partii['prihodid'],
                                                                 prihod_type=row_partii['prihodtype'],
                                                                 prihod_no=row_partii['prihodno'],
                                                                 prihod_date=row_partii['prihoddate'],
                                                                 ostatok=row_partii['ostatok'],
                                                                 stoimost=row_partii['stoimost'],
                                                                 prodstoimost=row_partii['prodstoimost'],
                                                                 prodaga=row_partii['prodaga'],
                                                                 tovar_filial=row_partii['idtovarfil'])
                else:
                    row_nom_partii = wsdl_client.row_partii_type(tovar=row_partii['idtovar_artmarket'],
                                                                 prihod_id=row_partii['prihodid'],
                                                                 prihod_type=row_partii['prihodtype'],
                                                                 prihod_no=row_partii['prihodno'],
                                                                 prihod_date=row_partii['prihoddate'],
                                                                 ostatok=row_partii['ostatok'],
                                                                 stoimost=row_partii['stoimost'],
                                                                 prodstoimost=row_partii['prodstoimost'],
                                                                 prodaga=row_partii['prodaga'],
                                                                 tovar_filial=row_partii['idtovarfil'])
                list_partii.append(row_nom_partii)

        document_partii_rows = wsdl_client.rows_partii_type(rows=list_partii)
        document_partii = wsdl_client.document_partii_type(rowslist=document_partii_rows)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа списание', row_header['docno']]))
        n = wsdl_client.client.service.load_spisanie(document, document_partii, isclosed, 1)
        logging.info(';'.join(['Загрузка документа списание', row_header['docno'], n]))




def spisanie(cursor, wsdl_client, prm_row_delta):
    rows_header = get_spisanie_header(cursor, 0, prm_row_delta)
    for row_header in rows_header:
        if not check_firma(row_header, 0) or not check_docid(row_header, 0) or not check_sklad(row_header, 0):
            continue

        header = wsdl_client.header_type(document_type=2, firma=row_header['firma'].strip(), sklad=row_header['sklad'].strip(),
                                         client='', idartmarket=row_header['idartmarket'].strip()
                                         , document_date=row_header['datedoc'], nomerartmarket=row_header['docno'])
        isclosed = is_process_doc(row_header['closed'])
        rows_table = get_spisanie_rows(cursor, 0, row_header)
        row_list = []
        tovar_list = []

        for row_table in rows_table:
            if row_table['id_pdate']:
                unload_production_date(cursor, wsdl_client.client, row_table['bdid_pdate'])
            row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'], price=row_table['price'],
                                           koef=row_table['koef'], sum=row_table['sum'],
                                           pdate=row_table['id_pdate'])
            if row_table['idtovar'] is None:
                continue
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        nomenklatura.load_nomenklatura(cursor, prm_id_str=str_id, prm_id_mode=2, prm_with_parent=0,
                                       prm_update_mode=0, wsdl_client=wsdl_client)

        list_partii = []
        if isclosed == 1:
            logging.info('Выборка партий списания')
            cursor.execute('''
                select 
                sp4802 as idtovar_artmarket, ltrim(rtrim(_1sjourn.iddoc)) as prihodid, _1sjourn.iddocdef as prihodtype,
                docno as prihodno,CAST(LEFT(_1sjourn.Date_Time_IDDoc, 8) as DateTime) as prihoddate,
                SP1133 as ostatok, SP2655 as stoimost, SP2799 as prodstoimost, SP4307 as prodaga
                from ra1130 WITH (NOLOCK)
                left join sc33 WITH (NOLOCK) on ra1130.sp1131 = sc33.id
                left join _1sjourn WITH (NOLOCK) on ltrim(rtrim(substring(ltrim(rtrim(sp1132)),
                charindex(' ',ltrim(rtrim(sp1132))),100)))=ltrim(rtrim(_1sjourn.iddoc))
                where ra1130.iddoc=%s
            ''', prm_row_delta['OBJID'])
            logging.info('Выборка партий списаний завершена')
            rows_table_partii = cursor.fetchall()
            for row_partii in rows_table_partii:
                row_nom_partii = wsdl_client.row_partii_type(tovar=row_partii['idtovar_artmarket'],
                                                             prihod_id=row_partii['prihodid'],
                                                             prihod_type=row_partii['prihodtype'],
                                                             prihod_no=row_partii['prihodno'],
                                                             prihod_date=row_partii['prihoddate'],
                                                             ostatok=row_partii['ostatok'],
                                                             stoimost=row_partii['stoimost'],
                                                             prodstoimost=row_partii['prodstoimost'],
                                                             prodaga=row_partii['prodaga'])
                list_partii.append(row_nom_partii)
        document_partii_rows = wsdl_client.rows_partii_type(rows=list_partii)
        document_partii = wsdl_client.document_partii_type(rowslist=document_partii_rows)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа списание', row_header['docno']]))
        n = wsdl_client.client.service.load_spisanie(document, document_partii, isclosed, 0)
        logging.info(';'.join(['Загрузка документа списание', row_header['docno'], n]))


def move_tovar(cursor, wsdl_client, prm_row_delta):
    rows_header = get_move_header(cursor, 0, prm_row_delta)

    for row_header in rows_header:

        if not check_firma(row_header, 0) or not check_docid(row_header, 0) or not check_sklad(row_header, 0):
            continue

        header = wsdl_client.header_type(document_type=2, firma=row_header['firma'].strip(),
                                         sklad=row_header['sklad'].strip(), client=row_header['sklad_in'].strip(),
                                         idartmarket=row_header['idartmarket'].strip()
                                         , document_date=row_header['datedoc'], nomerartmarket=row_header['docno'])

        isclosed = is_process_doc(row_header['closed'])

        rows_table = get_move_rows(cursor, 0, row_header)
        row_list = []
        tovar_list = []

        for row_table in rows_table:
            row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'],
                                           price=row_table['price'], koef=row_table['koef'], sum=row_table['sum'],
                                           pdate=row_table['id_pdate'])
            if row_table['idtovar'] is None:
                continue
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            if row_table['id_pdate']:
                unload_production_date(cursor, wsdl_client.client, row_table['bdid_pdate'])
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        nomenklatura.load_nomenklatura(cursor, prm_id_str=str_id, prm_id_mode=2, prm_with_parent=0,
                                       prm_update_mode=0, wsdl_client=wsdl_client)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа перемещение', row_header['docno']]))
        n = wsdl_client.client.service.load_peremesh(document, isclosed, 0)
        logging.info(';'.join(['Загрузка документа перемещение', row_header['docno'], n]))


def get_vvodostatka_header(cursor, prm_isfilial, prm_row_delta):
    if prm_isfilial == 0:
        logging.info('Выборка ввод остатков заголовки')
        cursor.execute('''
    
        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
        sc13.sp4805 as firma,sc31.SP5639 as sklad,SP6077 as idartmarket,
        _1sjourn.iddoc FROM dh310 as dh
        left join _1sjourn on dh.iddoc=_1sjourn.iddoc 
        left join sc31 on SP312 = sc31.id
        left join sc13 on SP1005=sc13.id
        where _1sjourn.iddoc=%s
    
    
        ''', prm_row_delta['OBJID'])
    if prm_isfilial == 1:
        logging.info('Выборка ввод остатков заголовки')
        cursor.execute('''

        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
        SC4014.SP5011 as firma,sc55.SP8452 as sklad,SP9327 as idartmarket,
        _1sjourn.iddoc FROM DH2106 as dh
        left join _1sjourn on dh.iddoc=_1sjourn.iddoc 
        left join SC4014 WITH (NOLOCK) on SP4056=SC4014.id
        left join sc55 WITH (NOLOCK) on SP2094 = sc55.id
        where _1sjourn.iddoc=%s


        ''', prm_row_delta['OBJID'])

    logging.info('Выборка ввод остатков заголовки завершена')
    return cursor.fetchall()


def get_vvodostatka_rows(cursor, prm_isfilial, prm_row):
    if prm_isfilial == 0:
        logging.info(';'.join(['Выборка строк ввод остатка', prm_row['docno']]))
        cursor.execute('''
                select  sc33.sp4802 as idtovar,SP316 as kolvo,SP318 as koef,SP4716 as price,SP4717 as sum,
                 SP5641 as id_pdate, SC5196.id as bdid_pdate from dt310
                left join sc33 on sp315=sc33.id
                left join SC5196 on SP5197=SC5196.id
                where iddoc=%s
        ''', prm_row['iddoc'])
        logging.info(';'.join(['Выборка строк ввод остатка завершена', prm_row['docno']]))

    if prm_isfilial == 1:
        logging.info(';'.join(['Выборка строк ввод остатка', prm_row['docno']]))
        cursor.execute('''
                select  SC84.code as idtovar, SC84.SP8450 as idtovarfil, SP2099 as kolvo, SP2101 as koef,
                SP2102 as price,SP2103 as sum from dt2106
                left join SC84 WITH (NOLOCK) on SP2098=SC84.id
                where iddoc=%s
        ''', prm_row['iddoc'])
        logging.info(';'.join(['Выборка строк ввод остатка завершена', prm_row['docno']]))



    return cursor.fetchall()

def vvodostatka_tovar_filial(cursor, wsdl_client, prm_row_delta):
    # оприходование
    rows_header = get_vvodostatka_header(cursor, 1, prm_row_delta)

    for row_header in rows_header:
        if not check_firma(row_header, 1) or not check_docid(row_header, 1) or not check_sklad(row_header, 1):
            continue

        header = wsdl_client.header_type(document_type=2, firma=cb_firma_id,
                                         sklad=row_header['sklad'].strip(), client='',
                                         idartmarket=row_header['idartmarket'].strip()
                                         , document_date=row_header['datedoc'], nomerartmarket=row_header['docno'])

        isclosed = is_process_doc(row_header['closed'])
        rows_table = get_vvodostatka_rows(cursor, 1, row_header)
        row_list = []
        tovar_list = []

        for row_table in rows_table:
            if not row_table['idtovar'].strip().isdigit():
                logging.error(["Некорректный код товара", row_table['idtovar']])
                row_nom = wsdl_client.row_type(tovar=0, quantity=row_table['kolvo'],
                                                price=row_table['price'], koef=row_table['koef'], sum=row_table['sum'],
                                               tovar_filial=row_table['idtovarfil'])
            else:
                row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'],
                                               price=row_table['price'], koef=row_table['koef'], sum=row_table['sum'],
                                               tovar_filial=row_table['idtovarfil'])

            if row_table['idtovar'] is None:
                continue
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        nomenklatura.load_nomenklatura(cursor, str_id, prm_id_mode=3, prm_with_parent=0, prm_update_mode=0,
                                       wsdl_client=wsdl_client, is_filial=1)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа ввод остатка', row_header['docno']]))
        n = wsdl_client.client.service.load_vvodostatka_tovar(document, isclosed, 1)
        logging.info(';'.join(['Загрузка документа ввод остатка', row_header['docno'], n]))


def vvodostatka_tovar(cursor, wsdl_client, prm_row_delta):
    # оприходование
    rows_header = get_vvodostatka_header(cursor, 0, prm_row_delta)

    for row_header in rows_header:
        if not check_firma(row_header, 0) or not check_docid(row_header, 0) or not check_sklad(row_header, 0):
            continue

        header = wsdl_client.header_type(document_type=2, firma=row_header['firma'].strip(),
                                         sklad=row_header['sklad'].strip(), client='',
                                         idartmarket=row_header['idartmarket'].strip()
                                         , document_date=row_header['datedoc'], nomerartmarket=row_header['docno'])

        isclosed = is_process_doc(row_header['closed'])
        rows_table = get_vvodostatka_rows(cursor, 0, row_header)
        row_list = []
        tovar_list = []

        for row_table in rows_table:
            row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'],
                                           price=row_table['price'], koef=row_table['koef'], sum=row_table['sum'],
                                           pdate=row_table['id_pdate'])
            if row_table['idtovar'] is None:
                continue
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            if row_table['id_pdate']:
                unload_production_date(cursor, wsdl_client.client, row_table['bdid_pdate'])
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        nomenklatura.load_nomenklatura(cursor, prm_id_str=str_id, prm_id_mode=2, prm_with_parent=0,
                                       prm_update_mode=0, wsdl_client=wsdl_client)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа ввод остатка', row_header['docno']]))
        n = wsdl_client.client.service.load_vvodostatka_tovar(document, isclosed, 0)
        logging.info(';'.join(['Загрузка документа ввод остатка', row_header['docno'], n]))
