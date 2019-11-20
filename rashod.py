import logging
import nomenklatura
import hdb

def load_rashod_filial(cursor, wsdl_client, prm_row_delta):
    cursor.execute('''
                        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                        SC4014.SP5011 as firma,
                        SC172.SP573 as client,
                        sc55.SP8452 as sklad,
                        SP9325 as idartmarket,
                        '' as agent,
                        '' as expeditor,
                        '' as expeditorname,
                        _1sjourn.iddoc FROM DH1611 as dh WITH (NOLOCK)
                        left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
                        left join SC4014 WITH (NOLOCK) on SP4056=SC4014.id
                        left join SC172  WITH (NOLOCK) on SP1583 = SC172.id
                        left join sc55 WITH (NOLOCK) on SP1593 = sc55.id
                        where _1sjourn.iddoc=%s
                        ''', prm_row_delta['OBJID'])
    logging.info('Выборка расходов заголовки завершена')
    logging.warning(prm_row_delta['OBJID'])
    #rows_header = cursor.fetchall()

    for row in cursor.fetchall():
        logging.warning(row)


def load_rashod(cursor, wsdl_client, prm_row_delta):
    # расходы 410 - расходнаянакладная экспедитор SP4485
    # расходы 469 - расходнаяреализатора экспедитор SP4487
    # расходы 3716 - расходнаядоставка экспедитор SP3745
    logging.info('Выборка расходов заголовки')

    if prm_row_delta['TYPEID'] == 410:
        cursor.execute('''
                            SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                            sc13.sp4805 as firma,
                            sc46.sp4807 as client,
                            sc31.SP5639 as sklad,
                            SP6060 as idartmarket,
                            '' as agent,
                            sprexpeditor.SP4808 as expeditor,
                            sprexpeditor.descr as expeditorname,
                            _1sjourn.iddoc FROM DH410 as dh WITH (NOLOCK)
                            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
                            left join sc46 WITH (NOLOCK) on SP413 = sc46.id
                            left join sc31 WITH (NOLOCK) on SP412 = sc31.id
                            left join sc13 WITH (NOLOCK) on SP1005=sc13.id
                            left join SC3246  as sprexpeditor WITH (NOLOCK) on SP4485 = sprexpeditor.id
                            where _1sjourn.iddoc=%s
                            ''', prm_row_delta['OBJID'])
    elif prm_row_delta['TYPEID'] == 469:
        cursor.execute('''
                            SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                            sc13.sp4805 as firma,
                            sc46.sp4807 as client,
                            sc31.SP5639 as sklad,
                            SP6072 as idartmarket,
                            '' as agent,
                            sprexpeditor.SP4808 as expeditor,
                            sprexpeditor.descr as expeditorname,
                            _1sjourn.iddoc FROM DH469 as dh WITH (NOLOCK)
                            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
                            left join sc13 WITH (NOLOCK) on SP1005=sc13.id
                            left join sc46 WITH (NOLOCK) on SP472 = sc46.id
                            left join sc31 WITH (NOLOCK) on SP471 = sc31.id
                            left join SC3246  as sprexpeditor WITH (NOLOCK) on SP4487 = sprexpeditor.id
                            where _1sjourn.iddoc=%s
                            ''', prm_row_delta['OBJID'])
    elif prm_row_delta['TYPEID'] == 3716:
        # SP4639 агент
        cursor.execute('''
                            SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                            sc13.sp4805 as firma,
                            sc46.sp4807 as client,
                            sc31.SP5639 as sklad,
                            SP6071 as idartmarket,
                            spragent.SP4808 as agent,
                            spragent.descr as agentname,
                            sprexpeditor.SP4808 as expeditor,
                            sprexpeditor.descr as expeditorname,
                            _1sjourn.iddoc FROM DH3716 as dh WITH (NOLOCK)
                            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
                            left join sc13 WITH (NOLOCK) on SP1005=sc13.id
                            left join sc46 WITH (NOLOCK) on SP3718 = sc46.id
                            left join sc31 WITH (NOLOCK) on SP3717 = sc31.id
                            left join SC3246  as spragent WITH (NOLOCK) on SP4639 = spragent.id
                            left join SC3246  as sprexpeditor WITH (NOLOCK) on SP3745 = sprexpeditor.id
                            where _1sjourn.iddoc=%s
                            ''', prm_row_delta['OBJID'])

    logging.info('Выборка расходов заголовки завершена')
    rows_header = cursor.fetchall()

    for row in rows_header:
        client_list = []
        # list_partii=[]
        isclosed = row['closed'] and 1
        if isclosed != 1:
            # continue
            pass

        if row['firma'] != '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
            continue
        if row['idartmarket'] == None or row['idartmarket'].strip() == '':
            if isclosed == 1:
                logging.error(';'.join(['Пустой ид', row['docno']]))
            continue

        if row['sklad'] == None or row['sklad'].strip() == '':
            if isclosed == 1:
                logging.error(';'.join(['Пустой склад', row['docno']]))
            continue

        if row['client'] == None or row['client'].strip() == '':
            if isclosed == 1:
                logging.error(';'.join(['Пустой клиент', row['docno']]))
            continue

        if not "'" + row['client'] + "'" in client_list:
            client_list.append("'" + row['client'] + "'")
        if client_list == []:
            continue

        str_id = ",".join(client_list)
        hdb.get_client_groups(wsdl_client, cursor, str_id)
        header = wsdl_client.header_type(document_type=2, firma=row['firma'].strip(), sklad=row['sklad'].strip(),
                                         client=row['client'].strip(), idartmarket=row['idartmarket'].strip()
                                         , document_date=row['datedoc'], nomerartmarket=row['docno'])
        logging.info('Выборка строк расхода')
        if prm_row_delta['TYPEID'] == 410:
            cursor.execute('''
                   			            select  sp4802 as idtovar,SP424 as kolvo,SP427 as koef,SP426 as price,SP428 as sum from dt410 WITH (NOLOCK) left join sc33 WITH (NOLOCK) on SP423=sc33.id where iddoc=%s
                                ''', row['iddoc'])
        elif prm_row_delta['TYPEID'] == 469:
            cursor.execute('''
                   			            select  sp4802 as idtovar,SP483 as kolvo,SP486 as koef,SP485 as price,SP487 as sum from dt469 WITH (NOLOCK) left join sc33 WITH (NOLOCK) on SP482=sc33.id where iddoc=%s
                                ''', row['iddoc'])
        elif prm_row_delta['TYPEID'] == 3716:
            cursor.execute('''
                   			            select  sp4802 as idtovar,SP3731 as kolvo,SP3734 as koef,SP3733 as price,SP3735 as sum from dt3716 WITH (NOLOCK) left join sc33 WITH (NOLOCK) on SP3730=sc33.id where iddoc=%s
                                ''', row['iddoc'])
        logging.info('Выборка строк расхода завершена')

        rows_table = cursor.fetchall()
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
                                       prm_update_mode=1, wsdl_client=wsdl_client)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа расхода', row['docno']]))

        # hdb_agent=
        if row['agent'] != '' and row['agent'] != None:
            nom_agent = wsdl_client.hdb_type(name=row['agentname'].strip(), id=row['agent'].strip(), idparent='')
            hdb_array = wsdl_client.hdb_array_type(hdb_array=[nom_agent])
            logging.info('Загрузка агента начало')
            wsdl_client.client.service.load_hdb_elements(hdb_array, 1, 'agent')
            logging.info('Загрузка агента завершена')

        if row['expeditor'] != '' and row['expeditor'] != None:
            nom_agent = wsdl_client.hdb_type(name=row['expeditorname'].strip(), id=row['expeditor'].strip(),
                                             idparent='')
            hdb_array = wsdl_client.hdb_array_type(hdb_array=[nom_agent])
            logging.info('Загрузка экспедитора начало')
            wsdl_client.client.service.load_hdb_elements(hdb_array, 1, 'agent')
            logging.info('Загрузка экспедитора завершена')

        list_partii = []
        if isclosed == 1:
            logging.info('Выборка партий расхода')
            cursor.execute('''
                                    select 
                                    sp4802 as idtovar_artmarket, ltrim(rtrim(_1sjourn.iddoc)) as prihodid, _1sjourn.iddocdef as prihodtype,
                                    docno as prihodno,CAST(LEFT(_1sjourn.Date_Time_IDDoc, 8) as DateTime) as prihoddate,
                                    SP1133 as ostatok, SP2655 as stoimost, SP2799 as prodstoimost, SP4307 as prodaga
                                    from ra1130 WITH (NOLOCK)
                                    left join sc33 WITH (NOLOCK) on ra1130.sp1131 = sc33.id
                                    left join _1sjourn WITH (NOLOCK) on ltrim(rtrim(substring(ltrim(rtrim(sp1132)),charindex(' ',ltrim(rtrim(sp1132))),100)))=ltrim(rtrim(_1sjourn.iddoc))
                                    where ra1130.iddoc=%s
                                ''', row['iddoc'])
            logging.info('Выборка партий расхода завершена')
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
        n = wsdl_client.client.service.load_rashod_tovar(document, document_partii, isclosed, row['agent'], row['expeditor'])
        logging.info(';'.join(['Загрузка документа расхода', row['docno'], n]))
