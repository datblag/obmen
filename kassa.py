import logging

def prihod(cursor, wsdl_client, prm_row_delta):
    logging.info('Выборка приходный ордер заголовки')
    cursor.execute('''
            SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc, docno,
            sc13.sp4805 as firma, sp4115 as priniat_ot, sp4123 as summa, SP6118 as idartmarket,
            _1sjourn.iddoc FROM DH4114 as dh WITH (NOLOCK)
            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
            left join sc13 WITH (NOLOCK) on SP1005=sc13.id
            where _1sjourn.iddoc=%s
                        ''', prm_row_delta['OBJID'])
    logging.info('Выборка приходный ордер  заголовки завершена')
    rows_header = cursor.fetchall()

    for row_header in rows_header:
        if row_header['firma'] != '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
            logging.warning(row_header['firma'])
            continue
        if row_header['idartmarket'] == None or row_header['idartmarket'].strip() == '':
            logging.error(';'.join(['Пустой ид', row_header['docno']]))
            continue

        logging.warning(row_header)

        header = wsdl_client.header_type(document_type=2, firma=row_header['firma'].strip(), sklad='',
                             client=row_header['priniat_ot'].strip(), idartmarket=row_header['idartmarket'].strip()
                             , document_date=row_header['datedoc'], nomerartmarket=row_header['docno'],
                                         zatr_nashi=row_header['summa'])

        isclosed = row_header['closed'] and 1

        document = wsdl_client.document_type(header=header, rowslist=[])
        logging.info(';'.join(['Загрузка документа приходный ордер', row_header['docno']]))
        # n = ''
        n = wsdl_client.client.service.load_prihod_kassa(document, isclosed, 0)
        logging.info(';'.join(['Загрузка документа приходный ордер', row_header['docno'], n]))



def rashod(cursor, wsdl_client, prm_row_delta):
    logging.info('Выборка расходный ордер заголовки')
    cursor.execute('''
            SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc, docno,
            sc13.sp4805 as firma, sp4133 as priniat_ot, sp4139 as summa, SP6119 as idartmarket,
            _1sjourn.iddoc FROM DH4132 as dh WITH (NOLOCK)
            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
            left join sc13 WITH (NOLOCK) on SP1005=sc13.id
            where _1sjourn.iddoc=%s
                        ''', prm_row_delta['OBJID'])
    logging.info('Выборка расходный ордер  заголовки завершена')
    rows_header = cursor.fetchall()

    for row_header in rows_header:
        if row_header['firma'] != '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
            logging.warning(row_header['firma'])
            continue
        if row_header['idartmarket'] == None or row_header['idartmarket'].strip() == '':
            logging.error(';'.join(['Пустой ид', row_header['docno']]))
            continue

        logging.warning(row_header)

        header = wsdl_client.header_type(document_type=2, firma=row_header['firma'].strip(), sklad='',
                             client=row_header['priniat_ot'].strip(), idartmarket=row_header['idartmarket'].strip()
                             , document_date=row_header['datedoc'], nomerartmarket=row_header['docno'],
                                         zatr_nashi=row_header['summa'])

        isclosed = row_header['closed'] and 1

        document = wsdl_client.document_type(header=header, rowslist=[])
        logging.info(';'.join(['Загрузка расходный приходный ордер', row_header['docno']]))
        # n = ''
        n = wsdl_client.client.service.load_rashod_kassa(document, isclosed, 0)
        logging.info(';'.join(['Загрузка расходный расходный ордер', row_header['docno'], n]))
