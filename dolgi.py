import logging
from utils import convert_base, is_process_doc, check_firma
import hdb
import nomenklatura
import datetime


def load_client_balance(cursor, wsdl_client):
    # SP6065 тип клиента 1 покупатель 2 поставщик
    logging.info('Выборка фирм')
    cursor.execute('''SELECT  descr,sp4805 as idartmarket,id, sp4805 as firma''')
    logging.info('Выборка фирм завершена')
    rows_firma = cursor.fetchall()
    for row_firma in rows_firma:
        if not check_firma(row_firma, 0):
            continue
        if row_firma['idartmarket'].strip() != '':

            logging.info('Выборка остатков начало')
            cursor.execute('''
    
                            SELECT  sc46.sp4807 as client,sum(SP171) as ostatok,SP6065  from RG169
                            left join sc46 on ltrim(rtrim(SP170)) = '1A   '+ltrim(rtrim(sc46.id))
                            where (period='2018-12-01 00:00:00.000') and  (SP2671=%s)  group by sc46.sp4807,SP6065
    
                            ''', row_firma['id'])
            # + нам должны, - мы должны
            # and (sc46.SP6065=1)
            logging.info('Запрос остатков выполнен')
            rows = cursor.fetchall()
            logging.info('Курсор получен')
            header = wsdl_client.header_type(document_type=1, firma=row_firma['idartmarket'].strip(), sklad='')
            row_list_dolg_post = []
            row_list_dolg = []
            row_list_avans = []
            row_list_avans_post = []
            client_list = []
            for row in rows:
                if row['client'] is None:
                    continue
                if row['SP6065'] == 1:
                    if row['ostatok'] < 0:
                        row_nom = wsdl_client.row_type(tovar=row['client'], quantity=-1 * row['ostatok'], price=0,
                                                       koef=0, sum=0)
                        row_list_avans.append(row_nom)
                    elif row['ostatok'] > 0:
                        row_nom = wsdl_client.row_type(tovar=row['client'], quantity=row['ostatok'], price=0,
                                                       koef=0, sum=0)
                        row_list_dolg.append(row_nom)
                elif row['SP6065'] == 2:
                    if row['ostatok'] > 0:
                        row_nom = wsdl_client.row_type(tovar=row['client'], quantity=row['ostatok'], price=0,
                                                       koef=0, sum=0)
                        row_list_dolg_post.append(row_nom)
                    elif row['ostatok'] < 0:
                        row_nom = wsdl_client.row_type(tovar=row['client'], quantity=-1 * row['ostatok'], price=0,
                                                       koef=0, sum=0)
                        row_list_avans_post.append(row_nom)
                if not "'" + row['client'] + "'" in client_list:
                    client_list.append("'" + row['client'] + "'")
            if not client_list:
                continue
            str_id = ",".join(client_list)
            rows = wsdl_client.rows_type(rows=row_list_dolg)
            document = wsdl_client.document_type(header=header, rowslist=rows)
            rows = wsdl_client.rows_type(rows=row_list_avans)
            document2 = wsdl_client.document_type(header=header, rowslist=rows)
            rows = wsdl_client.rows_type(rows=row_list_dolg_post)
            document3 = wsdl_client.document_type(header=header, rowslist=rows)
            rows = wsdl_client.rows_type(rows=row_list_avans_post)
            document4 = wsdl_client.document_type(header=header, rowslist=rows)
            logging.info('Загрузка документа остатков')
            # prmmode    1-долги клиентов (sc46.SP6065=1) row['ostatok']>0
            #           2 - авнсы клиентов (sc46.SP6065=1) row['ostatok']<0
            #           3- долги поставщикам
            #           4 - авансы поставщиков
            # n=client.service.load_ostatki_client(document3,3)

            # проверено
            # n=client.service.load_ostatki_client(document,1) 00-00000211
            # n=client.service.load_ostatki_client(document4,3) 00-00000222
            # n=client.service.load_ostatki_client(document2,3) 00-00000223

            logging.info('Загрузка документа остатков завершена')


def load_supplier_balance(cursor, wsdl_client):
    logging.info('Выборка фирм')
    cursor.execute('''SELECT  descr,sp4805 as idartmarket,id, sp4805 as firma  FROM SC13''')
    logging.info('Выборка фирм завершена')
    rows_firma = cursor.fetchall()
    for row_firma in rows_firma:
        if not check_firma(row_firma, 0):
            continue
        if row_firma['idartmarket'].strip() != '':

            logging.info('Выборка остатков начало')
            cursor.execute('''
    
                       SELECT  sc46.sp4807 as client,sum(SP936) as ostatok,SP6065  from RG933
                            left join sc46 on ltrim(rtrim(SP934)) = ltrim(rtrim(sc46.id))
                            where (period='2018-12-01 00:00:00.000')  and  (SP2669=%s) group by sc46.sp4807,SP6065
    
                            ''', row_firma['id'])
            # + нам должны, - мы должны
            logging.info('Запрос остатков выполнен')
            rows = cursor.fetchall()
            logging.info('Курсор получен')
            header = wsdl_client.header_type(document_type=2, firma=row_firma['idartmarket'].strip(), sklad='')
            row_list_dolg_post = []
            row_list_dolg = []
            row_list_avans = []
            row_list_avans_post = []
            client_list = []
            for row in rows:
                if row['client'] is None:
                    continue
                if row['SP6065'] == 1:
                    if row['ostatok'] < 0:
                        row_nom = wsdl_client.row_type(tovar=row['client'], quantity=-1 * row['ostatok'], price=0,
                                                       koef=0, sum=0)
                        row_list_avans.append(row_nom)
                    elif row['ostatok'] > 0:
                        row_nom = wsdl_client.row_type(tovar=row['client'], quantity=row['ostatok'], price=0,
                                                       koef=0, sum=0)
                        row_list_dolg.append(row_nom)
                elif row['SP6065'] == 2:
                    if row['ostatok'] > 0:
                        row_nom = wsdl_client.row_type(tovar=row['client'], quantity=row['ostatok'], price=0,
                                                       koef=0, sum=0)
                        row_list_dolg_post.append(row_nom)
                    elif row['ostatok'] < 0:
                        row_nom = wsdl_client.row_type(tovar=row['client'], quantity=-1 * row['ostatok'], price=0,
                                                       koef=0, sum=0)
                        row_list_avans_post.append(row_nom)
                if not "'" + row['client'] + "'" in client_list:
                    client_list.append("'" + row['client'] + "'")
            if not client_list:
                continue
            str_id = ",".join(client_list)
            rows = wsdl_client.rows_type(rows=row_list_dolg)
            document = wsdl_client.document_type(header=header, rowslist=rows)
            rows = wsdl_client.rows_type(rows=row_list_avans)
            document2 = wsdl_client.document_type(header=header, rowslist=rows)
            rows = wsdl_client.rows_type(rows=row_list_dolg_post)
            document3 = wsdl_client.document_type(header=header, rowslist=rows)
            rows = wsdl_client.rows_type(rows=row_list_avans_post)
            document4 = wsdl_client.document_type(header=header, rowslist=rows)
            logging.info('Загрузка документа остатков')
            logging.info('Загрузка документа остатков завершена')


def load_docs(cursor, wsdl_client,start_date, end_date):
    # TODO перепровести расходы с 01.03.2020
    # 434 - приход
    doc_type_list = list()
    doc_type_list.append({'typeid': 434, 'typename': 'приход', 'idfield': 'SP6059', 'sumfield': 'sp453'})
    doc_type_list.append({'typeid': 3716, 'typename': 'доставка', 'idfield': 'SP6071', 'sumfield': 'SP3735'})
    doc_type_list.append({'typeid': 410, 'typename': 'расход', 'idfield': 'SP6060', 'sumfield': 'SP428'})
    doc_type_list.append({'typeid': 469, 'typename': 'реализатор', 'idfield': 'SP6072', 'sumfield': 'SP487'})
    doc_type_list.append({'typeid': 297, 'typename': 'списание', 'idfield': 'SP6076', 'sumfield': 'SP4178'})
    doc_type_list.append({'typeid': 310, 'typename': 'ввод. ост', 'idfield': 'SP6077', 'sumfield': 'SP4717'})
    doc_type_list.append({'typeid': 239, 'typename': 'перемещ', 'idfield': 'SP6079', 'sumfield': 'SP250'})
    doc_type_list.append({'typeid': 4425, 'typename': 'заказ', 'idfield': 'SP6114', 'sumfield': 'SP4439'})
    logging.warning([start_date, end_date])
    for doc_type in doc_type_list:
        load_partii(cursor, wsdl_client, doc_type, start_date, end_date)


def load_partii(cursor, wsdl_client, prm_doc_type, prm_date_begin, prm_date_end):
    list_partii = []
    cursor.execute('''select  _1sjourn.iddoc, docno as docno,
    CAST(LEFT(_1sjourn.Date_Time_IDDoc, 8) as DateTime) as docdate, 
    dt.sm,''' + prm_doc_type['idfield'] + ''' as idartmarket  from _1sjourn
    left join dh''' + str(prm_doc_type['typeid']) + ''' dh on _1sjourn.iddoc = dh.iddoc
    left join(select sum(''' + prm_doc_type['sumfield'] + ''') as sm, iddoc from dt''' + str(prm_doc_type['typeid']) +
                   ''' group by  iddoc) dt on _1sjourn.iddoc = dt.iddoc
                   where(iddocdef=''' + str(prm_doc_type['typeid']) +
                   ''') and (CAST(LEFT(_1sjourn.Date_Time_IDDoc, 8) as DateTime) between ''' +
                   "'" + prm_date_begin.strftime("%Y-%m-%d") + "'" + ''' and ''' +
                   "'" + prm_date_end.strftime("%Y-%m-%d") + "'" + ''')''')
    rows_doc = cursor.fetchall()
    for row_partii in rows_doc:
        row_nom_partii = wsdl_client.row_partii_type(tovar=0, prihod_id=row_partii['iddoc'],
                                                     prihod_no=row_partii['docno'],
                                                     prihod_date=row_partii['docdate'],
                                                     stoimost=row_partii['sm'],
                                                     tovar_filial=row_partii['idartmarket'],
                                                     prodaga=0)
        list_partii.append(row_nom_partii)
    document_partii_rows = wsdl_client.rows_partii_type(rows=list_partii)
    document_partii = wsdl_client.document_partii_type(rowslist=document_partii_rows)

    n = wsdl_client.client.service.load_doc_list(prm_doc_type['typeid'], prm_date_begin.strftime("%Y-%m-%d"),
                                                 document_partii, 0, prm_doc_type['typename'])

    logging.warning(['Загрузка списка документов', n])


def load_dolgi(cursor, wsdl_client, prm_row_delta):
    # взаиморасчеты
    # 2989 - движенияденежныхсредств
    # 4308 - выручкадоставка SP6083; sp4323 переброска
    # 2964 - ПриходныйОрдерТБ Прих.орд.(торг.) SP6084
    # 4179 - АктПереоценкиКлиенты Акт переоц. SP6085
    # 4225 РасходныйОрдерТБ SP6082
    logging.info(';'.join(['Выборка взаиморасчетов', str(prm_row_delta['TYPEID']), prm_row_delta['OBJID']]))
    if prm_row_delta['TYPEID'] == 2989:
        idartmarket_str = 'SP6081'
        doc_descr = 'Выписка банка'
    elif prm_row_delta['TYPEID'] == 4308:
        idartmarket_str = 'SP6083'
        doc_descr = 'Выручка доставка'
    elif prm_row_delta['TYPEID'] == 2964:
        idartmarket_str = 'SP6084'
        doc_descr = 'Прих.орд.(торг.)'
    elif prm_row_delta['TYPEID'] == 4179:
        idartmarket_str = 'SP6085'
        doc_descr = 'Акт переоц.'
    elif prm_row_delta['TYPEID'] == 4225:
        idartmarket_str = 'SP6082'
        doc_descr = 'Расх.орд.(торг.)'

    if prm_row_delta['TYPEID'] == 2989:
        select_str = '''
        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
        sc13.sp4805 as firma,
        ''' + idartmarket_str + ''' as idartmarket, SP1415 as rschet,
        _1sjourn.iddoc FROM DH''' + str(prm_row_delta['TYPEID']) + '''  as dh WITH (NOLOCK)
        left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
        left join sc13 WITH (NOLOCK) on SP1005=sc13.id
        left join SC1414 WITH (NOLOCK) on SP2990=SC1414.id
        where _1sjourn.iddoc=%s and _1sjourn.iddocdef=%s
        '''
    elif prm_row_delta['TYPEID'] == 4308:
        select_str = '''
        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
        sc13.sp4805 as firma,
        ''' + idartmarket_str + ''' as idartmarket,  '' as rschet,
        _1sjourn.iddoc, sp4323 as perebroska FROM DH''' + str(prm_row_delta['TYPEID']) + '''  as dh WITH (NOLOCK)
        left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc
        left join sc13 WITH (NOLOCK) on SP1005=sc13.id
        where _1sjourn.iddoc=%s and _1sjourn.iddocdef=%s
        '''
    else:
        select_str = '''
        SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
        sc13.sp4805 as firma,
        ''' + idartmarket_str + ''' as idartmarket,  '' as rschet,
        _1sjourn.iddoc FROM DH''' + str(prm_row_delta['TYPEID']) + '''  as dh WITH (NOLOCK)
        left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
        left join sc13 WITH (NOLOCK) on SP1005=sc13.id
        where _1sjourn.iddoc=%s and _1sjourn.iddocdef=%s
        '''
    cursor.execute(select_str, (prm_row_delta['OBJID'], prm_row_delta['TYPEID']))

    rows_header = cursor.fetchall()

    logging.info('Выборка взаиморасчетов')

    for row in rows_header:
        if prm_row_delta['TYPEID'] == 4308 and row['perebroska'] == 1:
            logging.warning('perebroska')
            isclosed = is_process_doc(row['closed'])
            if isclosed != 1:
                pass
            logging.info(['Выборка взаиморасчетов опмание объекта', row['datedoc'], row['docno']])

            if not check_firma(row, 0):
                continue
            if row['idartmarket'] is None or row['idartmarket'].strip() == '':
                if isclosed == 1:
                    logging.error(';'.join(['Пустой ид', row['docno']]))
                continue

            header = wsdl_client.header_type(document_type=2, firma=row['firma'].strip(), sklad=doc_descr,
                                             client=row['rschet'].strip(), idartmarket=row['idartmarket'].strip(),
                                             document_date=row['datedoc'], nomerartmarket=row['docno'])
            cursor.execute('''select  top 100 sc46.sp4807 as client,debkred, sp3711 as summa, 2 as typedvig,
                              sp3744 as docosnov from ra3707
                              left join sc46 on ltrim(rtrim(sp3710)) = ltrim(rtrim(sc46.id))
                              where ra3707.iddoc=%s''', row['iddoc'])
            rows_table = cursor.fetchall()
            row_list = []

            for row_table in rows_table:
                # docosnov_list = row_table['docosnov'].strip().split(' ')
                # logging.warning(convert_base(docosnov_list[0], from_base=36))
                # logging.warning([row_table['docosnov'], docosnov_list])
                if row_table['debkred']:
                    debkred = 1
                else:
                    debkred = 2

                if debkred == 1 and row_table['typedvig'] == 1:
                    logging.error(';'.join(['контроль операции', row['docno']]))

                if row_table['client'] is None:
                    continue
                row_nom = wsdl_client.row_type(tovar=row_table['client'], quantity=row_table['typedvig'],
                                               price=3716,
                                               koef=debkred, sum=row_table['summa'],
                                               tovar_filial=row_table['docosnov'].strip())
                row_list.append(row_nom)
            rows = wsdl_client.rows_type(rows=row_list)
            document = wsdl_client.document_type(header=header, rowslist=rows)
            logging.info(';'.join(['Загрузка документа взаиморасчетов', row['docno']]))
            n = wsdl_client.client.service.load_perebroska(document, isclosed)
            logging.info(';'.join(['Загрузка документа взаиморасчетов', row['docno'], n]))
        else:
            client_list = []
            isclosed = is_process_doc(row['closed'])
            if isclosed != 1:
                pass
            logging.info(['Выборка взаиморасчетов опмание объекта', row['datedoc'], row['docno']])

            if not check_firma(row, 0):
                continue
            if row['idartmarket'] is None or row['idartmarket'].strip() == '':
                if isclosed == 1:
                    logging.error(';'.join(['Пустой ид', row['docno']]))
                continue

            header = wsdl_client.header_type(document_type=2, firma=row['firma'].strip(), sklad=doc_descr,
                                             client=row['rschet'].strip(), idartmarket=row['idartmarket'].strip(),
                                             document_date=row['datedoc'], nomerartmarket=row['docno'])

            # расход debkred 1

            # покупатели
            # debkred 1 уменьшение долга клиента
            # debkred 0 увеличение долга клиента
            # SP4372 - кред документ, документ основание
            # TODO добавить документ основание

            cursor.execute('''select  sc46.sp4807 as client,debkred, sp171 as summa,
                                2 as typedvig, sp4372 as docosnov from ra169 WITH (NOLOCK)
                                left join sc46 WITH (NOLOCK) on ltrim(rtrim(SP170)) = '1A   '+ltrim(rtrim(sc46.id))
                                where ra169.iddoc=%s
                                ''', row['iddoc'])

            rows_table_1 = cursor.fetchall()

            if not rows_table_1 == [] and rows_table_1[0]['client'] is None:
                cursor.execute('''select  sc46.sp4807 as client,debkred, sp171 as summa,
                                    2 as typedvig, sp4372 as docosnov from ra169 WITH (NOLOCK)
                                    left join sc46 WITH (NOLOCK) on ltrim(rtrim(SP170)) = '1A    '+ltrim(rtrim(sc46.id))
                                    where ra169.iddoc=%s
                                    ''', row['iddoc'])

                rows_table_1 = cursor.fetchall()

            # поставщики
            # debkred 1 уменьшение долга клиента
            # debkred 0 увеличение долга клиента
            cursor.execute('''
                            select sc46.sp4807 as client, debkred, SP936 as summa,
                            1 as typedvig, SP4373 as docosnov from ra933 WITH (NOLOCK)
                            left join sc46 WITH (NOLOCK) on SP934 = sc46.id
                            where ra933.iddoc=%s
                                ''', row['iddoc'])

            rows_table_2 = cursor.fetchall()

            rows_table = rows_table_1 + rows_table_2

            client_list = []
            row_list = []

            for row_table in rows_table:
                docosnov_list = row_table['docosnov'].strip().split(' ')
                logging.warning(convert_base(docosnov_list[0], from_base=36))
                logging.warning([row_table['docosnov'], docosnov_list])
                if row_table['debkred']:
                    debkred = 1
                else:
                    debkred = 2

                if debkred == 1 and row_table['typedvig'] == 1:
                    logging.error(';'.join(['контроль операции', row['docno']]))

                if row_table['client'] is None:
                    continue
                row_nom = wsdl_client.row_type(tovar=row_table['client'], quantity=row_table['typedvig'],
                                               price=convert_base(docosnov_list[0], from_base=36),
                                               koef=debkred, sum=row_table['summa'],
                                               tovar_filial=docosnov_list[1])
                if not "'" + row_table['client'] + "'" in client_list:
                    client_list.append("'" + row_table['client'] + "'")
                row_list.append(row_nom)
            if not client_list:
                # continue
                pass
            else:
                str_id = ",".join(client_list)
                hdb.get_client_groups(wsdl_client, cursor, str_id)
            # print(row_list)
            rows = wsdl_client.rows_type(rows=row_list)
            document = wsdl_client.document_type(header=header, rowslist=rows)
            logging.info(';'.join(['Загрузка документа взаиморасчетов', row['docno']]))
            n = wsdl_client.client.service.load_client_rashet(document, isclosed)
            logging.info(';'.join(['Загрузка документа взаиморасчетов', row['docno'], n]))


def load_service_invoices(cursor, wsdl_client, prm_row_delta):
    logging.info('Выборка счет заголовки')
    cursor.execute('''
            SELECT closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc, docno,
            sc13.sp4805 as firma, sc46.sp4807 as client, SP6127 as idartmarket, sP4558 as schf,
            _1sjourn.iddoc FROM DH4553 as dh WITH (NOLOCK)
            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
            left join sc46 WITH (NOLOCK) on SP4557 = sc46.id
            left join sc13 WITH (NOLOCK) on SP1005=sc13.id
            where _1sjourn.iddoc=%s
                        ''', prm_row_delta['OBJID'])
    logging.info('Выборка счет заголовки завершена')
    rows_header = cursor.fetchall()

    for row_header in rows_header:
        if not check_firma(row_header, 0):
            continue
        if row_header['idartmarket'] is None or row_header['idartmarket'].strip() == '':
            logging.error(';'.join(['Пустой ид', row_header['docno']]))
            continue
        if row_header['client'] is None or row_header['client'].strip() == '':
            logging.error(';'.join(['Пустой клиент', row_header['docno']]))
            continue

        header = wsdl_client.header_type(document_type=2, firma=row_header['firma'].strip(),
                                         sklad=row_header['schf'],
                                         client=row_header['client'].strip(),
                                         idartmarket=row_header['idartmarket'].strip(),
                                         document_date=row_header['datedoc'], nomerartmarket=row_header['docno'],
                                         bdid=row_header['iddoc'])

        isclosed = is_process_doc(row_header['closed'])

        client_list = []
        if not "'" + row_header['client'] + "'" in client_list:
            client_list.append("'" + row_header['client'] + "'")
        if not client_list:
            continue
        str_id = ",".join(client_list)
        hdb.get_client_groups(wsdl_client, cursor, str_id)

        logging.info(';'.join(['Выборка строк счет', row_header['docno']]))

        cursor.execute('''
                        select sp4556 as usluga, sp4559 as sum,
                        SP6128 as finance_id, SP6129 as for_market_id, sc46.sp4807 as client_row_id, 
                        SP6126 as unit_id, sp6125 as cost_id
                        from DT4553 
                        left join SC5552 on DT4553.SP5556 = SC5552.id
                        left join SC5554 on  DT4553.SP5557 = SC5554.id
                        left join sc46 on SP5558 = sc46.id
                        left join SC3769 on SP6134 = SC3769.id
                        left join sc3773 on SP6135 = sc3773.id
                        where iddoc=%s
                            ''', (prm_row_delta['OBJID']))

        logging.info(';'.join(['Выборка строк заказ завершена', row_header['docno']]))
        rows_table = cursor.fetchall()
        # print(rows_table)
        row_list = []
        tovar_list = []
        # rows_table = []

        for row_table in rows_table:
            logging.info(row_table)

            finance_id = ''
            if row_table['finance_id'] is not None:
                finance_id = row_table['finance_id']

            for_market_id = ''
            if row_table['for_market_id'] is not None:
                for_market_id = row_table['for_market_id']

            client_row_id = ''
            if row_table['client_row_id'] is not None:
                client_row_id = row_table['client_row_id']

            unit_id = ''
            if row_table['unit_id'] is not None:
                unit_id = row_table['unit_id']

            cost_id = ''
            if row_table['cost_id'] is not None:
                cost_id = row_table['cost_id']




        rows = wsdl_client.rows_type(rows=[])

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа счет на услуги', row_header['docno']]))
        n = wsdl_client.client.service.load_invoice(document, isclosed, 0)
        logging.info(';'.join(['Загрузка документа на услуги', row_header['docno'], n]))


def load_order_supplier(cursor, wsdl_client, prm_row_delta):
    logging.info('Выборка заказ заголовки')
    cursor.execute('''
            SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc, docno,
            sc13.sp4805 as firma, sc31.SP5639 as sklad,sc46.sp4807 as client, SP6114 as idartmarket,
            _1sjourn.iddoc,SP4430 as fotgruz, SP4530 as foplata, SP4431 as fprihod, 
             SP4427 as shipment_date, SP4428 as payment_date, SP4429 as arrival_date,
             SP4750 as defer_days, SP1008 as osnovanie FROM DH4425 as dh WITH (NOLOCK)
            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc 
            left join sc46 WITH (NOLOCK) on SP4426 = sc46.id
            left join sc13 WITH (NOLOCK) on SP1005=sc13.id
            left join sc31 WITH (NOLOCK) on SP5605=sc31.id
            where _1sjourn.iddoc=%s
                        ''', prm_row_delta['OBJID'])
    logging.info('Выборка заказ заголовки завершена')
    rows_header = cursor.fetchall()

    for row_header in rows_header:
        if not check_firma(row_header, 0):
            continue
        if row_header['idartmarket'] is None or row_header['idartmarket'].strip() == '':
            logging.error(';'.join(['Пустой ид', row_header['docno']]))
            continue

        if row_header['client'] is None or row_header['client'].strip() == '':
            logging.error(';'.join(['Пустой клиент', row_header['docno']]))
            continue
        sklad = row_header['sklad']
        if row_header['sklad'] is None or row_header['sklad'].strip() == '':
            sklad = ''
        logging.warning(row_header)
        not_in_road = 0
        if row_header['fotgruz'] == 0 or row_header['fprihod'] == 1:
            not_in_road = 1

        if row_header['shipment_date'] is None or row_header['shipment_date'] == datetime.datetime(1753, 1, 1, 0, 0):
            shipment_date = datetime.datetime(1, 1, 1, 0, 0)
        else:
            shipment_date = row_header['shipment_date']

        if row_header['payment_date'] is None or row_header['payment_date'] == datetime.datetime(1753, 1, 1, 0, 0):
            payment_date = datetime.datetime(1, 1, 1, 0, 0)
        else:
            payment_date = row_header['payment_date']

        if row_header['arrival_date'] is None or row_header['arrival_date'] == datetime.datetime(1753, 1, 1, 0, 0):
            arrival_date = datetime.datetime(1, 1, 1, 0, 0)
        else:
            arrival_date = row_header['arrival_date']

        header = wsdl_client.header_type(document_type=2, firma=row_header['firma'].strip(),
                                         sklad=sklad.strip(), client=row_header['client'].strip(),
                                         idartmarket=row_header['idartmarket'].strip(),
                                         document_date=row_header['datedoc'], nomerartmarket=row_header['docno'],
                                         zatr_nashi=not_in_road, zatr_post=row_header['fotgruz'],
                                         naedinicu=row_header['foplata'], vozvrat=row_header['fprihod'],
                                         bdid=row_header['osnovanie'], skidka_procent=row_header['defer_days'],
                                         field_date=shipment_date, field_date2=payment_date,
                                         field_date3=arrival_date)

        isclosed = is_process_doc(row_header['closed'])

        client_list = []
        if not "'" + row_header['client'] + "'" in client_list:
            client_list.append("'" + row_header['client'] + "'")
        if not client_list:
            continue
        str_id = ",".join(client_list)
        hdb.get_client_groups(wsdl_client, cursor, str_id)

        logging.info(';'.join(['Выборка строк заказ', row_header['docno']]))

        prm_datedoc = datetime.datetime.strftime(row_header['datedoc'], '%Y-%m-%d')
        logging.info(prm_datedoc)

        cursor.execute('''
                    select  sc33.sp4802 as idtovar, SP4437 as kolvo, 1 as koef, SP4438 as price,
                    SP4439 as sum from DT4425
                    left join sc33 on SP4434=sc33.id
                    where iddoc=%s
                            ''', (prm_row_delta['OBJID']))

        logging.info(';'.join(['Выборка строк заказ завершена', row_header['docno']]))
        rows_table = cursor.fetchall()
        # print(rows_table)
        row_list = []
        tovar_list = []
        # rows_table = []

        for row_table in rows_table:
            row_nom = wsdl_client.row_type(tovar=row_table['idtovar'], quantity=row_table['kolvo'], price=row_table['price'],
                               koef=row_table['koef'], sum=row_table['sum'])
            if row_table['idtovar'] is None:
                continue
            if not "'" + row_table['idtovar'] + "'" in tovar_list:
                tovar_list.append("'" + row_table['idtovar'] + "'")
            row_list.append(row_nom)

        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)
        nomenklatura.load_nomenklatura(cursor,prm_id_str=str_id, prm_id_mode=2, prm_with_parent=0, prm_update_mode=0,wsdl_client=wsdl_client)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info(';'.join(['Загрузка документа заказ', row_header['docno']]))
        n = wsdl_client.client.service.load_order(document, isclosed, 0)
        logging.info(';'.join(['Загрузка документа заказ', row_header['docno'], n]))

