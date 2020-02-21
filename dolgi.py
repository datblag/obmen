import logging
from utils import convert_base
import hdb


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
            isclosed = row['closed'] and 1
            if isclosed != 1:
                pass
            logging.info(['Выборка взаиморасчетов опмание объекта', row['datedoc'], row['docno']])

            if row['firma'] != '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
                continue
            if row['idartmarket'] is None or row['idartmarket'].strip() == '':
                if isclosed == 1:
                    logging.error(';'.join(['Пустой ид', row['docno']]))
                continue

            header = wsdl_client.header_type(document_type=2, firma=row['firma'].strip(), sklad=doc_descr,
                                             client=row['rschet'].strip(), idartmarket=row['idartmarket'].strip(),
                                             document_date=row['datedoc'], nomerartmarket=row['docno'])
            cursor.execute('''select  sc46.sp4807 as client,debkred, sp171 as summa,
                                                                 2 as typedvig, sp4372 as docosnov from ra3707
                                                                 left join sc46 on ltrim(rtrim(SP170)) = '1A   '+ltrim(rtrim(sc46.id))
                                                                 where ra169.iddoc=%s
                                                                 ''', row['iddoc'])
        else:
            client_list = []
            isclosed = row['closed'] and 1
            if isclosed != 1:
                pass
            logging.info(['Выборка взаиморасчетов опмание объекта', row['datedoc'], row['docno']])

            if row['firma'] != '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
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
                                2 as typedvig, sp4372 as docosnov from ra169
                                left join sc46 on ltrim(rtrim(SP170)) = '1A   '+ltrim(rtrim(sc46.id))
                                where ra169.iddoc=%s
                                ''', row['iddoc'])

            rows_table_1 = cursor.fetchall()

            # поставщики
            # debkred 1 уменьшение долга клиента
            # debkred 0 увеличение долга клиента
            cursor.execute('''
                            select sc46.sp4807 as client, debkred, SP936 as summa,
                            1 as typedvig, SP4373 as docosnov from ra933
                            left join sc46 on SP934 = sc46.id
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
                continue
            str_id = ",".join(client_list)
            hdb.get_client_groups(wsdl_client, cursor, str_id)
            # print(row_list)
            rows = wsdl_client.rows_type(rows=row_list)
            document = wsdl_client.document_type(header=header, rowslist=rows)
            logging.info(';'.join(['Загрузка документа взаиморасчетов', row['docno']]))
            n = wsdl_client.client.service.load_client_rashet(document, isclosed)
            logging.info(';'.join(['Загрузка документа взаиморасчетов', row['docno'], n]))

