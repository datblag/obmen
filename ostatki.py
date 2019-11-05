import logging
import nomenklatura


def send_ostatki_sklad(wsdl_client, cursor, prm_ostatki_list, prm_row_firma,prm_row_sklad):
    header = wsdl_client.header_type(document_type=1, firma=prm_row_firma['idartmarket'].strip(),
                                     sklad=prm_row_sklad['idartmarket'].strip())
    row_list = []
    tovar_list = []
    for r in prm_ostatki_list:
        if r['ostatok'] <= 0:
            continue
        row = wsdl_client.row_type(tovar=r['idtovar'], quantity=r['ostatok'], price=r['sebestoimost'])
        if not "'" + r['idtovar'] + "'" in tovar_list:
            tovar_list.append("'" + r['idtovar'] + "'")
        row_list.append(row)
    if tovar_list == []:
        logging.info('Товары для загрузки не найдены')
    else:
        rows = wsdl_client.rows_type(rows=row_list)
        str_id = ",".join(tovar_list)

        nomenklatura.load_nomenklatura(str_id, prm_id_mode=2, prm_with_parent=0, prm_update_mode=0)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info('Загрузка документа остатков')
        wsdl_client.client.service.load_ostatki_tovar(document)
        logging.info('Загрузка документа остатков завершена')



def load_ostatki_sklad_filial(wsdl_client, cursor):
    # загрузка остатков товаров
    # rg99 остатки товаров sp3603 - фирма ("     1   " - артмаркет?); sp101 - товар; sp100 - склад ('    12БЛ '); SP5183 - дата розлива; SP102 - количество
    # фирма sc13 АРТмаркет sp4805 - "9CD36F19-B8BD-49BC-BED4-A3335D2175C2    "; id - "     1   "
    #филиал Зея RG405  - SP4062 фирма, SP408 - номенклатура, SP418 - склад, SP3117 - цена прод, SP8981 - дата розлива, SP411 - количество
    logging.info('Выборка фирм')
    cursor.execute(
        '''SELECT  descr,SP5011 as idartmarket FROM SC4014''')
    logging.info('Выборка фирм завершена')
    rows_firma = cursor.fetchall()
    for row_firma in rows_firma:
        if row_firma['idartmarket'].strip() == '':
            continue
        if row_firma['idartmarket'] == '058E7FFD-AA56-4257-800A-E8494930DC1C    ':
            continue
        logging.info('Выборка складов')
        cursor.execute('''SELECT  id,SP8452 as idartmarket,descr FROM SC55 ''')
        logging.info('Выборка складов завершена')
        rows_sklad = cursor.fetchall()
        for row_sklad in rows_sklad:
            logging.warning(row_sklad)
            if row_sklad['idartmarket'].strip() == '':
                continue
            continue
            logging.info('Выборка остатков начало')
            cursor.execute('''SELECT sc33.sp4802 as idtovar,sum(sp102) as ostatok,sp6055 as sebestoimost 
                                from rg99 left join sc33 on rg99.sp101=sc33.id where (period='2018-12-01 00:00:00.000')
                                and  (sp100=%s) and (sp3603='     1   ') group by sc33.sp4802,sp6055''',
                           row_sklad['id'])
            #
            logging.info('Запрос остатков выполнен')
            row = cursor.fetchall()
            logging.info('Курсор получен')
            #send_ostatki_sklad(wsdl_client,cursor,row,row_firma,row_sklad)
        # print(n)


def load_ostatki_sklad(wsdl_client, cursor):
    # загрузка остатков товаров
    # rg99 остатки товаров sp3603 - фирма ("     1   " - артмаркет?); sp101 - товар; sp100 - склад ('    12БЛ '); SP5183 - дата розлива; SP102 - количество
    # фирма sc13 АРТмаркет sp4805 - "9CD36F19-B8BD-49BC-BED4-A3335D2175C2    "; id - "     1   "
    #филиал Зея RG405  - SP4062 фирма, SP408 - номенклатура, SP418 - склад, SP3117 - цена прод, SP8981 - дата розлива, SP411 - количество
    logging.info('Выборка фирм')
    cursor.execute(
        '''SELECT  descr,sp4805 as idartmarket FROM SC13 where (sp4805 = '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ')''')
    logging.info('Выборка фирм завершена')
    rows_firma = cursor.fetchall()
    for row_firma in rows_firma:
        if row_firma['idartmarket'].strip() == '':
            continue
        logging.info('Выборка складов')
        cursor.execute('''SELECT  id,SP5639 as idartmarket FROM SC31 ''')
        logging.info('Выборка складов завершена')
        rows_sklad = cursor.fetchall()
        for row_sklad in rows_sklad:
            if row_sklad['idartmarket'].strip() == '':
                continue
            logging.info('Выборка остатков начало')
            cursor.execute('''SELECT sc33.sp4802 as idtovar,sum(sp102) as ostatok,sp6055 as sebestoimost 
                                from rg99 left join sc33 on rg99.sp101=sc33.id where (period='2018-12-01 00:00:00.000') and  (sp100=%s) and (sp3603='     1   ') group by sc33.sp4802,sp6055''',
                           row_sklad['id'])
            #
            logging.info('Запрос остатков выполнен')
            row = cursor.fetchall()
            logging.info('Курсор получен')
            send_ostatki_sklad(wsdl_client,cursor,row,row_firma,row_sklad)
        # print(n)
