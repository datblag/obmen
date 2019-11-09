import logging
import nomenklatura



def send_ostatki_sklad(wsdl_client, cursor, prm_ostatki_list, prm_row_firma,prm_row_sklad, is_filial=0):
    header = wsdl_client.header_type(document_type=1, firma=prm_row_firma['idartmarket'].strip(),
                                     sklad=prm_row_sklad['idartmarket'].strip())
    row_list = []
    tovar_list = []
    for r in prm_ostatki_list:
        if r['ostatok'] <= 0:
            continue
        #logging.info(r)
        if is_filial==1:
            if not r['idtovar'].strip().isdigit():
                logging.error(["Некорректный код товара",r['idtovar']])
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

        if is_filial == 1:
            pass
            # nomenklatura.load_nomenklatura(cursor, str_id, prm_id_mode=3, prm_with_parent=0, prm_update_mode=0,
            #                                wsdl_client=wsdl_client, is_filial=is_filial)
        else:
            nomenklatura.load_nomenklatura(cursor, str_id, prm_id_mode=2, prm_with_parent=0, prm_update_mode=0,
                                           wsdl_client=wsdl_client, is_filial=is_filial)

        document = wsdl_client.document_type(header=header, rowslist=rows)
        logging.info('Загрузка документа остатков')
        wsdl_client.client.service.load_ostatki_tovar(document, is_filial)
        logging.info('Загрузка документа остатков завершена')



def load_ostatki_sklad_filial(wsdl_client, cursor, prm_firma_list=[], prm_sklad_list=[]):
    # загрузка остатков товаров
    # rg99 остатки товаров sp3603 - фирма ("     1   " - артмаркет?); sp101 - товар; sp100 - склад ('    12БЛ '); SP5183 - дата розлива; SP102 - количество
    # фирма sc13 АРТмаркет sp4805 - "9CD36F19-B8BD-49BC-BED4-A3335D2175C2    "; id - "     1   "
    #филиал Зея RG405  - SP4062 фирма, SP408 - номенклатура, SP418 - склад, SP3117 - цена прод, SP8981 - дата розлива, SP411 - количество
    logging.info('Выборка фирм')
    str_id = ', '.join(["'%s'" % w for w in prm_firma_list])
    str_sql = '''SELECT  id, descr,SP5011 as idartmarket,code FROM SC4014 where (ltrim(rtrim(SP5011)) in
                ('''+str_id+'''))'''
    cursor.execute(str_sql)
    logging.info('Выборка фирм завершена')
    rows_firma = cursor.fetchall()
    for i, row_firma in enumerate(rows_firma):
        logging.warning(row_firma)
        if row_firma['idartmarket'].strip() == '':
            continue
        logging.info('Выборка складов')
        str_id = ', '.join(["'%s'" % w for w in prm_sklad_list])
        str_sql = '''SELECT  id,SP8452 as idartmarket,descr FROM SC55  where (ltrim(rtrim(SP8452)) in
                    (''' + str_id + '''))'''

        cursor.execute(str_sql)
        logging.info('Выборка складов завершена')
        rows_sklad = cursor.fetchall()
        for row_sklad in rows_sklad:
            #logging.warning(row_sklad)
            if row_sklad['idartmarket'].strip() == '':
                continue
            #continue
            logging.info('Выборка остатков начало')
            # TODO sebestoimost поле число для обмена в товаре
            cursor.execute('''SELECT SC84.code as idtovar,sum(SP411) as ostatok,0 as sebestoimost 
                                from RG405 left join SC84 on RG405.SP408=SC84.id where (period='2018-12-01 00:00:00.000')
                                and  (SP418=%s) and (SP4062=%s) group by SC84.code''',
                           (row_sklad['id'], row_firma['id']))
            #
            logging.info('Запрос остатков выполнен')
            row = cursor.fetchall()
            logging.info(['Выбрано товаров', len(row)])
            logging.info('Курсор получен')
            send_ostatki_sklad(wsdl_client, cursor, row, {'idartmarket':'9CD36F19-B8BD-49BC-BED4-A3335D2175C2'}, row_sklad, 1)
        # print(n)
        #break

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
            send_ostatki_sklad(wsdl_client, cursor, row, row_firma, row_sklad)
        # print(n)
