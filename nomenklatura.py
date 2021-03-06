import logging
from config import filial_tovar_group_id
from datetime import timedelta
from tqdm import *
#import logs
#from sql import cursor
#from wsdl import nomenklatura_type,arrayn_type,client

#from logging.handlers import TimedRotatingFileHandler


#logs.run()


# получает из кода перечисления ТипРасхЦен соответствующий код в справочнике типыцен
def get_price_code_from_enumeration(enumeration_code):
    if enumeration_code.strip() == '2U8':
        return 36
    elif enumeration_code.strip() == '3KO':
        return 38
    elif enumeration_code.strip() == '2U7':
        return 35
    elif enumeration_code.strip() == '3FY':
        return 4460
    elif enumeration_code.strip() == '4B':
        return 3677
    elif enumeration_code.strip() == '49':
        return 37
    elif enumeration_code.strip() == '3CJ':
        return 4340
    elif enumeration_code.strip() == '4A':
        return 3678
    elif enumeration_code.strip() == '3KE':
        return 4613
    elif enumeration_code.strip() == '4O5':
        return 6051
    else:
        logging.error(['Неизвестный тип расходной цены', enumeration_code])
        return 0


def unload_price(wsdl_client, cursor, start_date_0, end_date):
    # 36-доставка, 38-приобретение,
    # (4549 закуп, 35 киоск, 4460 - индивидуальная, 3677 - для сетей (оптовая), 37 - розничная) -
    # филиал - 4340, херека (специальная) - 3678 , самбери (акцизного склада) - 4613
    # желтый ценник 6051 выгрузили с 01.11.2020
    delta = timedelta(days=1)

    # [36, 38, 4549, 35, 4460, 3677, 37, 4340, 3678, 4613, 6051]
    price_type_to_load = [36, 38, 4549, 35, 4460, 3677, 37, 4340, 3678, 4613, 6051]
    for price_type in price_type_to_load:
        logging.warning(price_type)
        start_date = start_date_0
        while start_date <= end_date:
            k2 = start_date.strftime("%Y-%m-%d")
            logging.warning([price_type, k2])
            start_date += delta
            load_nomenklatura(cursor, prm_id_str='', prm_id_mode=1, prm_with_parent=0, prm_update_mode=1,
                              prm_unload_price=price_type, prm_unload_price_date=k2, wsdl_client=wsdl_client)


def unload_retail_min_price(cursor=None, wsdl_client=None):
    return '''
select code, descr, date,value, SP5902 as krepost from SC5900 elemement1 left join (
select mdate,a.objid as idtovar,date,value  from (
select objid,id,max(date) as mdate from _1SCONST where date<='2020-07-01' and (_1SCONST.id=5901) group by objid,id) a
inner join (select objid,id,date,max(value) as value from _1SCONST where _1SCONST.id=5901 group by objid,id,date) b on a.objid=b.objid
and mdate=date ) cdost on elemement1.id= cdost.idtovar where    (isnull(cdost.date,'1978-01-01')<>'1978-01-01') order by descr
'''


def unload_ean_codes(cursor=None, wsdl_client=None):
    ean_list = []
    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка штрихкодов')
    cursor.execute('''
            select sp3558 as ean_code, sp134 as coefficient, sp4802 as idartmarket, sc33.descr from SC131 
            left join sc33 on sc131.parentext = sc33.id
            where SP3558 > 0
            ''')
    logging.info('Выборка штрихкодов завершена')
    logging.info('Подготовка загрузки штрихкодов')
    row = cursor.fetchall()
    for r in tqdm(row):
        ean_list = []
        coefficient = 1
        if str(r['coefficient']).isdigit():
            coefficient = r['coefficient']
        nom = hdb_type(name=str(r['ean_code']), id=str(r['idartmarket']).strip(), idparent='',
                       value1num=coefficient)
        ean_list.append(nom)
        logging.info(r)

        hdb_array = hdb_array_type(hdb_array=ean_list)
        logging.info('Загрузка штрихкодов начало')
        wsdl_client.service.load_hdb_elements(hdb_array, 1, 'ean')
        logging.info('Загрузка штрихкодов завершена')


def unload_wholesale_min_price(cursor=None, wsdl_client=None):
    price_list = []
    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка минимальных цен')
    cursor.execute('''
            select code as idartmarket, descr, date, value, SP5833 as strength  from SC5646 elemement1 left join
            (
                select mdate,a.objid as idtovar,date,value  from ( select objid,id,max(date) as mdate from _1SCONST 
                where date<='2020-01-01' and (_1SCONST.id=5648) group by objid,id) a inner join
                (select objid,id,date,max(value) as value from _1SCONST where _1SCONST.id=5648 group by objid,id,date)
                b on a.objid=b.objid and mdate=date
            ) cdost on elemement1.id= cdost.idtovar where (isnull(cdost.date,'1978-01-01')<>'1978-01-01') order by descr
            ''')
    logging.info('Выборка минимальных цен завершена')
    logging.info('Подготовка загрузки минимальных цен')
    row = cursor.fetchall()
    for r in row:
        # if r['idartmarket'].strip() == '':
        #     logging.error(';'.join(['Пустой ид минимальных цен', r['descr']]))
        #     continue
        nom = hdb_type(name=r['descr'].strip(), id=str(r['idartmarket']).strip(), idparent='', value1num=r['strength'],
                       value2num=r['value'], value1date=r['date'])
        price_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=price_list)
    logging.info('Загрузка минимальных цен начало')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'minprice')
    logging.info('Загрузка минимальных цен завершена')



def get_str_select():
    return '''
            SELECT  elemement1.descr,elemement1.id,elemement1.code,elemement1.sp4802 as idartmarket,
            elemement1.SP3024 as fullname, elemement1.isfolder, elemement1.SP3694 as emkost,
            elemement1.SP6061 as fasovka, post0.sp4807 as postid0,
            groups1.sp4802 as idparent,groups1.descr as descrparent,
            post1.sp4807 as postid, post1.descr as postdexcr,
            groups2.sp4802 as idparent2,groups2.descr as descrparent2,
            post2.sp4807 as postid2, post2.descr as postdexcr2,
            groups3.sp4802 as idparent3,groups3.descr as descrparent3,
            post3.sp4807 as postid3, post3.descr as postdexcr3,
            groups4.sp4802 as idparent4,groups4.descr as descrparent4,
            post4.sp4807 as postid4, post4.descr as postdexcr4,
            groups5.sp4802 as idparent5,groups5.descr as descrparent5,
            post5.sp4807 as postid5, post5.descr as postdexcr5,
            groups6.sp4802 as idparent6,groups6.descr as descrparent6,
            post6.sp4807 as postid6, post6.descr as postdexcr6,
            groups7.sp4802 as idparent7,groups7.descr as descrparent7,
            post7.sp4807 as postid7, post7.descr as postdexcr7,
            groups8.sp4802 as idparent8,groups8.descr as descrparent8,
            post8.sp4807 as postid8, post8.descr as postdexcr8,
            groups9.sp4802 as idparent9,groups9.descr as descrparent9,
            post9.sp4807 as postid9, post9.descr as postdexcr9,
            cdost.value as pricedost, cdost.date as datedost, sc5468.SP6120 as idmaker,
            SC5646.code as idminprice, SC5900.code as idminprice_retail, elemement1.SP4950 as supplier_code,
            SC5525.code as alcohol_group_code
            FROM SC33 elemement1
            left join Sc46  post0 on elemement1.SP5257=post0.id
            left join SC33  groups1 on elemement1.parentid=groups1.id
            left join Sc46  post1 on groups1.SP5257=post1.id
            left join SC33  groups2 on groups1.parentid=groups2.id
            left join Sc46  post2 on groups2.SP5257=post2.id
            left join SC33  groups3 on groups2.parentid=groups3.id
            left join Sc46  post3 on groups3.SP5257=post3.id
            left join SC33  groups4 on groups3.parentid=groups4.id
            left join Sc46  post4 on groups4.SP5257=post4.id
            left join SC33  groups5 on groups4.parentid=groups5.id
            left join Sc46  post5 on groups5.SP5257=post5.id
            left join SC33  groups6 on groups5.parentid=groups6.id
            left join Sc46  post6 on groups6.SP5257=post6.id
            left join SC33  groups7 on groups6.parentid=groups7.id
            left join Sc46  post7 on groups7.SP5257=post7.id
            left join SC33  groups8 on groups7.parentid=groups8.id
            left join Sc46  post8 on groups8.SP5257=post8.id
            left join SC33  groups9 on groups8.parentid=groups9.id
            left join Sc46  post9 on groups9.SP5257=post9.id
            left join sc5468   on elemement1.SP5466=sc5468.id
            left join SC5646 on elemement1.SP5649=SC5646.id
            left join SC5900 on elemement1.SP5904=SC5900.id
            left join SC5525 on elemement1.SP5528=SC5525.id
            left join '''


def get_str_select_filial():
    return '''
                            SELECT  elemement1.descr,elemement1.id,elemement1.code,elemement1.code as idartmarket,
                            elemement1.SP101 as fullname, elemement1.isfolder, elemement1.SP8935 as emkost,
                            0 as fasovka,
                            groups1.code as idparent,groups1.descr as descrparent,
    						groups2.code as idparent2,groups2.descr as descrparent2,
    						groups3.code as idparent3,groups3.descr as descrparent3,
    						groups4.code as idparent4,groups4.descr as descrparent4,
    						groups5.code as idparent5,groups5.descr as descrparent5,
    						groups6.code as idparent6,groups6.descr as descrparent6,
    						groups7.code as idparent7,groups7.descr as descrparent7,
    						groups8.code as idparent8,groups8.descr as descrparent8,
    						groups9.code as idparent9,groups9.descr as descrparent9,
    						0 as pricedost, '1900-01-01' as datedost,
    						elemement1.SP8450 as idtovarfil
    						FROM SC84 elemement1
    						left join SC84  groups1 on elemement1.parentid=groups1.id
    						left join SC84  groups2 on groups1.parentid=groups2.id
    						left join SC84  groups3 on groups2.parentid=groups3.id
    						left join SC84  groups4 on groups3.parentid=groups4.id
    						left join SC84  groups5 on groups4.parentid=groups5.id
    						left join SC84  groups6 on groups5.parentid=groups6.id
    						left join SC84  groups7 on groups6.parentid=groups7.id
    						left join SC84  groups8 on groups7.parentid=groups8.id
    						left join SC84  groups9 on groups8.parentid=groups9.id
                            left join '''


def load_nomenklatura(cursor=None, prm_id_str='', prm_id_mode=1, prm_with_parent=0, prm_update_mode=0, prm_unload_price=0,
                      prm_unload_price_date='1900-01-01', wsdl_client=None, is_filial=0):
    # prm_id_mode 1- by id, 2 - by idartmarket, 3 - by tovar code
    # and (elemement1.sp4802 in ('''+str_id+'''))''')

    if (prm_id_str.strip() == '') and (not prm_unload_price > 0):
        return

    if prm_unload_price > 0:
        element_id_str = "((cdost.date='" + prm_unload_price_date + "'"
    else:
        if prm_id_mode == 1:
            element_id_str = '''(elemement1.id in ('''
        elif prm_id_mode == 2:
            element_id_str = '''(elemement1.sp4802 in ('''
        elif prm_id_mode == 3:
            element_id_str = '''(elemement1.code in ('''

    logging.info('Выборка элементов номенклатуры')
    logging.debug('Выборка элементов номенклатуры')
    logging.debug(element_id_str)
    logging.debug(prm_id_str)

    if is_filial == 1:
        str_select = get_str_select_filial()
    else:
        str_select = get_str_select()
    if prm_unload_price > 0:
        # 36-доставка
        # str_select=str_select+'''
        #                    (select a.objid as idtovar,value,date from (
        #                    select objid,id,max(date) as mdate from _1SCONST where date=('''+"'"+prm_unload_price_date+"'"+''') and (_1SCONST.id=36) group by objid,id) a
        #                    inner join (select objid,id,date,value from _1SCONST where _1SCONST.id=36) b on a.objid=b.objid and mdate=date ) cdost on elemement1.id= cdost.idtovar
        #                     where   (elemement1.isfolder=2) and (isnull(cdost.date,'1978-01-01')<>'1978-01-01') '''

        # date<=('''+"'"+prm_unload_price_date+"'"+''') все цены на дату первоначальная загрузка
        # date=('''+"'"+prm_unload_price_date+"'"+''') только изменения за дату
        str_select = str_select + '''
                            (select a.objid as idtovar,value,date from (
                            select objid,id,max(date) as mdate from _1SCONST where date=(''' + "'" + prm_unload_price_date + "'" + ''')
                            and (_1SCONST.id=''' + str(prm_unload_price) + ''') group by objid,id) a
                            inner join (select objid,id,date,max(value) as value from _1SCONST where _1SCONST.id=''' + str(
            prm_unload_price) + ''' group by objid,id,date) b on a.objid=b.objid
                            and mdate=date ) cdost on elemement1.id= cdost.idtovar
                             where   (elemement1.isfolder=2) and (isnull(cdost.date,'1978-01-01')<>'1978-01-01') 
                             order by elemement1.descr '''
    else:
        str_select = str_select + '''
                            (select a.objid as idtovar,value,date from (
                            select objid,id,max(date) as mdate from _1SCONST where _1SCONST.id=36 group by objid,id) a
                            inner join (select objid,id,date,value from _1SCONST where _1SCONST.id=36) b
                            on a.objid=b.objid and mdate=date ) cdost on elemement1.id= cdost.idtovar
                        where ''' + element_id_str + prm_id_str + '''))'''
        #logging.warning(str_select)
    logging.debug(str_select)
    cursor.execute(str_select)
    rows_nom = cursor.fetchall()
    tovar_list = []
    tovar_group_list = []
    logging.info('Подготовка загрузки номенклатуры')
    for row_nom in rows_nom:
        # logging.info(row_nom)
        if prm_with_parent == 1:
            if not row_nom['idparent9'] is None:
                if not row_nom['postid9'] is None:
                    postid = row_nom['postid9'].strip()
                else:
                    postid = ''
                nom_group = wsdl_client.nomenklatura_type(code='', name=row_nom['descrparent9'].strip(),
                                                          id=row_nom['idparent9'].strip(), idparent='', postid=postid)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent9'].strip()
            else:
                idparent_prev = ''
            if not row_nom['idparent8'] is None:
                if not row_nom['postid8'] is None:
                    postid = row_nom['postid8'].strip()
                else:
                    postid = ''
                nom_group = wsdl_client.nomenklatura_type(code='', name=row_nom['descrparent8'].strip(),
                                                          id=row_nom['idparent8'].strip(), idparent=idparent_prev,
                                                          postid=postid)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent8'].strip()
            else:
                idparent_prev = ''

            if not row_nom['idparent7'] is None:
                if not row_nom['postid7'] is None:
                    postid = row_nom['postid7'].strip()
                else:
                    postid = ''
                nom_group = wsdl_client.nomenklatura_type(code='', name=row_nom['descrparent7'].strip(),
                                                          id=row_nom['idparent7'].strip(), idparent=idparent_prev,
                                                          postid=postid)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent7'].strip()
            else:
                idparent_prev = ''

            if not row_nom['idparent6'] is None:
                if not row_nom['postid6'] is None:
                    postid = row_nom['postid6'].strip()
                else:
                    postid = ''
                nom_group = wsdl_client.nomenklatura_type(code='', name=row_nom['descrparent6'].strip(),
                                                          id=row_nom['idparent6'].strip(), idparent=idparent_prev,
                                                          postid=postid)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent6'].strip()
            else:
                idparent_prev = ''

            if not row_nom['idparent5'] is None:
                if not row_nom['postid5'] is None:
                    postid = row_nom['postid5'].strip()
                else:
                    postid = ''
                nom_group = wsdl_client.nomenklatura_type(code='', name=row_nom['descrparent5'].strip(),
                                                          id=row_nom['idparent5'].strip(), idparent=idparent_prev,
                                                          postid=postid)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent5'].strip()
            else:
                idparent_prev = ''

            if not row_nom['idparent4'] is None:
                if not row_nom['postid4'] is None:
                    postid = row_nom['postid4'].strip()
                else:
                    postid = ''
                nom_group = wsdl_client.nomenklatura_type(code='', name=row_nom['descrparent4'].strip(),
                                                          id=row_nom['idparent4'].strip(), idparent=idparent_prev,
                                                          postid=postid)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent4'].strip()
            else:
                idparent_prev = ''

            if not row_nom['idparent3'] is None:
                if not row_nom['postid3'] is None:
                    postid = row_nom['postid3'].strip()
                else:
                    postid = ''
                nom_group = wsdl_client.nomenklatura_type(code='', name=row_nom['descrparent3'].strip(),
                                                          id=row_nom['idparent3'].strip(), idparent=idparent_prev,
                                                          postid=postid)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent3'].strip()
            else:
                idparent_prev = ''

            if not row_nom['idparent2'] is None:
                if not row_nom['postid2'] is None:
                    postid = row_nom['postid'].strip()
                else:
                    postid = ''
                nom_group = wsdl_client.nomenklatura_type(code='', name=row_nom['descrparent2'].strip(),
                                                          id=row_nom['idparent2'].strip(), idparent=idparent_prev,
                                                          postid=postid)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent2'].strip()
            else:
                idparent_prev = ''

            if not row_nom['idparent'] is None:
                if not row_nom['postid'] is None:
                    postid = row_nom['postid'].strip()
                else:
                    postid = ''
                nom_group = wsdl_client.nomenklatura_type(code='', name=row_nom['descrparent'].strip(),
                                                          id=row_nom['idparent'].strip(), idparent=idparent_prev,
                                                          postid=postid)
                tovar_group_list.append(nom_group)

        if row_nom['idparent'] != None:
            idparent_prev = row_nom['idparent'].strip()
        else:
            idparent_prev = ''

        if not row_nom['pricedost'] is None:
            pricedostval = row_nom['pricedost']
        else:
            pricedostval = 0

        if is_filial == 1:
            if row_nom['code'].strip().isdigit():
                #continue
                nom = wsdl_client.nomenklatura_type(code=row_nom['code'].strip(), name=row_nom['descr'].strip(),
                                                    id=row_nom['idartmarket'].strip(), idparent=idparent_prev,
                                                    emkost=row_nom['emkost'], pricedost=pricedostval,
                                                    datedost=row_nom['datedost'])
            else:
                nom = wsdl_client.nomenklatura_type(code=0, name=row_nom['descr'].strip(),
                                                    id=row_nom['idtovarfil'].strip(), idparent=filial_tovar_group_id,
                                                    emkost=row_nom['emkost'], pricedost=pricedostval,
                                                    datedost=row_nom['datedost'])
                logging.error(['Некорректный код товара', row_nom['code'].strip(), row_nom['descr'].strip(),
                               row_nom['idtovarfil'].strip()])
                #continue
            if row_nom['isfolder'] == 1:
                tovar_group_list.append(nom)
            else:
                tovar_list.append(nom)
        else:
            if row_nom['code'].strip().isdigit():
                if not row_nom['postid0'] is None:
                    postid = row_nom['postid0'].strip()
                else:
                    postid = ''

                if not row_nom['idmaker'] is None:
                    makerid = row_nom['idmaker'].strip()
                else:
                    makerid = ''

                if not row_nom['idminprice'] is None:
                    minpriceid = row_nom['idminprice'].strip()
                else:
                    minpriceid = ''

                if not row_nom['idminprice_retail'] is None:
                    minpriceid_retail = row_nom['idminprice_retail'].strip()
                else:
                    minpriceid_retail = ''

                nom = wsdl_client.nomenklatura_type(code=row_nom['code'].strip(), name=row_nom['descr'].strip(),
                                                    id=row_nom['idartmarket'].strip(), idparent=idparent_prev,
                                                    emkost=row_nom['emkost'], pricedost=pricedostval,
                                                    datedost=row_nom['datedost'], fasovka=row_nom['fasovka'],
                                                    postid=postid, makerid=makerid, minpriceid=minpriceid,
                                                    minpriceid_retail=minpriceid_retail,
                                                    supplier_code=row_nom['supplier_code'].strip(),
                                                    alcohol_group_code=row_nom['alcohol_group_code'])
            else:
                logging.error(['Некорректный код товара', row_nom['code'].strip(), row_nom['descr'].strip()])
                continue
            if row_nom['isfolder'] == 1:
                tovar_group_list.append(nom)
            else:
                tovar_list.append(nom)

    arrayn_group = wsdl_client.arrayn_type(nomenklatura=tovar_group_list)
    if is_filial == 1:
        wsdl_client.client.service.load_nom_groups_filial(arrayn_group, prm_update_mode)
    else:
        wsdl_client.client.service.load_nom_groups(arrayn_group, prm_update_mode)

    arrayn = wsdl_client.arrayn_type(nomenklatura=tovar_list)
    #logging.warning(tovar_list)
    # print(tovar_group_list)
    if is_filial == 1:
        logging.info('Загрузка номенклатуры начало фил')
        wsdl_client.client.service.load_nom_elements_filial(arrayn, prm_update_mode, prm_unload_price,
                                                            prm_unload_price_date)
    else:
        logging.info('Загрузка номенклатуры начало')
        wsdl_client.client.service.load_nom_elements(arrayn, prm_update_mode, prm_unload_price, prm_unload_price_date)
    logging.info('Загрузка номенклатуры завершена')
    logging.debug('Загрузка номенклатуры завершена')
    logging.debug(tovar_list)

    logging.info('Выборка элементов номенклатуры завершена')
