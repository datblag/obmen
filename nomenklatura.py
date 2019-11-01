import logging
#import logs
from sql import cursor
from wsdl import nomenklatura_type,arrayn_type,client

#from logging.handlers import TimedRotatingFileHandler


#logs.run()

def load_nomenklatura(prm_id_str='', prm_id_mode=1, prm_with_parent=0, prm_update_mode=0, prm_unload_price=0,
                      prm_unload_price_date='1900-01-01'):
    # prm_id_mode 1- by id, 2 - by idartmarket
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

    logging.info('Выборка элементов номенклатуры')
    logging.debug('Выборка элементов номенклатуры')
    logging.debug(element_id_str)
    logging.debug(prm_id_str)

    str_select = '''
                        SELECT  elemement1.descr,elemement1.id,elemement1.code,elemement1.sp4802 as idartmarket,elemement1.SP3024 as fullname, elemement1.isfolder, elemement1.SP3694 as emkost,
                        groups1.sp4802 as idparent,groups1.descr as descrparent,
						groups2.sp4802 as idparent2,groups2.descr as descrparent2,
						groups3.sp4802 as idparent3,groups3.descr as descrparent3,
						groups4.sp4802 as idparent4,groups4.descr as descrparent4,
						groups5.sp4802 as idparent5,groups5.descr as descrparent5,
						groups6.sp4802 as idparent6,groups6.descr as descrparent6,
						groups7.sp4802 as idparent7,groups7.descr as descrparent7,
						groups8.sp4802 as idparent8,groups8.descr as descrparent8,
						groups9.sp4802 as idparent9,groups9.descr as descrparent9,
						cdost.value as pricedost, cdost.date as datedost
						FROM SC33 elemement1
						left join SC33  groups1 on elemement1.parentid=groups1.id
						left join SC33  groups2 on groups1.parentid=groups2.id
						left join SC33  groups3 on groups2.parentid=groups3.id
						left join SC33  groups4 on groups3.parentid=groups4.id
						left join SC33  groups5 on groups4.parentid=groups5.id
						left join SC33  groups6 on groups5.parentid=groups6.id
						left join SC33  groups7 on groups6.parentid=groups7.id
						left join SC33  groups8 on groups7.parentid=groups8.id
						left join SC33  groups9 on groups8.parentid=groups9.id
                        left join '''
    if prm_unload_price > 0:
        # 36-доставка
        # str_select=str_select+'''
        #                    (select a.objid as idtovar,value,date from (
        #                    select objid,id,max(date) as mdate from _1SCONST where date<=('''+"'"+prm_unload_price_date+"'"+''') and (_1SCONST.id=36) group by objid,id) a
        #                    inner join (select objid,id,date,value from _1SCONST where _1SCONST.id=36) b on a.objid=b.objid and mdate=date ) cdost on elemement1.id= cdost.idtovar
        #                     where   (elemement1.isfolder=2) and (isnull(cdost.date,'1978-01-01')<>'1978-01-01') '''

        # date<=('''+"'"+prm_unload_price_date+"'"+''') все цены на дату первоначальная загрузка
        # date=('''+"'"+prm_unload_price_date+"'"+''') только изменения за дату
        str_select = str_select + '''
                            (select a.objid as idtovar,value,date from (
                            select objid,id,max(date) as mdate from _1SCONST where date<=(''' + "'" + prm_unload_price_date + "'" + ''')
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

    logging.debug(str_select)
    cursor.execute(str_select)
    rows_nom = cursor.fetchall()
    tovar_list = []
    tovar_group_list = []
    logging.info('Подготовка загрузки номенклатуры')
    for row_nom in rows_nom:
        # print(row_nom)
        if prm_with_parent == 1:
            if row_nom['idparent9'] != None:
                nom_group = nomenklatura_type(code='', name=row_nom['descrparent9'].strip(),
                                              id=row_nom['idparent9'].strip(), idparent='')
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent9'].strip()
            else:
                idparent_prev = ''
            if row_nom['idparent8'] != None:
                nom_group = nomenklatura_type(code='', name=row_nom['descrparent8'].strip(),
                                              id=row_nom['idparent8'].strip(), idparent=idparent_prev)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent8'].strip()
            else:
                idparent_prev = ''

            if row_nom['idparent7'] != None:
                nom_group = nomenklatura_type(code='', name=row_nom['descrparent7'].strip(),
                                              id=row_nom['idparent7'].strip(), idparent=idparent_prev)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent7'].strip()
            else:
                idparent_prev = ''

            if row_nom['idparent6'] != None:
                nom_group = nomenklatura_type(code='', name=row_nom['descrparent6'].strip(),
                                              id=row_nom['idparent6'].strip(), idparent=idparent_prev)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent6'].strip()
            else:
                idparent_prev = ''

            if row_nom['idparent5'] != None:
                nom_group = nomenklatura_type(code='', name=row_nom['descrparent5'].strip(),
                                              id=row_nom['idparent5'].strip(), idparent=idparent_prev)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent5'].strip()
            else:
                idparent_prev = ''

            if row_nom['idparent4'] != None:
                nom_group = nomenklatura_type(code='', name=row_nom['descrparent4'].strip(),
                                              id=row_nom['idparent4'].strip(), idparent=idparent_prev)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent4'].strip()
            else:
                idparent_prev = ''

            if row_nom['idparent3'] != None:
                nom_group = nomenklatura_type(code='', name=row_nom['descrparent3'].strip(),
                                              id=row_nom['idparent3'].strip(), idparent=idparent_prev)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent3'].strip()
            else:
                idparent_prev = ''

            if row_nom['idparent2'] != None:
                nom_group = nomenklatura_type(code='', name=row_nom['descrparent2'].strip(),
                                              id=row_nom['idparent2'].strip(), idparent=idparent_prev)
                tovar_group_list.append(nom_group)
                idparent_prev = row_nom['idparent2'].strip()
            else:
                idparent_prev = ''

            if row_nom['idparent'] != None:
                nom_group = nomenklatura_type(code='', name=row_nom['descrparent'].strip(),
                                              id=row_nom['idparent'].strip(), idparent=idparent_prev)
                tovar_group_list.append(nom_group)

        if row_nom['idparent'] != None:
            idparent_prev = row_nom['idparent'].strip()
        else:
            idparent_prev = ''

        if row_nom['pricedost'] != None:
            pricedostval = row_nom['pricedost']
        else:
            pricedostval = 0

        nom = nomenklatura_type(code=row_nom['code'].strip(), name=row_nom['descr'].strip(),
                                id=row_nom['idartmarket'].strip(), idparent=idparent_prev,
                                emkost=row_nom['emkost'], pricedost=pricedostval, datedost=row_nom['datedost'])
        if row_nom['isfolder'] == 1:
            tovar_group_list.append(nom)
        else:
            tovar_list.append(nom)

    arrayn_group = arrayn_type(nomenklatura=tovar_group_list)
    client.service.load_nom_groups(arrayn_group, prm_update_mode)

    arrayn = arrayn_type(nomenklatura=tovar_list)
    # print(tovar_group_list)
    logging.info('Загрузка номенклатуры начало')
    client.service.load_nom_elements(arrayn, prm_update_mode, prm_unload_price, prm_unload_price_date)
    logging.info('Загрузка номенклатуры завершена')
    logging.debug('Загрузка номенклатуры завершена')
    logging.debug(tovar_list)

    logging.info('Выборка элементов номенклатуры завершена')
