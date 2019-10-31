# -*- encoding: utf-8 -*-
from logging.handlers import TimedRotatingFileHandler
import logging
import logs
logs.run()
from config import logname,logname_debug,logname_error
#import pymssql
from logging.handlers import TimedRotatingFileHandler
import datetime
from datetime import date, timedelta
import time
from tqdm import *
#from config import cb_sql_address,cb_sql_user_name,cb_sql_password,cb_sql_database
import nomenklatura
import prihod
from wsdl import *
import sql
from sql import cursor,conn
import hdb
#import timeit
#import profile
#from config import prod_server_address,prod_server_user,prod_server_password
#import sys
#from zeep import Client
#from lxml import etree
#import os
#from logging import StreamHandler
#from requests import Session
#from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
#from zeep.transports import Transport


# def get_nomenklatura_groups(prm_cursor,prm_parent_id,prm_parent_id_2,prm_list,prm_nomenklatura_type,prm_arrayn_type):
#     prm_cursor.execute('''SELECT descr,id,code,sp4802 as idartmarket,SP3024 as fullname
#                                  from sc33 where (isfolder='1') and (ltrim(rtrim(parentid))= %s)''',prm_parent_id)
#     row = prm_cursor.fetchall()
#     for r in row:
#         nom=nomenklatura_type(code=r[2].strip(),name=r[0].strip(),id=r[3].strip(),idparent=prm_parent_id_2)
#         prm_list.append(nom)
#         get_nomenklatura_groups(prm_cursor,r[1].strip(),r[3].strip(),prm_list,prm_nomenklatura_type,prm_arrayn_type)


# def get_nomenklatura_elements1(prm_cursor,prm_parent_id,prm_parent_id_2,prm_list,prm_nomenklatura_type,prm_arrayn_type,prm_id_list):
#     logging.info('Выборка элементов номенклатуры')
#     prm_cursor.execute('''SELECT  elemement1.descr,elemement1.id,elemement1.code,elemement1.sp4802 as idartmarket,elemement1.SP3024 as fullname,
#     groups1.sp4802 as idparent FROM SC33 elemement1 left join SC33  groups1 on elemement1.parentid=groups1.id where elemement1.isfolder<>'1'
#     and element1.id in %s''',prm_id_list)
#     logging.info('Выборка элементов номенклатуры завершена')
#     row = cursor.fetchall()
#     for r in row:
#         nom=nomenklatura_type(code=r[2].strip(),name=r[0].strip(),id=r[3].strip(),idparent=r[5].strip())
#         prm_list.append(nom)
#         #get_nom_groups(prm_cursor,r[1].strip(),r[3].strip(),prm_list,prm_nomenklatura_type,prm_arrayn_type)





def get_region_groups(prm_cursor,prm_id_list=''):
    hdb_type = client.get_type('ns3:hdb_element')
    hdb_array_type = client.get_type('ns3:hdb_array_element')

    list=[]
    logging.info('Выборка регионов уровень 1')

    #nom_agent=hdb_type(name=row['agentname'].strip(),id=row['agent'].strip(),idparent='')
    #hdb_array=hdb_array_type(hdb_array=[nom_agent])
    #logging.info('Загрузка агента начало')
    #client.service.load_hdb_elements(hdb_array,1,'agent')
    #logging.info('Загрузка агента завершена')


    prm_cursor.execute('''select sp4807 as idartmarket,descr,'' as idparent,'' as nameparent from
                      sc46 where ltrim(rtrim(parentid))='0' and isfolder=1''')


    logging.info('Выборка регионов уровень 1')
    logging.info('Подготовка регионов уровень 1')

    row = cursor.fetchall()  
    for r in row:
        nom_region=hdb_type(name=r['descr'].strip(),id=r['idartmarket'].strip(),idparent='')
        list.append(nom_region)

    logging.info('Выборка регионов уровень 2')

    prm_cursor.execute('''select sc46.sp4807 as idartmarket,sc46.descr,scparent.sp4807 as idparent,scparent.descr as nameparent from sc46
    left join sc46 as scparent on sc46.parentid=scparent.id
     where (sc46.parentid in (select id from sc46 where isfolder=1 and ltrim(rtrim(parentid))='0')) and sc46.isfolder=1''')

    logging.info('Выборка регионов уровень 2')
    logging.info('Подготовка регионов уровень 2')
    row = cursor.fetchall()  
    for r in row:
        nom_region=hdb_type(name=r['descr'].strip(),id=r['idartmarket'].strip(),idparent=r['idparent'])
        list.append(nom_region)

    hdb_array=hdb_array_type(hdb_array=list)
    logging.info('Загрузка регионов начало')
    client.service.load_hdb_elements(hdb_array,1,'region')
    logging.info('Загрузка регионов завершена')





logging.warning('Начало работы')




#client.wsdl.dump()

list=[]

#os.environ['TDSDUMP'] = 'stdout'





#try:
while True:
    k=input('Введите команду:')
    if k=='0':
        break
    elif k == 'ценынач':
        nomenklatura.load_nomenklatura(prm_id_str='', prm_id_mode=1, prm_with_parent=0, prm_update_mode=1,
                                       prm_unload_price=4549, prm_unload_price_date='2018-12-31')
    elif k=='цены':
        #36-доставка 38-приобретение, 4549 закуп
        #выгрузка истории
        end_date = date(2019, 10,20)
        delta = timedelta(days=1)
        price_type_to_load=[36,38]
        for price_type in price_type_to_load:
            print(price_type)
            start_date = date(2019, 10, 2)
            while start_date <= end_date:
                k2=start_date.strftime("%Y-%m-%d")
                print(k2)
                start_date += delta
                nomenklatura.load_nomenklatura(prm_id_str='',prm_id_mode=1,prm_with_parent=0,prm_update_mode=1,prm_unload_price=price_type,prm_unload_price_date=k2)
    elif k == 'документ7':
        #434 - приход
        start_date = date(2018, 1, 1)
        end_date = date(2018, 1,31)
        doc_type_list=[{'typeid':434,'typename':'приход','idfield':'SP6059','sumfield':'sp453'}]
        for doc_type in doc_type_list:
            print(doc_type['sumfield'])
            cursor.execute('''select  closed, _1sjourn.iddocdef as doctype, _1sjourn.iddoc, docno as docno, CAST(LEFT(_1sjourn.Date_Time_IDDoc, 8) as DateTime) as docdate,
            dt.sm,'''+doc_type['idfield']+''' as idartmarket  from _1sjourn
            left join dh'''+str(doc_type['typeid'])+''' dh on _1sjourn.iddoc = dh.iddoc
            left join(select sum('''+doc_type['sumfield']+''') as sm, iddoc from dt'''+str(doc_type['typeid'])+''' group by  iddoc) dt on _1sjourn.iddoc = dt.iddoc
            where(iddocdef='''+str(doc_type['typeid'])+''') and (CAST(LEFT(_1sjourn.Date_Time_IDDoc, 8) as DateTime) between '''+
                           "'" + start_date.strftime("%Y-%m-%d") + "'" +''' and '''+
                           "'" + end_date.strftime("%Y-%m-%d") + "'" +''')''')
            rows_doc=cursor.fetchall()
            for row_doc in rows_doc:
                pass
                #print(row_doc)
    elif k=='авто':
        white_list=[]
        if 1==1:

            #расход
            white_list.append(3716) #расходнаядоставка
            white_list.append(410) #расходнаянакладная
            white_list.append(469) #расходнаяреализатора

            white_list.append(33) #номенклатура
        
            white_list.append(239) #перемещение
            white_list.append(310) #ввод остатков
            white_list.append(434) #приход
            white_list.append(2989) #движенияденежныхсредств
            white_list.append(4308) #выручкадоставка
            white_list.append(2964) #ПриходныйОрдерТБ
            white_list.append(4179) #АктПереоценкиКлиенты
            white_list.append(4225) #РасходныйОрдерТБ
        #if 1==1:

            #взаиморасчеты
        if 1==1:
            white_list.append(297) #списания

        while True:
            logging.warning('Выборка изменений')
            # коды баз 'P1 ','БП '
            #типы объектов: номенклатура - 33, 
            try:
                cursor.execute('''update  _1SUPDTS set DWNLDID='1122!!' where (DBSIGN = 'P1 ') and not (DWNLDID='1122!!')''')  
                conn.commit()
            except:
                logging.error('ошибка при обновлении таблицы _1SUPDTS')
            #    pass
            cursor.execute('''SELECT  * from _1SUPDTS WITH (NOLOCK) where (DBSIGN = 'P1 ') and (DWNLDID='1122!!')''')  
            rows_delta = cursor.fetchall()
            for row_delta in tqdm(rows_delta):
                if not (row_delta['TYPEID'] in white_list):
                    continue
                    pass
                #номенклатура
                if row_delta['TYPEID']==33: 
                    #print(row_delta)
                    str_id=",".join(["'"+row_delta['OBJID']+"'"])
                    #load_nomenklatura(prm_id_str=str_id,prm_id_mode=1,prm_with_parent=1,prm_update_mode=1)                                  
                    nomenklatura.load_nomenklatura(prm_id_str=str_id,prm_id_mode=1,prm_with_parent=0,prm_update_mode=1)
                
                #перемещение
                elif row_delta['TYPEID']==239: 
                    #continue
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

        
                    ''',row_delta['OBJID'])  
                    logging.info('Выборка перемещение заголовки завершена')
                    rows_header = cursor.fetchall()

                    for row_header in rows_header:
                        if row_header['firma']!='9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
                            continue
                        if row_header['idartmarket']==None or row_header['idartmarket'].strip()=='':
                            logging.error(';'.join(['Пустой ид',row_header['docno']]))
                            continue
                        if row_header['sklad']==None or row_header['sklad'].strip()=='':
                            logging.error(';'.join(['Пустой склад',row_header['docno']]))
                            continue

                        if row_header['sklad_in']==None or row_header['sklad_in'].strip()=='':
                            logging.error(';'.join(['Пустой склад получатель',row_header['docno']]))
                            continue


                        header=header_type(document_type=2,firma=row_header['firma'].strip(),sklad=row_header['sklad'].strip(),client=row_header['sklad_in'].strip(),idartmarket=row_header['idartmarket'].strip()
                                            ,document_date=row_header['datedoc'],nomerartmarket=row_header['docno'])

                        isclosed=row_header['closed'] and 1

                        logging.info(';'.join(['Выборка строк перемещение',row_header['docno']]))
                        cursor.execute('''
                                select  sc33.sp4802 as idtovar,SP250 as kolvo,SP1034 as koef,0 as price,0 as sum from dt239
                                left join sc33 on SP249=sc33.id
                                where iddoc=%s
                        ''',row_delta['OBJID'])
                        logging.info(';'.join(['Выборка строк перемещение завершена',row_header['docno']]))
                        rows_table = cursor.fetchall()
                        row_list=[]
                        tovar_list=[]

                        for row_table in rows_table:
                            row_nom=row_type(tovar=row_table['idtovar'],quantity=row_table['kolvo'],price=row_table['price'],koef=row_table['koef'],sum=row_table['sum'])
                            if row_table['idtovar']==None:
                                continue
                            if not "'"+row_table['idtovar']+"'" in tovar_list:
                                tovar_list.append("'"+row_table['idtovar']+"'")
                            row_list.append(row_nom)

                        rows=rows_type(rows=row_list)
                        str_id=",".join(tovar_list)
                        nomenklatura.load_nomenklatura(prm_id_str=str_id,prm_id_mode=2,prm_with_parent=0,prm_update_mode=0)



                        document=document_type(header=header,rowslist=rows)
                        logging.info(';'.join(['Загрузка документа перемещение',row_header['docno']]))
                        n=client.service.load_peremesh(document,isclosed)
                        logging.info(';'.join(['Загрузка документа перемещение',row_header['docno'],n]))


                #списание
                elif row_delta['TYPEID']==297: 
                #списание
                    logging.info('Выборка списания заголовки')
                    cursor.execute('''
        
                    SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                    sc13.sp4805 as firma,sc31.SP5639 as sklad,SP6076 as idartmarket,
                    _1sjourn.iddoc FROM dh297   as dh WITH (NOLOCK)
                    left join _1sjourn WITH (NOLOCK)  on dh.iddoc=_1sjourn.iddoc 
                    left join sc31 WITH (NOLOCK)  on SP300 = sc31.id
                    left join sc13 WITH (NOLOCK)  on SP1005=sc13.id

                    
                    where _1sjourn.iddoc=%s

        
                    ''',row_delta['OBJID'])  
                    logging.info('Выборка списания заголовки завершена')

                    rows_header = cursor.fetchall()

                    for row_header in rows_header:
                        if row_header['firma']!='9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
                            continue
                        if row_header['idartmarket']==None or row_header['idartmarket'].strip()=='':
                            logging.error(';'.join(['Пустой ид',row_header['docno']]))
                            continue
                        if row_header['sklad']==None or row_header['sklad'].strip()=='':
                            logging.error(';'.join(['Пустой склад',row_header['docno']]))
                            continue


                        header=header_type(document_type=2,firma=row_header['firma'].strip(),sklad=row_header['sklad'].strip(),client='',idartmarket=row_header['idartmarket'].strip()
                                            ,document_date=row_header['datedoc'],nomerartmarket=row_header['docno'])

                        isclosed=row_header['closed'] and 1

                        logging.info(';'.join(['Выборка строк списания',row_header['docno']]))
                        cursor.execute('''
                                select  sc33.sp4802 as idtovar,SP304 as kolvo,SP306 as koef,0 as price,0 as sum from dt297
                                left join sc33 on SP303=sc33.id
                                where iddoc=%s
                        ''',row_delta['OBJID'])
                        logging.info(';'.join(['Выборка строк списания завершена',row_header['docno']]))
                        rows_table = cursor.fetchall()
                        row_list=[]
                        tovar_list=[]

                        for row_table in rows_table:
                            row_nom=row_type(tovar=row_table['idtovar'],quantity=row_table['kolvo'],price=row_table['price'],koef=row_table['koef'],sum=row_table['sum'])
                            if row_table['idtovar']==None:
                                continue
                            if not "'"+row_table['idtovar']+"'" in tovar_list:
                                tovar_list.append("'"+row_table['idtovar']+"'")
                            row_list.append(row_nom)

                        rows=rows_type(rows=row_list)
                        str_id=",".join(tovar_list)
                        nomenklatura.load_nomenklatura(prm_id_str=str_id,prm_id_mode=2,prm_with_parent=0,prm_update_mode=0)


                        list_partii=[]
                        if isclosed==1:
                            logging.info('Выборка партий списания')
                            cursor.execute('''
                                select 
                                sp4802 as idtovar_artmarket, ltrim(rtrim(_1sjourn.iddoc)) as prihodid, _1sjourn.iddocdef as prihodtype,
                                docno as prihodno,CAST(LEFT(_1sjourn.Date_Time_IDDoc, 8) as DateTime) as prihoddate,
                                SP1133 as ostatok, SP2655 as stoimost, SP2799 as prodstoimost, SP4307 as prodaga
                                from ra1130 WITH (NOLOCK)
                                left join sc33 WITH (NOLOCK) on ra1130.sp1131 = sc33.id
                                left join _1sjourn WITH (NOLOCK) on ltrim(rtrim(substring(ltrim(rtrim(sp1132)),charindex(' ',ltrim(rtrim(sp1132))),100)))=ltrim(rtrim(_1sjourn.iddoc))
                                where ra1130.iddoc=%s
                            ''',row_delta['OBJID'])
                            logging.info('Выборка партий списаний завершена')
                            rows_table_partii = cursor.fetchall()
                            for row_partii in rows_table_partii:
                                row_nom_partii=row_partii_type(tovar=row_partii['idtovar_artmarket'],
                                                               prihod_id=row_partii['prihodid'],
                                                               prihod_type=row_partii['prihodtype'],
                                                               prihod_no=row_partii['prihodno'],
                                                               prihod_date=row_partii['prihoddate'],
                                                               ostatok=row_partii['ostatok'],
                                                               stoimost=row_partii['stoimost'],
                                                               prodstoimost=row_partii['prodstoimost'],
                                                               prodaga=row_partii['prodaga'])
                                list_partii.append(row_nom_partii)
                        document_partii_rows=rows_partii_type(rows=list_partii)
                        document_partii=document_partii_type(rowslist=document_partii_rows)



                        document=document_type(header=header,rowslist=rows)
                        logging.info(';'.join(['Загрузка документа списание',row_header['docno']]))
                        n=client.service.load_spisanie(document,document_partii,isclosed)
                        logging.info(';'.join(['Загрузка документа списание',row_header['docno'],n]))


                 #оприходование
                elif row_delta['TYPEID']==310:
                    #оприходование
                    #continue
                    logging.info('Выборка ввод остатков заголовки')
                    cursor.execute('''
        
                    SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                    sc13.sp4805 as firma,sc31.SP5639 as sklad,SP6077 as idartmarket,
                    _1sjourn.iddoc FROM dh310 as dh
                    left join _1sjourn on dh.iddoc=_1sjourn.iddoc 
                    left join sc31 on SP312 = sc31.id
                    left join sc13 on SP1005=sc13.id
                    where _1sjourn.iddoc=%s

        
                    ''',row_delta['OBJID'])  
                    logging.info('Выборка ввод остатков заголовки завершена')
                    rows_header = cursor.fetchall()

                    for row_header in rows_header:
                        if row_header['firma']!='9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
                            continue
                        if row_header['idartmarket']==None or row_header['idartmarket'].strip()=='':
                            logging.error(';'.join(['Пустой ид',row_header['docno']]))
                            continue
                        if row_header['sklad']==None or row_header['sklad'].strip()=='':
                            logging.error(';'.join(['Пустой склад',row_header['docno']]))
                            continue


                        header=header_type(document_type=2,firma=row_header['firma'].strip(),sklad=row_header['sklad'].strip(),client='',idartmarket=row_header['idartmarket'].strip()
                                            ,document_date=row_header['datedoc'],nomerartmarket=row_header['docno'])

                        isclosed=row_header['closed'] and 1

                        logging.info(';'.join(['Выборка строк ввод остатка',row_header['docno']]))
                        cursor.execute('''
                                select  sc33.sp4802 as idtovar,SP316 as kolvo,SP318 as koef,SP4716 as price,SP4717 as sum from dt310
                                left join sc33 on sp315=sc33.id
                                where iddoc=%s
                        ''',row_delta['OBJID'])
                        logging.info(';'.join(['Выборка строк ввод остатка завершена',row_header['docno']]))
                        rows_table = cursor.fetchall()
                        row_list=[]
                        tovar_list=[]

                        for row_table in rows_table:
                            row_nom=row_type(tovar=row_table['idtovar'],quantity=row_table['kolvo'],price=row_table['price'],koef=row_table['koef'],sum=row_table['sum'])
                            if row_table['idtovar']==None:
                                continue
                            if not "'"+row_table['idtovar']+"'" in tovar_list:
                                tovar_list.append("'"+row_table['idtovar']+"'")
                            row_list.append(row_nom)

                        rows=rows_type(rows=row_list)
                        str_id=",".join(tovar_list)
                        nomenklatura.load_nomenklatura(prm_id_str=str_id,prm_id_mode=2,prm_with_parent=0,prm_update_mode=0)




                        document=document_type(header=header,rowslist=rows)
                        logging.info(';'.join(['Загрузка документа ввод остатка',row_header['docno']]))
                        n=client.service.load_vvodostatka_tovar(document,isclosed)
                        logging.info(';'.join(['Загрузка документа ввод остатка',row_header['docno'],n]))
                
                #взаиморасчеты
                elif row_delta['TYPEID'] in [2989,4308,2964,4179,4225]: #взаиморасчеты
                    #взаиморасчеты
                    #2989 - движенияденежныхсредств
                    #4308 - выручкадоставка SP6083
                    #2964 - ПриходныйОрдерТБ Прих.орд.(торг.) SP6084
                    #4179 - АктПереоценкиКлиенты Акт переоц. SP6085
                    #4225 РасходныйОрдерТБ SP6082
                    logging.info(';'.join(['Выборка взаиморасчетов',str(row_delta['TYPEID']),row_delta['OBJID']]))
                    if (row_delta['TYPEID']==2989) :
                        idartmarket_str='SP6081'
                        doc_descr='Выписка банка'
                    elif (row_delta['TYPEID']==4308) :
                        idartmarket_str='SP6083'
                        doc_descr='Выручка доставка'
                    elif (row_delta['TYPEID']==2964) :
                        idartmarket_str='SP6084'
                        doc_descr='Прих.орд.(торг.)'
                    elif (row_delta['TYPEID']==4179) :
                        idartmarket_str='SP6085'
                        doc_descr='Акт переоц.'
                    elif (row_delta['TYPEID']==4225) :
                        idartmarket_str='SP6082'
                        doc_descr='Расх.орд.(торг.)'

                    select_str='''
                    SELECT   closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
                    sc13.sp4805 as firma,
                    '''+idartmarket_str+''' as idartmarket,
                    _1sjourn.iddoc FROM DH'''+str(row_delta['TYPEID'])+''' as dh
                    left join _1sjourn on dh.iddoc=_1sjourn.iddoc 
                    left join sc13 on SP1005=sc13.id
                    where _1sjourn.iddoc=%s
                    '''    
                    cursor.execute(select_str,row_delta['OBJID'])  
                    
                    rows_header=cursor.fetchall()

                    logging.info('Выборка взаиморасчетов завершена')


                    for row in rows_header:
                        client_list=[]
                        isclosed=row['closed'] and 1
                        if isclosed!=1:
                            #continue
                            pass

                        if row['firma']!='9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
                            continue
                        if row['idartmarket']==None or row['idartmarket'].strip()=='':
                            if isclosed==1:
                                logging.error(';'.join(['Пустой ид',row['docno']]))
                            continue

                        header=header_type(document_type=2,firma=row['firma'].strip(),sklad=doc_descr,client='',idartmarket=row['idartmarket'].strip()
                                            ,document_date=row['datedoc'],nomerartmarket=row['docno'])

                        
                        #расход debkred 1

                        #покупатели
                        #debkred 1 уменьшение долга клиента
                        #debkred 0 увеличение долга клиента
                        #SP4372 - кред документ, документ основание
                        #TODO добавить документ основание
                        cursor.execute('''select  sc46.sp4807 as client,debkred, sp171 as summa,
                                            2 as typedvig, sp4372 as docosnov from ra169
                                            left join sc46 on ltrim(rtrim(SP170)) = '1A   '+ltrim(rtrim(sc46.id))
                                            where ra169.iddoc=%s
                                            ''',row['iddoc'])

                        rows_table_1 = cursor.fetchall()

                        #поставщики
                        #debkred 1 уменьшение долга клиента
                        #debkred 0 увеличение долга клиента
                        cursor.execute('''
                                        select sc46.sp4807 as client,debkred, SP936 as summa,
                                        1 as typedvig from ra933
                                        left join sc46 on SP934 = sc46.id
                                        where ra933.iddoc=%s
                                            ''',row['iddoc'])

                        rows_table_2 = cursor.fetchall()

                        rows_table=rows_table_1+rows_table_2

                        client_list=[]
                        row_list=[]



                        for row_table in rows_table:
                            if row_table['debkred']:
                                debkred=1
                            else:
                                debkred=2
                            


                            if debkred==1 and row_table['typedvig']==1:
                                logging.error(';'.join(['контроль операции',row['docno']]))

                            if row_table['client']==None:
                                continue
                            row_nom=row_type(tovar=row_table['client'],quantity=row_table['typedvig'],price=0,koef=debkred,sum=row_table['summa'])
                            if not "'"+row_table['client']+"'" in client_list:
                                client_list.append("'"+row_table['client']+"'")
                            row_list.append(row_nom)
                        if client_list==[]:
                            continue
                        str_id=",".join(client_list)
                        hdb.get_client_groups(cursor,str_id)
                        #print(row_list)
                        rows=rows_type(rows=row_list)
                        document=document_type(header=header,rowslist=rows)
                        logging.info(';'.join(['Загрузка документа взаиморасчетов',row['docno']]))
                        n=client.service.load_client_rashet(document,isclosed)
                        logging.info(';'.join(['Загрузка документа взаиморасчетов',row['docno'],n]))


                #расход
                elif row_delta['TYPEID'] in [410,469,3716]:
                    #расходы 410 - расходнаянакладная экспедитор SP4485
                    #расходы 469 - расходнаяреализатора экспедитор SP4487
                    #расходы 3716 - расходнаядоставка экспедитор SP3745
                    logging.info('Выборка расходов заголовки')

                    if (row_delta['TYPEID']==410) :
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
                        ''',row_delta['OBJID'])  
                    elif row_delta['TYPEID']==469:
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
                        ''',row_delta['OBJID'])  
                    elif row_delta['TYPEID']==3716:
                        #SP4639 агент
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
                        ''',row_delta['OBJID'])  
                    
                    logging.info('Выборка расходов заголовки завершена')
                    rows_header = cursor.fetchall()

                    for row in rows_header:
                        client_list=[]
                        #list_partii=[]
                        isclosed=row['closed'] and 1
                        if isclosed!=1:
                            #continue
                            pass

                        if row['firma']!='9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ':
                            continue
                        if row['idartmarket']==None or row['idartmarket'].strip()=='':
                            if isclosed==1:
                                logging.error(';'.join(['Пустой ид',row_header['docno']]))
                            continue

                        if row['sklad']==None or row['sklad'].strip()=='':
                            if isclosed==1:
                                logging.error(';'.join(['Пустой склад',row['docno']]))
                            continue

                        if row['client']==None or row['client'].strip()=='':
                            if isclosed==1:
                                logging.error(';'.join(['Пустой клиент',row['docno']]))
                            continue

                        if not "'"+row['client']+"'" in client_list:
                            client_list.append("'"+row['client']+"'")
                        if client_list==[]:
                            continue


                        str_id=",".join(client_list)
                        hdb.get_client_groups(cursor,str_id)
                        header=header_type(document_type=2,firma=row['firma'].strip(),sklad=row['sklad'].strip(),client=row['client'].strip(),idartmarket=row['idartmarket'].strip()
                                            ,document_date=row['datedoc'],nomerartmarket=row['docno'])
                        logging.info('Выборка строк расхода')
                        if row_delta['TYPEID']==410:
                            cursor.execute('''
               			            select  sp4802 as idtovar,SP424 as kolvo,SP427 as koef,SP426 as price,SP428 as sum from dt410 WITH (NOLOCK) left join sc33 WITH (NOLOCK) on SP423=sc33.id where iddoc=%s
                            ''',row['iddoc'])
                        elif row_delta['TYPEID']==469:
                            cursor.execute('''
               			            select  sp4802 as idtovar,SP483 as kolvo,SP486 as koef,SP485 as price,SP487 as sum from dt469 WITH (NOLOCK) left join sc33 WITH (NOLOCK) on SP482=sc33.id where iddoc=%s
                            ''',row['iddoc'])
                        elif row_delta['TYPEID']==3716:
                            cursor.execute('''
               			            select  sp4802 as idtovar,SP3731 as kolvo,SP3734 as koef,SP3733 as price,SP3735 as sum from dt3716 WITH (NOLOCK) left join sc33 WITH (NOLOCK) on SP3730=sc33.id where iddoc=%s
                            ''',row['iddoc'])
                        logging.info('Выборка строк расхода завершена')

                        rows_table = cursor.fetchall()
                        row_list=[]
                        tovar_list=[]

                        for row_table in rows_table:
                            row_nom=row_type(tovar=row_table['idtovar'],quantity=row_table['kolvo'],price=row_table['price'],koef=row_table['koef'],sum=row_table['sum'])
                            if row_table['idtovar']==None:
                                continue
                            if not "'"+row_table['idtovar']+"'" in tovar_list:
                                tovar_list.append("'"+row_table['idtovar']+"'")
                            row_list.append(row_nom)

                        rows=rows_type(rows=row_list)
                        str_id=",".join(tovar_list)
                        nomenklatura.load_nomenklatura(prm_id_str=str_id,prm_id_mode=2,prm_with_parent=0,prm_update_mode=1)

                        document=document_type(header=header,rowslist=rows)
                        logging.info(';'.join(['Загрузка документа расхода',row['docno']]))

                        #hdb_agent=
                        if row['agent']!='' and row['agent']!=None:
                            nom_agent=hdb_type(name=row['agentname'].strip(),id=row['agent'].strip(),idparent='')
                            hdb_array=hdb_array_type(hdb_array=[nom_agent])
                            logging.info('Загрузка агента начало')
                            client.service.load_hdb_elements(hdb_array,1,'agent')
                            logging.info('Загрузка агента завершена')

                        if row['expeditor']!='' and row['expeditor']!=None:
                            nom_agent=hdb_type(name=row['expeditorname'].strip(),id=row['expeditor'].strip(),idparent='')
                            hdb_array=hdb_array_type(hdb_array=[nom_agent])
                            logging.info('Загрузка экспедитора начало')
                            client.service.load_hdb_elements(hdb_array,1,'agent')
                            logging.info('Загрузка экспедитора завершена')

                        list_partii=[]
                        if isclosed==1:
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
                            ''',row['iddoc'])
                            logging.info('Выборка партий расхода завершена')
                            rows_table_partii = cursor.fetchall()
                            for row_partii in rows_table_partii:
                                row_nom_partii=row_partii_type(tovar=row_partii['idtovar_artmarket'],
                                                               prihod_id=row_partii['prihodid'],
                                                               prihod_type=row_partii['prihodtype'],
                                                               prihod_no=row_partii['prihodno'],
                                                               prihod_date=row_partii['prihoddate'],
                                                               ostatok=row_partii['ostatok'],
                                                               stoimost=row_partii['stoimost'],
                                                               prodstoimost=row_partii['prodstoimost'],
                                                               prodaga=row_partii['prodaga'])
                                list_partii.append(row_nom_partii)
                        document_partii_rows=rows_partii_type(rows=list_partii)
                        document_partii=document_partii_type(rowslist=document_partii_rows)
                        n=client.service.load_rashod_tovar(document,document_partii,isclosed,row['agent'],row['expeditor'])
                        logging.info(';'.join(['Загрузка документа расхода',row['docno'],n]))
          

                #приходы
                elif row_delta['TYPEID']==434:
                    #приходы
                    prihod.load_prihod(row_delta)
                try:
                    cursor.execute('''delete from _1SUPDTS where (DBSIGN = 'P1 ') and (DWNLDID='1122!!') and (OBJID=%s)''',row_delta['OBJID'])  
                    conn.commit()
                    logging.info(';'.join(['Загружен объект',str(row_delta['OBJID']),str(row_delta['TYPEID'])]))
                except:
                    logging.error(';'.join(['Ошибка загрузки объекта',str(row_delta['OBJID']),str(row_delta['TYPEID'])]))

            logging.warning('Выборка изменений завершена')
            time.sleep(10) 

    elif k=='фирма':
    #загрузка  фирм
        firm_list=[]
        logging.info('Выборка фирм')
        cursor.execute('''SELECT  descr,sp4805 as idartmarket FROM SC13 where (sp4805 = '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ')''')  
        logging.info('Выборка фирм завершена')
        logging.info('Подготовка загрузки фирм')
        row = cursor.fetchall()  
        for r in row:
            if r[1].strip()=='':
                continue
            nom=hdb_type(name=r[0].strip(),id=r[1].strip(),idparent='')
            firm_list.append(nom)

        hdb_array=hdb_array_type(hdb_array=firm_list)
        logging.info('Загрузка фирм начало')
        client.service.load_hdb_elements(hdb_array,1,'firma')
        logging.info('Загрузка фирм завершена')
    elif k=='склад':
    #загрузка  складов
        sklad_list=[]
        hdb_type = client.get_type('ns3:hdb_element')
        hdb_array_type = client.get_type('ns3:hdb_array_element')
        logging.info('Выборка складов')
        cursor.execute('''SELECT  descr,sp5639 as idartmarket FROM SC31 ''')  
        logging.info('Выборка складов завершена')
        logging.info('Подготовка загрузки складов')
        row = cursor.fetchall()  
        for r in row:
            if r['idartmarket'].strip()=='':
                logging.error(';'.join(['Пустой ид склада',r['descr']]))
                continue
            nom=hdb_type(name=r['descr'].strip(),id=r['idartmarket'].strip(),idparent='')
            sklad_list.append(nom)

        hdb_array=hdb_array_type(hdb_array=sklad_list)
        logging.info('Загрузка складов начало')
        client.service.load_hdb_elements(hdb_array,0,'sklad')
        logging.info('Загрузка складов завершена')
    elif k=='клиент':
        #client_type = client.get_type('ns1:client_group')
        #array_client_groups=client.get_type('ns1:array_client_groups')

        get_client_groups(cursor)
    elif k=='регион':
        get_region_groups(cursor)

    elif k=='остаткипоставщик':
        logging.info('Выборка фирм')
        cursor.execute('''SELECT  descr,sp4805 as idartmarket,id FROM SC13 where (sp4805 = '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ')''')  
        logging.info('Выборка фирм завершена')
        rows_firma = cursor.fetchall() 
        for row_firma in rows_firma:
            if row_firma['idartmarket'].strip()!='':

                logging.info('Выборка остатков начало')
                cursor.execute('''
                
                   SELECT  sc46.sp4807 as client,sum(SP936) as ostatok,SP6065  from RG933
                        left join sc46 on ltrim(rtrim(SP934)) = ltrim(rtrim(sc46.id))
                        where (period='2018-12-01 00:00:00.000')  and  (SP2669=%s) group by sc46.sp4807,SP6065
                        
                        ''',row_firma['id'])  
                #+ нам должны, - мы должны
                logging.info('Запрос остатков выполнен')
                rows = cursor.fetchall()  
                logging.info('Курсор получен')
                header=header_type(document_type=2,firma=row_firma['idartmarket'].strip(),sklad='')
                row_list_dolg_post=[]
                row_list_dolg=[]
                row_list_avans=[]
                row_list_avans_post=[]
                client_list=[]
                for row in rows:
                    if row['client']==None:
                        continue
                    if row['SP6065']==1:
                        if  row['ostatok']<0:
                            row_nom=row_type(tovar=row['client'],quantity=-1*row['ostatok'],price=0,koef=0,sum=0)
                            row_list_avans.append(row_nom)
                        elif  row['ostatok']>0:
                            row_nom=row_type(tovar=row['client'],quantity=row['ostatok'],price=0,koef=0,sum=0)
                            row_list_dolg.append(row_nom)
                    elif row['SP6065']==2:
                        if  row['ostatok']>0:
                            row_nom=row_type(tovar=row['client'],quantity=row['ostatok'],price=0,koef=0,sum=0)
                            row_list_dolg_post.append(row_nom)
                        elif  row['ostatok']<0:
                            row_nom=row_type(tovar=row['client'],quantity=-1*row['ostatok'],price=0,koef=0,sum=0)
                            row_list_avans_post.append(row_nom)
                    if not "'"+row['client']+"'" in client_list:
                        client_list.append("'"+row['client']+"'")
                if client_list==[]:
                    continue
                str_id=",".join(client_list)
                #get_client_groups(cursor,str_id)
                #print(row_list)
                rows=rows_type(rows=row_list_dolg)
                document=document_type(header=header,rowslist=rows)
                rows=rows_type(rows=row_list_avans)
                document2=document_type(header=header,rowslist=rows)
                rows=rows_type(rows=row_list_dolg_post)
                document3=document_type(header=header,rowslist=rows)
                rows=rows_type(rows=row_list_avans_post)
                document4=document_type(header=header,rowslist=rows)
                logging.info('Загрузка документа остатков')
                #prmmode    1-долги клиентов (sc46.SP6065=1) row['ostatok']>0
                #           2 - авнсы клиентов (sc46.SP6065=1) row['ostatok']<0
                #           3- долги поставщикам
                #           4 - авансы поставщиков
                #n=client.service.load_ostatki_client(document,2)
                
                #проверено
                #n=client.service.load_ostatki_client(document2,3) 00-00000215
                #n=client.service.load_ostatki_client(document4,3) 1 00-00000216
                #n=client.service.load_ostatki_client(document3,4) 00-00000221

                logging.info('Загрузка документа остатков завершена')
    elif k=='остаткиклиент':
        #SP6065 тип клиента 1 покупатель 2 поставщик
        logging.info('Выборка фирм')
        cursor.execute('''SELECT  descr,sp4805 as idartmarket,id FROM SC13 where (sp4805 = '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ')''')  
        logging.info('Выборка фирм завершена')
        rows_firma = cursor.fetchall() 
        for row_firma in rows_firma:
            if row_firma['idartmarket'].strip()!='':

                logging.info('Выборка остатков начало')
                cursor.execute('''
                
                        SELECT  sc46.sp4807 as client,sum(SP171) as ostatok,SP6065  from RG169
                        left join sc46 on ltrim(rtrim(SP170)) = '1A   '+ltrim(rtrim(sc46.id))
                        where (period='2018-12-01 00:00:00.000') and  (SP2671=%s)  group by sc46.sp4807,SP6065
                        
                        ''',row_firma['id'])  
                #+ нам должны, - мы должны
               #  and (sc46.SP6065=1)
                logging.info('Запрос остатков выполнен')
                rows = cursor.fetchall()  
                logging.info('Курсор получен')
                header=header_type(document_type=1,firma=row_firma['idartmarket'].strip(),sklad='')
                row_list_dolg_post=[]
                row_list_dolg=[]
                row_list_avans=[]
                row_list_avans_post=[]
                client_list=[]
                for row in rows:
                    if row['client']==None:
                        continue
                    if row['SP6065']==1:
                        if  row['ostatok']<0:
                            row_nom=row_type(tovar=row['client'],quantity=-1*row['ostatok'],price=0,koef=0,sum=0)
                            row_list_avans.append(row_nom)
                        elif  row['ostatok']>0:
                            row_nom=row_type(tovar=row['client'],quantity=row['ostatok'],price=0,koef=0,sum=0)
                            row_list_dolg.append(row_nom)
                    elif row['SP6065']==2:
                        if  row['ostatok']>0:
                            row_nom=row_type(tovar=row['client'],quantity=row['ostatok'],price=0,koef=0,sum=0)
                            row_list_dolg_post.append(row_nom)
                        elif  row['ostatok']<0:
                            row_nom=row_type(tovar=row['client'],quantity=-1*row['ostatok'],price=0,koef=0,sum=0)
                            row_list_avans_post.append(row_nom)
                    if not "'"+row['client']+"'" in client_list:
                        client_list.append("'"+row['client']+"'")
                if client_list==[]:
                    continue
                str_id=",".join(client_list)
                #get_client_groups(cursor,str_id)
                #print(row_list)
                rows=rows_type(rows=row_list_dolg)
                document=document_type(header=header,rowslist=rows)
                rows=rows_type(rows=row_list_avans)
                document2=document_type(header=header,rowslist=rows)
                rows=rows_type(rows=row_list_dolg_post)
                document3=document_type(header=header,rowslist=rows)
                rows=rows_type(rows=row_list_avans_post)
                document4=document_type(header=header,rowslist=rows)
                logging.info('Загрузка документа остатков')
                #prmmode    1-долги клиентов (sc46.SP6065=1) row['ostatok']>0
                #           2 - авнсы клиентов (sc46.SP6065=1) row['ostatok']<0
                #           3- долги поставщикам
                #           4 - авансы поставщиков
                #n=client.service.load_ostatki_client(document3,3)
                
                #проверено
                #n=client.service.load_ostatki_client(document,1) 00-00000211
                #n=client.service.load_ostatki_client(document4,3) 00-00000222
                #n=client.service.load_ostatki_client(document2,3) 00-00000223
                
                logging.info('Загрузка документа остатков завершена')


    elif k=='остаткисклад':
    #загрузка остатков товаров
    #rg99 остатки товаров sp3603 - фирма ("     1   " - артмаркет?); sp101 - товар; sp100 - склад ('    12БЛ '); SP5183 - дата розлива; SP102 - количество
        #фирма sc13 АРТмаркет sp4805 - "9CD36F19-B8BD-49BC-BED4-A3335D2175C2    "; id - "     1   "
        logging.info('Выборка фирм')
        cursor.execute('''SELECT  descr,sp4805 as idartmarket FROM SC13 where (sp4805 = '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ')''')  
        logging.info('Выборка фирм завершена')
        rows_firma = cursor.fetchall()  
        for row_firma in rows_firma:
            if row_firma['idartmarket'].strip()=='':
                continue
            logging.info('Выборка складов')
            cursor.execute('''SELECT  id,SP5639 as idartmarket FROM SC31 ''')  
            logging.info('Выборка складов завершена')
            rows_sklad = cursor.fetchall()  
            for row_sklad in rows_sklad:
                if row_sklad['idartmarket'].strip()=='':
                    continue
                logging.info('Выборка остатков начало')
                cursor.execute('''SELECT sc33.sp4802 as idtovar,sum(sp102) as ostatok,sp6055 as sebestoimost 
                                from rg99 left join sc33 on rg99.sp101=sc33.id where (period='2018-12-01 00:00:00.000') and  (sp100=%s) and (sp3603='     1   ') group by sc33.sp4802,sp6055''',row_sklad['id'])  
                #
                logging.info('Запрос остатков выполнен')
                row = cursor.fetchall()  
                logging.info('Курсор получен')
                header=header_type(document_type=1,firma=row_firma['idartmarket'].strip(),sklad=row_sklad['idartmarket'].strip())
                row_list=[]
                tovar_list=[]
                for r in row:
                    if r['ostatok']<=0:
                        continue
                    row=row_type(tovar=r['idtovar'],quantity=r['ostatok'],price=r['sebestoimost'])
                    if not "'"+r['idtovar']+"'" in tovar_list:
                        tovar_list.append("'"+r['idtovar']+"'")
                    row_list.append(row)
                if tovar_list==[]:
                    continue
                rows=rows_type(rows=row_list)
                str_id=",".join(tovar_list)
                
                nomenklatura.load_nomenklatura(str_id,prm_id_mode=2,prm_with_parent=0,prm_update_mode=0)
                
                document=document_type(header=header,rowslist=rows)
                logging.info('Загрузка документа остатков')
                n=client.service.load_ostatki_tovar(document)
                logging.info('Загрузка документа остатков завершена')
            #print(n)
    elif k=='dump':
        client.wsdl.dump()

#except KeyboardInterrupt:
#        logging.warning('Выполнение автообмена прекращено пользователем')

        

        
conn.close()
logging.info('Конец работы')



#exit()


#base_code='M1 '
#cursor.execute('SELECT * from _1SUPDTS where dbsign=%d;',base_code)  
#'M1 '

#row = cursor.fetchone()  
#while row:  
#  print(row)
#  row = cursor.fetchone()  

#conn.close()

#Приходная накладная
# Name                  |Descr               |Type|Length|Precision
#F=IDDOC                 |ID Document's       |C   |9     |0        
#F=SP437                 |(P)Клиент           |C   |9     |0        
#F=SP1005                |(P)Фирма            |C   |9     |0        
#F=SP436                 |(P)Склад            |C   |9     |0        
#F=SP446                 |(P)ПризнакНакладной |C   |9     |0  '    3L   ' приход, '    3J   ' возврат      
#F=SP439                 |(P)Валюта           |C   |9     |0        
#F=SP440                 |(P)Дата_курса       |D   |0     |0        
#F=SP441                 |(P)Курс             |N   |9     |4        
#F=SP908                 |(P)Глубина          |N   |3     |0        
#F=SP910                 |(P)ДатаОплаты       |D   |0     |0        
#F=SP2698                |(P)ДокументОснование|C   |13    |0        
#F=SP4172                |(P)ЗатратыНаши      |N   |13    |2        
#F=SP4176                |(P)ЗатратыПоставщика|N   |13    |2        
#F=SP4173                |(P)НаЕдиницуТовара  |N   |1     |0        
#F=SP4259                |(P)НеСоздаватьПартию|N   |1     |0        
#F=SP5591                |(P)ВходящийДокументН|C   |50    |0        
#F=SP5592                |(P)ВходящийДокументД|D   |0     |0        
#F=SP5593                |(P)Перевозчик1Наимен|C   |100   |0        
#F=SP5594                |(P)Перевозчик1НомТС |C   |50    |0        
#F=SP5595                |(P)Перевозчик1ВидПер|N   |1     |0        
#F=SP5596                |(P)Перевозчик2Наимен|C   |100   |0        
#F=SP5597                |(P)Перевозчик2НомТС |C   |50    |0        
#F=SP5598                |(P)Перевозчик2ВидПер|N   |1     |0        
#F=SP5676                |(P)ПланироватьОплату|N   |1     |0        
#F=SP5927                |(P)GUID1C8          |C   |40    |0        
#F=SP5998                |(P)НакладнаяЕГАИС   |C   |9     |0        
#F=SP6054                |(P)флАктРасхождения |N   |1     |0        
#F=SP453                 |(P)Сумма            |N   |16    |2        
#F=SP605                 |(P)НДС              |N   |16    |2        
#F=SP3664                |(P)СуммаНП          |N   |16    |2        
#F=SP4175                |(P)КолЕдиниц        |N   |14    |0        
#F=SP1006                |(P)Автор            |C   |9     |0        
#F=SP1008                |(P)Основание        |C   |64    |0        
#F=SP1151                |(P)ТипУчета         |N   |1     |0        
#F=SP5005                |(P)Пароль           |C   |10    |0        


#журнал


# проведен and (Closed and 1 = 1) первый бит



#WITH (UPDLOCK)

