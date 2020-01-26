# -*- encoding: utf-8 -*-
import logs
from datetime import date, timedelta
import calendar
import time

import nomenklatura
import hdb
import prihod, rashod, sklad, dolgi
import ostatki
from utils import convert_base


from tqdm import *
from config import cb_config,logname, logname_debug, logname_error
from wsdl import *
from sql import SqlClient

logs.run(logname, logname_debug, logname_error)

wsdl_client = WsdlClient(cb_config['server_1c'])

client = wsdl_client.client

sql_client = SqlClient(cb_config['sql'])
cursor = sql_client.cursor
conn = sql_client.conn







logging.warning('Начало работы')



logging.warning(convert_base('BE', from_base=36))


#client.wsdl.dump()

# list = []

# os.environ['TDSDUMP'] = 'stdout'
# try:
while True:
    k=input('Введите команду:')
    if k == '0':
        break
    elif k == 'ценынач':
        nomenklatura.load_nomenklatura(cursor, prm_id_str='', prm_id_mode=1, prm_with_parent=0, prm_update_mode=1,
                                       prm_unload_price=3678, prm_unload_price_date='2018-12-31',
                                       wsdl_client=wsdl_client)
    elif k == 'цены':
        # 36-доставка (загружены за 2019й год), 38-приобретение (загружены за 2019й год),
        # (4549 закуп, 35 киоск, 4460 - индивидуальная, 3677 - для сетей (оптовая), 37 - розничная) -
        # загружены за 2019й год


        # выгрузка истории
        # TODO филиал - 4340, херека (специальная) - 3678 , самбери (акцизного склада) - 4613
        start_date_0 = date(2019, 1, 1)
        end_date = date(2019, 12, 31)
        delta = timedelta(days=1)

        # [36, 38, 4549, 35, 4460, 3677, 37, 4340, 3678, 4613] полный список
        price_type_to_load = [36, 38, 4549, 35, 4460, 3677, 37, 4340, 3678, 4613]
        for price_type in price_type_to_load:
            logging.warning(price_type)
            start_date = start_date_0
            while start_date <= end_date:
                k2 = start_date.strftime("%Y-%m-%d")
                logging.warning(k2)
                start_date += delta
                nomenklatura.load_nomenklatura(cursor, prm_id_str='', prm_id_mode=1, prm_with_parent=0,
                                               prm_update_mode=1, prm_unload_price=price_type, prm_unload_price_date=k2,
                                               wsdl_client=wsdl_client)
    elif k == 'док':
        # 434 - приход
        doc_type_list = [{'typeid': 434, 'typename': 'приход', 'idfield': 'SP6059', 'sumfield': 'sp453'},
                         {'typeid': 3716, 'typename': 'доставка', 'idfield': 'SP6071', 'sumfield': 'SP3735'},
                         {'typeid': 410, 'typename': 'расход', 'idfield': 'SP6060', 'sumfield': 'SP428'},
                         {'typeid': 469, 'typename': 'реализатор', 'idfield': 'SP6072', 'sumfield': 'SP487'},
                         {'typeid': 297, 'typename': 'списание', 'idfield': 'SP6076', 'sumfield': 'SP4178'},
                         {'typeid': 310, 'typename': 'ввод. ост', 'idfield': 'SP6077', 'sumfield': 'SP4717'},
                         {'typeid': 239, 'typename': 'перемещ', 'idfield': 'SP6079', 'sumfield': 'SP250'}
                         ]
        for month_num in range(1, 12+1):
            monthrange = calendar.monthrange(2019, month_num)
            #print(monthrange, month_num)
            start_date = date(2019, month_num, 1)
            end_date = date(2019, month_num, monthrange[1])
            logging.warning([start_date, end_date])
            for doc_type in doc_type_list:
                list_partii = []
                # print(doc_type['sumfield'])
                cursor.execute('''select  _1sjourn.iddoc, docno as docno,
                CAST(LEFT(_1sjourn.Date_Time_IDDoc, 8) as DateTime) as docdate, 
                dt.sm,'''+doc_type['idfield']+''' as idartmarket  from _1sjourn
                left join dh'''+str(doc_type['typeid'])+''' dh on _1sjourn.iddoc = dh.iddoc
                left join(select sum('''+doc_type['sumfield']+''') as sm, iddoc from dt'''+str(doc_type['typeid']) +
                               ''' group by  iddoc) dt on _1sjourn.iddoc = dt.iddoc
                               where(iddocdef='''+str(doc_type['typeid'])+
                               ''') and (CAST(LEFT(_1sjourn.Date_Time_IDDoc, 8) as DateTime) between ''' +
                               "'" + start_date.strftime("%Y-%m-%d") + "'" + ''' and ''' +
                               "'" + end_date.strftime("%Y-%m-%d") + "'" + ''')''')
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

                n = wsdl_client.client.service.load_doc_list(doc_type['typeid'], start_date.strftime("%Y-%m-%d"), document_partii, 0, doc_type['typename'])

                logging.warning(['Загрузка списка документов'])
    elif k == 'авто':
        white_list = []
        if 1 == 0:
            white_list.append(3716)     # расходнаядоставка
            white_list.append(410)      # расходнаянакладная
            white_list.append(469)      # расходнаяреализатора

            white_list.append(33)       # номенклатура
        
            white_list.append(239)      # перемещение
            white_list.append(310)      # ввод остатков
            white_list.append(434)      # приход
            white_list.append(2989)     # движенияденежныхсредств
            white_list.append(4308)     # выручкадоставка
            white_list.append(2964)     # ПриходныйОрдерТБ
            white_list.append(4179)     # АктПереоценкиКлиенты
            white_list.append(4225)     # РасходныйОрдерТБ
            white_list.append(297)      # списания

        if 1 == 1:
            white_list.append(33)       # номенклатура

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
                # номенклатура
                if row_delta['TYPEID'] == 33:
                    str_id = ",".join(["'"+row_delta['OBJID']+"'"])
                    nomenklatura.load_nomenklatura(cursor, prm_id_str=str_id, prm_id_mode=1, prm_with_parent=0,
                                                   prm_update_mode=1, wsdl_client=wsdl_client)
                
                # перемещение
                elif row_delta['TYPEID'] == 239:
                    sklad.move_tovar(cursor, wsdl_client, row_delta)

                # списание
                elif row_delta['TYPEID'] == 297:
                    sklad.spisanie(cursor, wsdl_client, row_delta)

                # оприходование
                elif row_delta['TYPEID'] == 310:
                    sklad.vvodostatka_tovar(cursor, wsdl_client, row_delta)

                # взаиморасчеты
                elif row_delta['TYPEID'] in [2989, 4308, 2964, 4179, 4225]: #взаиморасчеты
                    dolgi.load_dolgi(cursor, wsdl_client, row_delta)

                #расход
                elif row_delta['TYPEID'] in [410, 469, 3716]:
                    rashod.load_rashod(cursor, wsdl_client, row_delta)

                #приходы
                elif row_delta['TYPEID'] == 434:
                    #приходы
                    prihod.load_prihod(cursor, wsdl_client, row_delta)
                try:
                    cursor.execute('''delete from _1SUPDTS where (DBSIGN = 'P1 ') and (DWNLDID='1122!!') 
                                    and (OBJID=%s) and (TYPEID=%s)''', (row_delta['OBJID'], row_delta['TYPEID']))
                    conn.commit()
                    logging.info(';'.join(['Загружен объект', str(row_delta['OBJID']), str(row_delta['TYPEID'])]))
                except:
                    logging.error(';'.join(['Ошибка загрузки объекта', str(row_delta['OBJID']),
                                            str(row_delta['TYPEID'])]))

            logging.warning('Выборка изменений завершена')
            time.sleep(10) 

    elif k=='фирма':
    #загрузка  фирм
        firm_list=[]
        logging.info('Выборка фирм')
        cursor.execute('''SELECT  descr, sp4805 as idartmarket FROM SC13 where (sp4805 = '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ')''')
        logging.info('Выборка фирм завершена')
        logging.info('Подготовка загрузки фирм')
        row = cursor.fetchall()  
        for r in row:
            if r[1].strip()=='':
                continue
            nom = hdb_type(name=r[0].strip(),id=r[1].strip(),idparent='')
            firm_list.append(nom)

        hdb_array = hdb_array_type(hdb_array=firm_list)
        logging.info('Загрузка фирм начало')
        client.service.load_hdb_elements(hdb_array,1,'firma')
        logging.info('Загрузка фирм завершена')
    elif k == 'склад':
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
            if r['idartmarket'].strip() == '':
                logging.error(';'.join(['Пустой ид склада', r['descr']]))
                continue
            nom=hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent='')
            sklad_list.append(nom)

        hdb_array=hdb_array_type(hdb_array=sklad_list)
        logging.info('Загрузка складов начало')
        client.service.load_hdb_elements(hdb_array, 0, 'sklad')
        logging.info('Загрузка складов завершена')
    elif k == 'клиент':
        #client_type = client.get_type('ns1:client_group')
        #array_client_groups=client.get_type('ns1:array_client_groups')

        hdb.get_client_groups(wsdl_client, cursor)
    elif k == 'регион':
        get_region_groups(cursor)

    elif k == 'остаткипоставщик':
        logging.info('Выборка фирм')
        cursor.execute('''SELECT  descr,sp4805 as idartmarket,id FROM SC13 where (sp4805 = '9CD36F19-B8BD-49BC-BED4-A3335D2175C2    ')''')  
        logging.info('Выборка фирм завершена')
        rows_firma = cursor.fetchall() 
        for row_firma in rows_firma:
            if row_firma['idartmarket'].strip() != '':

                logging.info('Выборка остатков начало')
                cursor.execute('''
                
                   SELECT  sc46.sp4807 as client,sum(SP936) as ostatok,SP6065  from RG933
                        left join sc46 on ltrim(rtrim(SP934)) = ltrim(rtrim(sc46.id))
                        where (period='2018-12-01 00:00:00.000')  and  (SP2669=%s) group by sc46.sp4807,SP6065
                        
                        ''', row_firma['id'])
                #+ нам должны, - мы должны
                logging.info('Запрос остатков выполнен')
                rows = cursor.fetchall()  
                logging.info('Курсор получен')
                header=wsdl_client.header_type(document_type=2, firma=row_firma['idartmarket'].strip(), sklad='')
                row_list_dolg_post = []
                row_list_dolg = []
                row_list_avans = []
                row_list_avans_post = []
                client_list=[]
                for row in rows:
                    if row['client']==None:
                        continue
                    if row['SP6065'] == 1:
                        if row['ostatok'] < 0:
                            row_nom=wsdl_client.row_type(tovar=row['client'],quantity=-1*row['ostatok'],price=0,koef=0,sum=0)
                            row_list_avans.append(row_nom)
                        elif row['ostatok'] > 0:
                            row_nom=wsdl_client.row_type(tovar=row['client'],quantity=row['ostatok'],price=0,koef=0,sum=0)
                            row_list_dolg.append(row_nom)
                    elif row['SP6065'] == 2:
                        if row['ostatok'] > 0:
                            row_nom=wsdl_client.row_type(tovar=row['client'],quantity=row['ostatok'],price=0,koef=0,sum=0)
                            row_list_dolg_post.append(row_nom)
                        elif  row['ostatok'] < 0:
                            row_nom=wsdl_client.row_type(tovar=row['client'], quantity=-1*row['ostatok'],price=0,koef=0,sum=0)
                            row_list_avans_post.append(row_nom)
                    if not "'"+row['client']+"'" in client_list:
                        client_list.append("'"+row['client']+"'")
                if client_list==[]:
                    continue
                str_id=",".join(client_list)
                #get_client_groups(cursor,str_id)
                #print(row_list)
                rows=wsdl_client.rows_type(rows=row_list_dolg)
                document=wsdl_client.document_type(header=header,rowslist=rows)
                rows=wsdl_client.rows_type(rows=row_list_avans)
                document2=wsdl_client.document_type(header=header,rowslist=rows)
                rows=wsdl_client.rows_type(rows=row_list_dolg_post)
                document3=wsdl_client.document_type(header=header,rowslist=rows)
                rows=wsdl_client.rows_type(rows=row_list_avans_post)
                document4=wsdl_client.document_type(header=header,rowslist=rows)
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
                header=wsdl_client.header_type(document_type=1,firma=row_firma['idartmarket'].strip(),sklad='')
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
                            row_nom=wsdl_client.row_type(tovar=row['client'],quantity=-1*row['ostatok'],price=0,koef=0,sum=0)
                            row_list_avans.append(row_nom)
                        elif  row['ostatok']>0:
                            row_nom=wsdl_client.row_type(tovar=row['client'],quantity=row['ostatok'],price=0,koef=0,sum=0)
                            row_list_dolg.append(row_nom)
                    elif row['SP6065']==2:
                        if  row['ostatok']>0:
                            row_nom=wsdl_client.row_type(tovar=row['client'],quantity=row['ostatok'],price=0,koef=0,sum=0)
                            row_list_dolg_post.append(row_nom)
                        elif  row['ostatok']<0:
                            row_nom=wsdl_client.row_type(tovar=row['client'],quantity=-1*row['ostatok'],price=0,koef=0,sum=0)
                            row_list_avans_post.append(row_nom)
                    if not "'"+row['client']+"'" in client_list:
                        client_list.append("'"+row['client']+"'")
                if client_list==[]:
                    continue
                str_id=",".join(client_list)
                #get_client_groups(cursor,str_id)
                #print(row_list)
                rows=wsdl_client.rows_type(rows=row_list_dolg)
                document=wsdl_client.document_type(header=header,rowslist=rows)
                rows=wsdl_client.rows_type(rows=row_list_avans)
                document2=wsdl_client.document_type(header=header,rowslist=rows)
                rows=wsdl_client.rows_type(rows=row_list_dolg_post)
                document3=wsdl_client.document_type(header=header,rowslist=rows)
                rows=wsdl_client.rows_type(rows=row_list_avans_post)
                document4=wsdl_client.document_type(header=header,rowslist=rows)
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
        ostatki.load_ostatki_sklad(wsdl_client,cursor)
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

