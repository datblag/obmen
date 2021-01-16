# -*- encoding: utf-8 -*-
import logs
import time
from datetime import date
from calendar import monthrange

import nomenklatura
import hdb
import kassa
import prihod, rashod, sklad, dolgi
import ostatki
from utils import convert_base


from tqdm import *
from config import cb_config, logname, logname_debug, logname_error
from wsdl import *
from sql import SqlClient
from doc_control import check_rashod
from os import path, remove


def load_chicago():
    if path.exists(r'\\new8srv\zakaz\flg.flg'):
        answ = wsdl_client.client.service.unload_client_orders()
        logging.info(answ)
        file = open(r'\\new8srv\zakaz\flg.txt', "w")
        file.write(answ)
        file.close()
        remove(r'\\new8srv\zakaz\flg.flg')


def auto_load(prm_cursor):
    white_list = []
    load_all = 0
    if load_all == 1:
        white_list.append(3716)  # расходнаядоставка
        white_list.append(410)  # расходнаянакладная
        white_list.append(469)  # расходнаяреализатора

        white_list.append(297)  # списания


        white_list.append(310)  # ввод остатков v_alko++
        white_list.append(434)  # приход v_alko++
        white_list.append(239)  # перемещение
        white_list.append(4425)  # заказ поставщику
        white_list.append(4553)  # счет на услуги service invoices

        white_list.append(2989)  # движенияденежныхсредств
        white_list.append(4308)  # выручкадоставка  sp4323 переброска
        white_list.append(2964)  # ПриходныйОрдерТБ
        white_list.append(4179)  # АктПереоценкиКлиенты
        white_list.append(4225)  # РасходныйОрдерТБ

        white_list.append(4114)  # приходный ордер Б
        white_list.append(4132)  # расходный ордер Б

        white_list.append(33)  # номенклатура
        white_list.append(46)  # контрагенты

        white_list.append(5468)  # производители импортеры
        white_list.append(5196)  # даты розлива
        white_list.append(4840)  # клиенты агента
        white_list.append(4843)  # ассортимент агента

        white_list.append(3769)  # подразделения

        white_list.append(3773)  # статьи затрат

        white_list.append(5552)  # источник финансирования SP6128
        white_list.append(5554)  # для маркетинга SP6129

        white_list.append(5510)  # аналитики
        white_list.append(5584)  # доверенности
        white_list.append(5529)  # транспортное средство
        white_list.append(1183)  # счета контрагентов банк
        white_list.append(5494)  # скидки клиентам

    if load_all == 0:
        pass
        white_list.append(434)

    commit_limit = 100
    commit_count = 0

    while True:
        logging.warning('Выборка изменений')
        # коды баз 'P1 ','БП '
        # типы объектов: номенклатура - 33,
        try:
            prm_cursor.execute(
                '''update  _1SUPDTS set DWNLDID='1122!!' where (DBSIGN = 'P1 ') and not (DWNLDID='1122!!')''')
            conn.commit()
        except Exception as e:
            logging.error('ошибка при обновлении таблицы _1SUPDTS')
            logging.error(e)
        #    pass
        prm_cursor.execute('''SELECT  top 100000 * from _1SUPDTS WITH (NOLOCK) where (DBSIGN = 'P1 ') and 
                            (DWNLDID='1122!!') ''')
        rows_delta = prm_cursor.fetchall()
        for row_delta in tqdm(rows_delta):
            try:
                load_chicago()
            except:
                pass
            # logging.info(['typeid', row_delta['TYPEID']])
            if not (row_delta['TYPEID'] in white_list):
                logging.info(['TYPEID', row_delta['TYPEID']])
                if load_all == 1:
                    try:
                        prm_cursor.execute('''delete from _1SUPDTS where (DBSIGN = 'P1 ') and (DWNLDID='1122!!') and 
                        (TYPEID=%s)''', row_delta['TYPEID'])
                        # prm_cursor.execute('''delete from _1SUPDTS where (DBSIGN = 'P1 ') and (DWNLDID='1122!!') and
                        # (OBJID=%s) and (TYPEID=%s)''', (row_delta['OBJID'], row_delta['TYPEID']))
                        conn.commit()
                        logging.info(';'.join(['Удалены изменения объект', str(row_delta['OBJID']),
                                               str(row_delta['TYPEID'])]))
                        break
                    except:
                        logging.error(';'.join(['Ошибка отмены изменений объекта', str(row_delta['OBJID']),
                                                str(row_delta['TYPEID'])]))
                continue

            logging.info(row_delta['TYPEID'])
            # номенклатура
            if row_delta['TYPEID'] == 33:
                str_id = ",".join(["'" + row_delta['OBJID'] + "'"])
                nomenklatura.load_nomenklatura(prm_cursor, prm_id_str=str_id, prm_id_mode=1, prm_with_parent=0,
                                               prm_update_mode=1, wsdl_client=wsdl_client)

            # скидки клиентам
            elif row_delta['TYPEID'] == 5494:
                logging.info(row_delta['TYPEID'])
                hdb.unload_customer_discounts(prm_cursor, wsdl_client, row_delta['OBJID'])

            # счета контрагентов
            elif row_delta['TYPEID'] == 1183:
                logging.info(row_delta['TYPEID'])
                hdb.unload_bank_accounts(prm_cursor, wsdl_client.client, row_delta['OBJID'])

            # транспорт
            elif row_delta['TYPEID'] == 5529:
                logging.info(row_delta['TYPEID'])
                hdb.unload_transport(prm_cursor, wsdl_client.client, row_delta['OBJID'])

            # доверенности
            elif row_delta['TYPEID'] == 5584:
                logging.info(row_delta['TYPEID'])
                hdb.unload_attorney(prm_cursor, wsdl_client.client, row_delta['OBJID'])
            # аналитика
            elif row_delta['TYPEID'] == 5510:
                logging.info(row_delta['TYPEID'])
                hdb.unload_analytics(prm_cursor, wsdl_client.client, row_delta['OBJID'])
            # статьи затрат
            elif row_delta['TYPEID'] == 3773:
                logging.info(row_delta['TYPEID'])
                hdb.unload_cost(prm_cursor, wsdl_client.client, row_delta['OBJID'])

            # для маркетинга
            elif row_delta['TYPEID'] == 5554:
                logging.info(row_delta['TYPEID'])
                hdb.unload_for_marketing(prm_cursor, wsdl_client.client, row_delta['OBJID'])

            # источник финансирования
            elif row_delta['TYPEID'] == 5552:
                logging.info(row_delta['TYPEID'])
                hdb.unload_financing(prm_cursor, wsdl_client.client, row_delta['OBJID'])

            # подразделение
            elif row_delta['TYPEID'] == 3769:
                logging.info(row_delta['TYPEID'])
                hdb.unload_unit(prm_cursor, wsdl_client.client, row_delta['OBJID'])

            # производитель импортер
            elif row_delta['TYPEID'] == 5468:
                hdb.unload_maker(prm_cursor, wsdl_client.client, row_delta['OBJID'])

            # контрагенты
            elif row_delta['TYPEID'] == 46:

                # client_list.append("'" + row_delta['OBJID'] + "'")
                str_id = ",".join(["'" + row_delta['OBJID'] + "'"])

                hdb.get_client_groups(wsdl_client, cursor, prm_id_list=str_id, prm_parent_id_list=None,
                                      prm_id_list_mode=1)

            # ассортимент агента
            elif row_delta['TYPEID'] == 4843:
                hdb.unload_agent_products(prm_cursor, wsdl_client, row_delta['OBJID'])

            # клиенты агента
            elif row_delta['TYPEID'] == 4840:
                hdb.unload_agent_clients(prm_cursor, wsdl_client, row_delta['OBJID'])

            # даты розлива
            elif row_delta['TYPEID'] == 5196:
                hdb.unload_production_date(prm_cursor, wsdl_client.client, row_delta['OBJID'])
            # приходный ордер Б
            elif row_delta['TYPEID'] == 4114:
                kassa.prihod(prm_cursor, wsdl_client, row_delta)
                # continue

            # расходный ордер Б
            elif row_delta['TYPEID'] == 4132:
                kassa.rashod(prm_cursor, wsdl_client, row_delta)
                # continue

            # перемещение
            elif row_delta['TYPEID'] == 239:
                sklad.move_tovar(prm_cursor, wsdl_client, row_delta)

            # заказ поставщику
            elif row_delta['TYPEID'] == 4425:
                dolgi.load_order_supplier(prm_cursor, wsdl_client, row_delta)

            # счет на услуги
            elif row_delta['TYPEID'] == 4553:
                dolgi.load_service_invoices(prm_cursor, wsdl_client, row_delta)

            # списание
            elif row_delta['TYPEID'] == 297:
                sklad.spisanie(prm_cursor, wsdl_client, row_delta)

            # оприходование
            elif row_delta['TYPEID'] == 310:
                sklad.vvodostatka_tovar(prm_cursor, wsdl_client, row_delta)

            # взаиморасчеты
            elif row_delta['TYPEID'] in [2989, 4308, 2964, 4179, 4225]:  # взаиморасчеты
                dolgi.load_dolgi(prm_cursor, wsdl_client, row_delta)

            # расход
            elif row_delta['TYPEID'] in [410, 469, 3716]:
                rashod.load_rashod(prm_cursor, wsdl_client, row_delta)

            # приходы
            elif row_delta['TYPEID'] == 434:
                # приходы
                prihod.load_prihod(prm_cursor, wsdl_client, row_delta)
            try:

                prm_cursor.execute('''delete from _1SUPDTS where (DBSIGN = 'P1 ') and (DWNLDID='1122!!')
                                    and (OBJID=%s) and (TYPEID=%s)''', (row_delta['OBJID'], row_delta['TYPEID']))
                commit_count += 1
                if commit_count == commit_limit:
                    conn.commit()
                    commit_count = 0
                    logging.warning('commit')
                logging.info(';'.join(['Загружен объект', str(row_delta['OBJID']), str(row_delta['TYPEID'])]))
            except Exception as e:
                logging.error(';'.join(['Ошибка загрузки объекта', str(row_delta['OBJID']),
                                        str(row_delta['TYPEID'])]))
                logging.error(e)

        logging.warning('Выборка изменений завершена')
        time.sleep(10)


    #exit()


logs.run(logname, logname_debug, logname_error)

wsdl_client = WsdlClient(cb_config['server_1c'])

client = wsdl_client.client

sql_client = SqlClient(cb_config['sql'])
cursor = sql_client.cursor
conn = sql_client.conn


def main():
    logging.warning('Начало работы')

    logging.warning(convert_base('6053', to_base=36))

    # client.wsdl.dump()

    # list = []

    # os.environ['TDSDUMP'] = 'stdout'
    # try:
    while True:
        k = input('Введите команду:')
        if k == '0':
            break
        elif k == 'ч':
            load_chicago()

        elif k == 'контроль':
            check_rashod(cursor, wsdl_client)
        elif k == 'ценынач':
            nomenklatura.load_nomenklatura(cursor, prm_id_str='', prm_id_mode=1, prm_with_parent=0, prm_update_mode=1,
                                           prm_unload_price=3678, prm_unload_price_date='2018-12-31',
                                           wsdl_client=wsdl_client)
        elif k == 'цены':
            start_date_0 = date(2020, 11, 20)
            end_date = date(2020, 12, 1)
            nomenklatura.unload_price(wsdl_client, cursor, start_date_0, end_date)

        elif k == 'штрихкода':
            nomenklatura.unload_ean_codes(cursor=cursor, wsdl_client=wsdl_client.client)

        elif k == 'док':
            month_num = 12
            year_num = 2020
            start_date = date(year_num, month_num, 1)
            end_date = date(year_num, month_num, monthrange(year_num, month_num)[1])
            dolgi.load_docs(cursor, wsdl_client, start_date, end_date)
        elif k == 'авто' or k == 'avto':
            auto_load(cursor)
        elif k == 'фирма':
            hdb.load_firm(client, cursor)
        elif k == 'агентгруппы':
            hdb.unload_agent_groups(client, cursor)
        elif k == 'агент':
            hdb.unload_agent(client, cursor)
        elif k == 'минцены':
            nomenklatura.unload_wholesale_min_price(cursor=cursor, wsdl_client=wsdl_client.client)
        elif k == 'склад':
            hdb.load_storage(client, cursor)
        elif k == 'клиент':
            hdb.get_client_groups(wsdl_client, cursor)
        elif k == 'клиентструк':
            hdb.load_client_structure(cursor, wsdl_client)

        elif k == 'регион':
            hdb.get_region_groups(cursor, wsdl_client=wsdl_client)

        elif k == 'остаткипоставщик':
            dolgi.load_supplier_balance(cursor, wsdl_client)
        elif k == 'остаткиклиент':
            dolgi.load_client_balance(cursor, wsdl_client)

        elif k == 'остаткисклад':
            ostatki.load_ostatki_sklad(wsdl_client, cursor)
        elif k == 'dump':
            client.wsdl.dump()

    conn.close()
    logging.info('Конец работы')

    # base_code='M1 '
    # cursor.execute('SELECT * from _1SUPDTS where dbsign=%d;',base_code)
    # 'M1 '

    # row = cursor.fetchone()
    # while row:
    #  print(row)
    #  row = cursor.fetchone()

    # conn.close()

    # Приходная накладная
    # Name                  |Descr               |Type|Length|Precision
    # F=IDDOC                 |ID Document's       |C   |9     |0
    # F=SP437                 |(P)Клиент           |C   |9     |0
    # F=SP1005                |(P)Фирма            |C   |9     |0
    # F=SP436                 |(P)Склад            |C   |9     |0
    # F=SP446                 |(P)ПризнакНакладной |C   |9     |0  '    3L   ' приход, '    3J   ' возврат
    # F=SP439                 |(P)Валюта           |C   |9     |0
    # F=SP440                 |(P)Дата_курса       |D   |0     |0
    # F=SP441                 |(P)Курс             |N   |9     |4
    # F=SP908                 |(P)Глубина          |N   |3     |0
    # F=SP910                 |(P)ДатаОплаты       |D   |0     |0
    # F=SP2698                |(P)ДокументОснование|C   |13    |0
    # F=SP4172                |(P)ЗатратыНаши      |N   |13    |2
    # F=SP4176                |(P)ЗатратыПоставщика|N   |13    |2
    # F=SP4173                |(P)НаЕдиницуТовара  |N   |1     |0
    # F=SP4259                |(P)НеСоздаватьПартию|N   |1     |0
    # F=SP5591                |(P)ВходящийДокументН|C   |50    |0
    # F=SP5592                |(P)ВходящийДокументД|D   |0     |0
    # F=SP5593                |(P)Перевозчик1Наимен|C   |100   |0
    # F=SP5594                |(P)Перевозчик1НомТС |C   |50    |0
    # F=SP5595                |(P)Перевозчик1ВидПер|N   |1     |0
    # F=SP5596                |(P)Перевозчик2Наимен|C   |100   |0
    # F=SP5597                |(P)Перевозчик2НомТС |C   |50    |0
    # F=SP5598                |(P)Перевозчик2ВидПер|N   |1     |0
    # F=SP5676                |(P)ПланироватьОплату|N   |1     |0
    # F=SP5927                |(P)GUID1C8          |C   |40    |0
    # F=SP5998                |(P)НакладнаяЕГАИС   |C   |9     |0
    # F=SP6054                |(P)флАктРасхождения |N   |1     |0
    # F=SP453                 |(P)Сумма            |N   |16    |2
    # F=SP605                 |(P)НДС              |N   |16    |2
    # F=SP3664                |(P)СуммаНП          |N   |16    |2
    # F=SP4175                |(P)КолЕдиниц        |N   |14    |0
    # F=SP1006                |(P)Автор            |C   |9     |0
    # F=SP1008                |(P)Основание        |C   |64    |0
    # F=SP1151                |(P)ТипУчета         |N   |1     |0
    # F=SP5005                |(P)Пароль           |C   |10    |0


    # журнал


    # проведен and (Closed and 1 = 1) первый бит


    # WITH (UPDLOCK)

if __name__ == '__main__':
    main()

