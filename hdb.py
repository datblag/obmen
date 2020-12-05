import logging
from config import filial_region_id, filial_region_name


def load_firm(client, cursor):
    # загрузка  фирм
    hdb_type = client.get_type('ns3:hdb_element')
    hdb_array_type = client.get_type('ns3:hdb_array_element')
    firm_list = []
    logging.info('Выборка фирм')
    cursor.execute('''SELECT  descr, sp4805 as idartmarket, sp4805 as firma FROM SC13''')
    logging.info('Выборка фирм завершена')
    logging.info('Подготовка загрузки фирм')
    row = cursor.fetchall()
    for r in row:
        if not check_firma(r, 0):
            continue
        if r[1].strip() == '':
            continue
        nom = hdb_type(name=r[0].strip(), id=r[1].strip(), idparent='')
        firm_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=firm_list)
    logging.info('Загрузка фирм начало')
    client.service.load_hdb_elements(hdb_array, 1, 'firma')
    logging.info('Загрузка фирм завершена')


def load_client_structure(cursor, wsdl_client):
    cursor.execute('''select sp4807 as idartmarket, id, descr from
                               sc46 where isfolder=1 and code=30''')
    # 20080 артмаркет опт
    rows_root = cursor.fetchall()
    for row_root in rows_root:
        logging.warning(row_root)
        hdb.get_client_groups(wsdl_client, cursor, prm_parent_id_list=row_root)
        cursor.execute('''select sp4807 as idartmarket, id, descr from
                                   sc46 where isfolder=1 and parentid=%s''', row_root['id'])
        rows_level1 = cursor.fetchall()
        for row_level1 in rows_level1:
            logging.warning(row_level1)
            hdb.get_client_groups(wsdl_client, cursor, prm_parent_id_list=row_level1)
            cursor.execute('''select sp4807 as idartmarket, id, descr from
                                       sc46 where isfolder=1 and parentid=%s''', row_level1['id'])
            rows_level2 = cursor.fetchall()
            for row_level2 in rows_level2:
                logging.warning(row_level2)
                hdb.get_client_groups(wsdl_client, cursor, prm_parent_id_list=row_level2)


def load_storage(client, cursor):
    # загрузка  складов
    sklad_list = []
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
        nom = hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent='')
        sklad_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=sklad_list)
    logging.info('Загрузка складов начало')
    client.service.load_hdb_elements(hdb_array, 0, 'sklad')
    logging.info('Загрузка складов завершена')


def unload_agent(client, cursor):
    agent_list = []
    hdb_type = client.get_type('ns3:hdb_element')
    hdb_array_type = client.get_type('ns3:hdb_array_element')
    logging.info('Выборка агентов')
    cursor.execute('''SELECT  descr,SP4808 as idartmarket, parentid as parentid FROM SC3246 where isfolder = 2''')
    row = cursor.fetchall()
    logging.info('Выборка агентов завершена')
    logging.info('подготовка загрузки  агентов')
    for r in row:
        if r['idartmarket'].strip() == '':
            logging.error(';'.join(['Пустой ид агента', r['descr']]))
            continue
        nom = hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent=r['parentid'].strip())
        agent_list.append(nom)
    hdb_array = hdb_array_type(hdb_array=agent_list)
    logging.info('Загрузка агентов начало')
    client.service.load_hdb_elements(hdb_array, 1, 'agent')
    logging.info('Загрузка агентов завершена')


def unload_agent_groups(client, cursor):
    agent_list = []
    hdb_type = client.get_type('ns3:hdb_element')
    hdb_array_type = client.get_type('ns3:hdb_array_element')
    logging.info('Выборка агентов группы')
    cursor.execute('''SELECT  descr, id as idartmarket FROM SC3246 where isfolder = 1''')
    row = cursor.fetchall()
    logging.info('Выборка агентов группы завершена')
    logging.info('подготовка загрузки  агентов группы')
    for r in row:
        nom = hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent='')
        agent_list.append(nom)
    hdb_array = hdb_array_type(hdb_array=agent_list)
    logging.info('Загрузка агентов группы начало')
    client.service.load_hdb_elements(hdb_array, 0, 'agentgroup')
    logging.info('Загрузка агентов группы завершена')


def unload_agents(wsdl_client=None, agent_id='', agent_parent_id='', agent_name='', transport_id=None):

    transport = ''
    if transport_id is not None:
        transport = transport_id

    nom_agent = wsdl_client.hdb_type(name=agent_name.strip(), id=agent_id.strip(),
                                     idparent=agent_parent_id.strip(), value1=transport.strip())

    hdb_array = wsdl_client.hdb_array_type(hdb_array=[nom_agent])
    logging.info('Загрузка агента начало')
    wsdl_client.client.service.load_hdb_elements(hdb_array, 1, 'agent')
    logging.info('Загрузка агента завершена')


def unload_agent_clients(cursor=None, wsdl_client=None, objid=''):
    clients_list = []
    logging.info('Выборка клиенты агента')
    cursor.execute('''
                    select sc4840.id, sc4840.ismark,sp6124 as idartmarket,
                    spragent.SP4808 as agent,
                    spragent.descr as agentname,
                    spragent.parentid as agentparentid,
                    sprclient.descr as clientname,
                    sprclient.sp4807 as clientid,
                    sprclient.isfolder
                     from sc4840
                    left join SC3246  as spragent WITH (NOLOCK) on parentext = spragent.id
                    left join sc46 as sprclient WITH (NOLOCK) on sp4838 = sprclient.id
                where sc4840.id=%s
            ''', objid)
    logging.info('Выборка клиенты агента завершена')
    logging.info('Подготовка загрузки клиентов агента')
    row = cursor.fetchall()
    for r in row:
        logging.warning(r)
        if r['idartmarket'] is None or r['idartmarket'].strip() == '':
            logging.warning('Не задан идентификатор')
            continue
        unload_agents(wsdl_client, r['agent'], r['agentparentid'], r['agentname'])

        client_list_2 = []
        if not r['clientid'] is None:
            client_list_2.append("'" + r['clientid'] + "'")
        if not client_list_2:
            continue
        else:
            str_id = ",".join(client_list_2)
            get_client_groups(wsdl_client, cursor, str_id)

        is_disable = '0'
        if r['ismark']:
            is_disable = '1'
        nom = wsdl_client.hdb_type(name='', id=str(r['idartmarket']).strip(), idparent=r['clientid'].strip(),
                                   value1=r['agent'].strip(), value2=is_disable, value1num=r['isfolder'])
        clients_list.append(nom)

    hdb_array = wsdl_client.hdb_array_type(hdb_array=clients_list)
    logging.info('Загрузка начало клиенты агента')
    res = wsdl_client.client.service.load_hdb_table_part(hdb_array, 1, 'agentclients')
    logging.info(['Загрузка конец клиенты агента', res])


def unload_agent_products(cursor=None, wsdl_client=None, objid=''):
    products_list = []
    logging.info('Выборка ассортимент агента')
    cursor.execute('''
                select sc4843.id, sc4843.ismark,sp6122 as idartmarket,
                spragent.SP4808 as agent,
                spragent.descr as agentname,
                spragent.parentid as agentparentid,
                sprproducts.descr as productname,
                sprproducts.sp4802 as productid,
                sprproducts.isfolder
                from sc4843
                left join SC3246  as spragent WITH (NOLOCK) on parentext = spragent.id
                left join sc33 as sprproducts WITH (NOLOCK) on sp4841 = sprproducts.id
                where sc4843.id=%s
            ''', objid)
    logging.info('Выборка ассортимент агента завершена')
    logging.info('Подготовка загрузки ассортимент агента')
    row = cursor.fetchall()
    for r in row:
        logging.warning(r)
        if r['idartmarket'] is None or r['idartmarket'].strip() == '':
            logging.warning('Не задан идентификатор')
            continue
        if r['agentname'] is None or r['agentname'].strip() == '':
            logging.warning('Не задано имя агента')
            continue
        unload_agents(wsdl_client, r['agent'], r['agentparentid'], r['agentname'])
    #
        product_list_2 = []
        if not r['productid'] is None:
            product_list_2.append("'" + r['productid'] + "'")
        if not product_list_2:
            continue
        else:
            str_id = ",".join(product_list_2)
            get_client_groups(wsdl_client, cursor, str_id)
    #
        is_disable = '0'
        if r['ismark']:
            is_disable = '1'
        nom = wsdl_client.hdb_type(name='', id=str(r['idartmarket']).strip(), idparent=r['productid'].strip(),
                                   value1=r['agent'].strip(), value2=is_disable)
        products_list.append(nom)

    hdb_array = wsdl_client.hdb_array_type(hdb_array=products_list)
    logging.info('Загрузка начало клиенты агента')
    res = wsdl_client.client.service.load_hdb_table_part(hdb_array, 1, 'agentproducts')
    logging.info(['Загрузка конец ассортимент агента', res])


def unload_production_date(cursor=None, wsdl_client=None, objid=''):
    date_list = []
    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка даты розлива')
    cursor.execute('''
                select SP5641 as idartmarket, SP5194 as produce_date, SP5681 as egais_code, SC5196.descr,
                sc33.sp4802 as idartmarket_owner, SP5520 as gtd, SP5684 as gtd_date  from SC5196 WITH (NOLOCK)
                left join sc33 WITH (NOLOCK) on sc5196.parentext = sc33.id 
                where SC5196.id=%s
            ''', objid)
    logging.info('Выборка даты розлива завершена')
    logging.info('Подготовка загрузки даты розлива')
    row = cursor.fetchall()
    for r in row:
        logging.info(r)
        nom = hdb_type(name=r['descr'].strip(), id=str(r['idartmarket']).strip(), idparent='',
                       value1=r['idartmarket_owner'].strip(), value2=r['egais_code'].strip(),
                       value3=r['gtd'].strip(), value1date=r['produce_date'], value2date=r['gtd_date'])
        date_list.append(nom)
    hdb_array = hdb_array_type(hdb_array=date_list)
    logging.info('Загрузка начало даты розлива')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'pdate')
    logging.info('Загрузка даты розлива завершена')


# доверенности
def unload_attorney(cursor=None, wsdl_client=None, objid=''):
    attorney_list = []
    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка доверенности')
    cursor.execute('''
                        select  sp6139 as idartmarket, sc5584.code as number, sp5579 as date_in, sp5580 as date_out,
                        sp5581 as fio, sp5582 as status, sp4807 as patentid from sc5584 WITH (NOLOCK)
                        left join sc46 WITH (NOLOCK) on sc5584.parentext = sc46.id
                        where sc5584.id=%s''', objid)
    row = cursor.fetchall()
    logging.info(row)
    for r in row:
        nom = hdb_type(name=r'', id=str(r['idartmarket']).strip(), idparent='', value1=r['number'],
                       value1date=r['date_in'], value2date=r['date_out'], value2=r['fio'], value3=r['status'],
                       value4=r['patentid'])
        attorney_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=attorney_list)
    logging.info('Загрузка начало доверенности')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'attorney')
    logging.info('Загрузка доверенности завершена')


def unload_transport(cursor=None, wsdl_client=None, objid=''):
    analytic_list = []
    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка транспортные средства')
    cursor.execute('''
                        select descr as name, sp6140 as idartmarket, sp5531 as number, sp5539 as capacity,
                        sp5570 as volume  from sc5529 sc WITH (NOLOCK)  
                        where sc.id=%s''', objid)
    row = cursor.fetchall()

    logging.info(row)

    for r in row:
        nom = hdb_type(name=r['name'].strip(), id=str(r['idartmarket']).strip(), idparent='', value1=r['number'],
                       value1num=r['capacity'], value2num=r['volume'])
        analytic_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=analytic_list)
    logging.info('Загрузка начало транспортные средства')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'transport')
    logging.info('Загрузка транспортные средства завершена')


def unload_analytics(cursor=None, wsdl_client=None, objid=''):
    analytic_list = []
    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка аналитика')
    cursor.execute('''select sc.id, sc.descr as scname, sc.SP6137 as idartmarket, sc.isfolder
                        from SC5510 WITH (NOLOCK) sc  where sc.isfolder=2 and sc.id=%s''', objid)
    row = cursor.fetchall()

    logging.info(row)

    for r in row:
        logging.info(r['scname'])
        nom = hdb_type(name=r['scname'].strip(), id=str(r['idartmarket']).strip(), idparent='')
        analytic_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=analytic_list)
    logging.info('Загрузка начало аналитика')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'analytic')
    logging.info('Загрузка аналитика завершена')


def unload_bank_accounts(cursor=None, wsdl_client=None, objid=''):
    accounts_list = []

    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка счета клиентов')
    cursor.execute('''select SP6144 as idartmarket, sp1188 as bik, sp1184 as rs, SC1183.descr as name, sp4807  as parentid   from sc1183 
                    left join sc46 on sc1183.parentext = sc46.id where ltrim(rtrim(sp1184)) <> '' 
                    and ltrim(rtrim(sp1184)) <> 'новый' and len(ltrim(rtrim(sp1184)))=20  and 
                    len(ltrim(rtrim(sp1188))) = 9  and sc1183.id=%s ''', objid)
    row = cursor.fetchall()

    logging.info(row)

    for r in row:
        logging.info(r)
        nom = hdb_type(name=r['name'].strip(), id=r['idartmarket'].strip(), idparent=r['parentid'],
                       value1=r['bik'], value2=r['rs'].strip())
        accounts_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=accounts_list)
    logging.info('Загрузка начало счета клиентов')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'accounts')
    logging.info('Загрузка счета клиентов завершена')





def unload_cost(cursor=None, wsdl_client=None, objid=''):
    cost_list = []
    parents_id = ['ABD7028F-DACB-4CF0-BD60-D51FC849760B', 'FBE1B007-8050-476D-BD62-962B9D65C19D',
                  'FE7C24DA-E3D5-4186-A361-A1AEB5C5722E', '280F18F5-4E46-4774-B168-50891A2A765D',
                  '7A88694A-4C27-48C5-8425-228363F99F26']

    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка затраты')
    cursor.execute('''select sc.id, sc.descr as scname, sc.sp6125 as idartmarket,sc.isfolder, scp.descr  as scpname,
        scp.sp6125 as parentid, scp.descr as parentname from sc3773 sc
        left join sc3773 scp on sc.parentid = scp.id where sc.isfolder=2 and sc.id=%s''', objid)
    row = cursor.fetchall()

    logging.info(row)

    for r in row:
        logging.info(r['parentid'])
        if r['parentid'] is not None and r['parentid'].strip() in parents_id:
            logging.info(r)
            nom = hdb_type(name=r['scname'].strip(), id=str(r['idartmarket']).strip(), idparent=r['parentid'],
                           nameparent=r['scpname'])
            cost_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=cost_list)
    logging.info('Загрузка начало затраты')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'cost')
    logging.info('Загрузка затраты завершена')


def unload_unit(cursor=None, wsdl_client=None, objid=''):
    unit_list = []
    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка подразделения')
    cursor.execute('''select el.SP6126 as idartmarket, el.descr, el.parentid, pr.descr as parentname, pr.sp6126 as parentid
                    from SC3769 el left join sc3769 pr on el.parentid = pr.id 
                    where el.id=%s  and el.isfolder=2''', objid)
    logging.info('Выборка подразделения завершена')
    logging.info('Подготовка загрузки подразделения')
    row = cursor.fetchall()
    logging.info(objid)
    logging.info(row)
    for r in row:
        logging.warning(r)
        nom = hdb_type(name=r['descr'].strip(), id=str(r['idartmarket']).strip(), idparent=r['parentid'],
                       nameparent=r['parentname'])
        unit_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=unit_list)
    logging.info('Загрузка начало подразделения')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'unit')
    logging.info('Загрузка подразделения завершена')


def unload_for_marketing(cursor=None, wsdl_client=None, objid=''):
    marketing_list = []
    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка для маркетинга')
    cursor.execute('''
            select SP6129 as idartmarket, descr  from SC5554 where id=%s
            ''', objid)
    logging.info('Выборка для маркетинга завершена')
    logging.info('Подготовка загрузки для маркетинга')
    row = cursor.fetchall()
    for r in row:
        logging.warning(r)
        nom = hdb_type(name=r['descr'].strip(), id=str(r['idartmarket']).strip(), idparent='')
        marketing_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=marketing_list)
    logging.info('Загрузка начало для маркетинга')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'marketing')
    logging.info('Загрузка для маркетинга завершена')


def unload_financing(cursor=None, wsdl_client=None, objid=''):
    financing_list = []
    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка источник финансирования')
    cursor.execute('''
            select SP6128 as idartmarket, descr  from SC5552 where id=%s
            ''', objid)
    logging.info('Выборка источник финансирования завершена')
    logging.info('Подготовка загрузки источник финансирования')
    row = cursor.fetchall()
    for r in row:
        logging.warning(r)
        nom = hdb_type(name=r['descr'].strip(), id=str(r['idartmarket']).strip(), idparent='')
        financing_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=financing_list)
    logging.info('Загрузка начало источник финансирования')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'financing')
    logging.info('Загрузка источник финансирования завершена')


def unload_maker(cursor=None, wsdl_client=None, objid=''):
    maker_list = []
    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка производители')
    cursor.execute('''
            select SP6120 as idartmarket, SP5470 as inn, SP5471 as kpp, descr  from SC5468
             where id=%s
            ''', objid)
    logging.info('Выборка производители завершена')
    logging.info('Подготовка загрузки производители')
    row = cursor.fetchall()
    for r in row:
        logging.warning(r)
        nom = hdb_type(name=r['descr'].strip(), id=str(r['idartmarket']).strip(), idparent='', value1=r['inn'],
                       value2=r['kpp'])
        maker_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=maker_list)
    logging.info('Загрузка начало производители')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'maker')
    logging.info('Загрузка производители завершена')


def get_region_groups_filial(prm_cursor, prm_id_list='', wsdl_client=None):
    hdb_type = wsdl_client.client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.client.get_type('ns3:hdb_array_element')
    list_region = []
    nom_region = hdb_type(name=filial_region_name, id=filial_region_id, idparent='')
    list_region.append(nom_region)
    logging.info('Выборка регионов уровень 1')
    prm_cursor.execute('''select SP573 as idartmarket, descr,'' as idparent, '' as nameparent from
                       SC172 where ltrim(rtrim(parentid))='0' and isfolder=1''')
    logging.info('Выборка регионов уровень 1')
    logging.info('Подготовка регионов уровень 1')
    row = prm_cursor.fetchall()
    for r in row:
        nom_region = hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent=filial_region_id)
        list_region.append(nom_region)

    logging.info('Выборка регионов уровень 2')
    prm_cursor.execute('''select SC172.SP573 as idartmarket,SC172.descr,scparent.SP573 as idparent,
                          scparent.descr as nameparent from SC172 left join SC172 as scparent on SC172.parentid=scparent.id
                          where (SC172.parentid in (select id from SC172 where isfolder=1 and ltrim(rtrim(parentid))='0'))
                          and SC172.isfolder=1''')

    logging.info('Выборка регионов уровень 2')
    logging.info('Подготовка регионов уровень 2')
    row = prm_cursor.fetchall()
    for r in row:
        nom_region=hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(),idparent=r['idparent'])
        list_region.append(nom_region)

    hdb_array=hdb_array_type(hdb_array=list_region)
    logging.info('Загрузка регионов начало')
    wsdl_client.client.service.load_hdb_elements(hdb_array,1,'region')
    logging.info('Загрузка регионов завершена')


def get_region_groups(prm_cursor, prm_id_list='', wsdl_client=None):
    hdb_type = wsdl_client.client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.client.get_type('ns3:hdb_array_element')
    list_region = []
    logging.info('Выборка регионов уровень 1')
    prm_cursor.execute('''select sp4807 as idartmarket, descr,'' as idparent, '' as nameparent from
                       sc46 where ltrim(rtrim(parentid))='0' and isfolder=1''')
    logging.info('Выборка регионов уровень 1')
    logging.info('Подготовка регионов уровень 1')
    row = prm_cursor.fetchall()
    for r in row:
        nom_region = hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent='')
        list_region.append(nom_region)

    logging.info('Выборка регионов уровень 2')

    prm_cursor.execute('''select sc46.sp4807 as idartmarket,sc46.descr,scparent.sp4807 as idparent,
                          scparent.descr as nameparent, sc46.id from sc46 left join sc46 as scparent on sc46.parentid=scparent.id
                          where (sc46.parentid in (select id from sc46 where isfolder=1 and ltrim(rtrim(parentid))='0'))
                          and sc46.isfolder=1''')

    logging.info('Выборка регионов уровень 2')
    logging.info('Подготовка регионов уровень 2')
    row = prm_cursor.fetchall()
    for r in row:
        nom_region = hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent=r['idparent'])
        list_region.append(nom_region)

        logging.info('Выборка регионов уровень 3')
        prm_cursor.execute('''select sc46.sp4807 as idartmarket,sc46.descr,scparent.sp4807 as idparent,
                              scparent.descr as nameparent from sc46 left join sc46 as scparent on sc46.parentid=scparent.id
                              where sc46.isfolder=1 and ltrim(rtrim(sc46.parentid))=%s''', r['id'].strip())
        rows_child = prm_cursor.fetchall()
        logging.info('Подготовка регионов уровень 3')
        for r_child in rows_child:
            nom_region = hdb_type(name=r_child['descr'].strip(), id=r_child['idartmarket'].strip(), idparent=r_child['idparent'])
            list_region.append(nom_region)

    hdb_array = hdb_array_type(hdb_array=list_region)
    logging.info('Загрузка регионов начало')
    wsdl_client.client.service.load_hdb_elements(hdb_array, 1, 'region')
    logging.info('Загрузка регионов завершена')


def send_clients(prm_clients_rows, wsdl_client, id_prefix=''):
    for r in prm_clients_rows:
        list_clients = []

        inn = '0000000000'
        if not r['inn'].strip() == '':
            inn = r['inn'].strip()

        parent_kpp = ''
        if r['parent_kpp'] is not None:
            parent_kpp = r['parent_kpp']

        typett = ''
        if r['typett'] is not None:
            typett = r['typett']

        okpo = ''
        if r['okpo'] is not None:
            okpo = r['okpo']

        license1 = ''
        logging.info(r['license'])
        if r['license'] is not None:
            license1 = r['license']

        logging.info(r['control_license'])
        control_license = ''
        if r['control_license'] is not None:
            control_license = r['control_license']

        logging.info([license1.strip(), control_license.strip()])

        idparent = ''
        parent_descr = ''
        if r['idparent'] is not None and r['idparent'].strip() != '':
            idparent = r['idparent'].strip()
            parent_descr = r['descrparent']

        idparent2 = ''
        parent2_descr = ''
        if r['idparent2'] is not None and r['idparent2'].strip() != '':
            idparent2 = r['idparent2'].strip()
            parent2_descr = r['descrparent2']

        idparent3 = ''
        parent3_descr = ''
        if r['idparent3'] is not None and r['idparent3'].strip() != '':
            idparent3 = r['idparent3'].strip()
            parent3_descr = r['descrparent3']

        nom = wsdl_client.hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(),
                                   idparent=r['regionid'].strip(),
                                   value1=id_prefix+inn, value2=r['kpp'].strip(), value3=r['parentname'].strip(),
                                   value4=r['adres'].strip(), value5=r['adres_post'].strip(),
                                   value6=typett.strip(), value7=license1.strip(), value8=control_license.strip(),
                                   value9=idparent, value10=parent_descr,
                                   value11=idparent2, value12=parent2_descr,
                                   value13=idparent3, value14=parent3_descr, value15=okpo, value16=parent_kpp,
                                   value17=r['contact_staff'].strip(),
                                   value1date=r['license_begin_date'], value2date=r['license_end_date'],
                                   value1num=r['pernodricard_custtype'],
                                   value2num=r['pernodricard_custpositioning'],
                                   value3num=r['pernodricard_custreason'],
                                   value4num=r['pernodricard_custsegment'],
                                   value5num=r['pernodricard_custchannel'],
                                   value6num=r['pernodricard_custsubchannel'])
        list_clients.append(nom)

        hdb_array = wsdl_client.hdb_array_type(hdb_array=list_clients)
        logging.info('Загрузка клиентов начало')
        logging.info(hdb_array)
        wsdl_client.client.service.load_client_groups(hdb_array, 1)
        logging.info('Загрузка клиентов завершена')


def get_client_groups_filial(wsdl_client=None, prm_cursor=None, prm_id_list=''):
    logging.info('Выборка клиентов')
    if prm_id_list == '':
        prm_cursor.execute('''
            select  SP9312 as inn, SP9313 as kpp, descr, '' as parentname, SP573 as idartmarket, SP9314 as regionid,
            '' as adres, '' as adres_post, '' as typett
            from SC172 where (isfolder<>'1')                       
                           ''')
    else:
        prm_cursor.execute('''
            select  SP9312 as inn, SP9313 as kpp,descr,'' as parentname,SP573 as idartmarket, SP9314 as regionid,
            '' as adres, '' as adres_post, '' as typett
             from SC172 where (isfolder<>'1') and (SP573 in (''' + prm_id_list + '''))''')
    logging.info('Выборка клиентов завершена')
    logging.info('Подготовка загрузки клиентов')
    row = prm_cursor.fetchall()
    send_clients(row, wsdl_client=wsdl_client, id_prefix='Z')
    # TODO перенести префикс в конфиг


def get_client_groups(wsdl_client=None, prm_cursor=None, prm_id_list='', prm_parent_id_list=None, prm_id_list_mode=0):
    logging.info('Выборка клиентов')
    str_base_sql_query = '''
            select element1.SP56 as inn, element1.sp4603 as kpp, element1.descr, element1.sp48 as parentname,
            element1.sp4807 as idartmarket, element1.SP6066 as regionid, element1.SP3145 as adres,
            element1.SP50 as adres_post, SC5464.descr as typett,
            element1.SP3691 as license, element1.SP5239 as control_license, element1.SP5422 as license_begin_date,
            element1.SP5423 as license_end_date,
            hdb_custtype.code as pernodricard_custtype, hdb_custpositioning.code as pernodricard_custpositioning,
            hdb_custreason.code as  pernodricard_custreason, hdb_custsegment.code as pernodricard_custsegment,
            hdb_custchannel.code as pernodricard_custchannel, hdb_custsubchannel.code as pernodricard_custsubchannel,
            groups1.sp4807 as idparent,groups1.descr as descrparent,
            groups2.sp4807 as idparent2,groups2.descr as descrparent2,
            groups3.sp4807 as idparent3,groups3.descr as descrparent3, element1.SP5532 as okpo,
            element1.SP5585 as parent_kpp, element1.SP5253 as contact_staff
            from sc46 element1
            left join SC5464 on  element1.SP5467 = SC5464.id
            left join SC6090 hdb_custtype         on  element1.SP6093 = hdb_custtype.id
            left join SC6094 hdb_custpositioning  on  element1.SP6096 = hdb_custpositioning.id
            left join SC6097 hdb_custreason       on  element1.SP6099 = hdb_custreason.id
            left join SC6100 hdb_custsegment      on  element1.SP6102 = hdb_custsegment.id
            left join SC6108 hdb_custchannel      on  element1.SP6107 = hdb_custchannel.id
            left join SC6110 hdb_custsubchannel   on  element1.SP6112 = hdb_custsubchannel.id
            left join sc46  groups1 on element1.parentid=groups1.id
            left join sc46  groups2 on groups1.parentid=groups2.id
            left join sc46  groups3 on groups2.parentid=groups3.id
            where (element1.isfolder<>'1')  
                           '''
    logging.info(prm_id_list)
    if prm_id_list == '' and prm_parent_id_list is None:
        prm_cursor.execute(str_base_sql_query)
    elif prm_parent_id_list is not None:
        prm_cursor.execute(str_base_sql_query + ''' and (element1.parentid=%s)''', prm_parent_id_list['id'])
    else:
        if prm_id_list_mode == 0:
            prm_cursor.execute(str_base_sql_query + ''' and (element1.sp4807 in (''' + prm_id_list + '''))''')
        else:
            logging.info(str_base_sql_query + ''' and (element1.id in (''' + prm_id_list + '''))''')
            prm_cursor.execute(str_base_sql_query + ''' and (element1.id in (''' + prm_id_list + '''))''')
    logging.info('Выборка клиентов завершена')
    logging.info('Подготовка загрузки клиентов')
    row = prm_cursor.fetchall()
    send_clients(row, wsdl_client=wsdl_client)
