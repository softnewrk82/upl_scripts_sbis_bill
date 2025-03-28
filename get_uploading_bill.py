import requests
import json

import cryptography

import pandas as pd
import numpy as np

import xmltodict

import re
import requests

import warnings
warnings.simplefilter("ignore")

from functools import lru_cache

import importlib

import modules.api_info
importlib.reload(modules.api_info)

from datetime import datetime

from sqlalchemy import create_engine

from modules.api_info import var_encrypt_var_app_client_id
from modules.api_info import var_encrypt_var_app_secret
from modules.api_info import var_encrypt_var_secret_key

from modules.api_info import var_encrypt_url_sbis
from modules.api_info import var_encrypt_url_sbis_unloading

from modules.api_info import var_encrypt_var_db_user_name
from modules.api_info import var_encrypt_var_db_user_pass

from modules.api_info import var_encrypt_var_db_host
from modules.api_info import var_encrypt_var_db_port

from modules.api_info import var_encrypt_var_db_name
from modules.api_info import var_encrypt_var_db_name_for_upl
from modules.api_info import var_encrypt_var_db_schema
from modules.api_info import var_encryptvar_API_sbis
from modules.api_info import var_encrypt_API_sbis_pass

from modules.api_info import f_decrypt, load_key_external


var_app_client_id = f_decrypt(var_encrypt_var_app_client_id, load_key_external()).decode("utf-8")
var_app_secret = f_decrypt(var_encrypt_var_app_secret, load_key_external()).decode("utf-8")
var_secret_key = f_decrypt(var_encrypt_var_secret_key, load_key_external()).decode("utf-8")

url_sbis = f_decrypt(var_encrypt_url_sbis, load_key_external()).decode("utf-8")
url_sbis_unloading = f_decrypt(var_encrypt_url_sbis_unloading, load_key_external()).decode("utf-8")

var_db_user_name = f_decrypt(var_encrypt_var_db_user_name, load_key_external()).decode("utf-8")
var_db_user_pass = f_decrypt(var_encrypt_var_db_user_pass, load_key_external()).decode("utf-8")

var_db_host = f_decrypt(var_encrypt_var_db_host, load_key_external()).decode("utf-8")
var_db_port = f_decrypt(var_encrypt_var_db_port, load_key_external()).decode("utf-8")

var_db_name = f_decrypt(var_encrypt_var_db_name, load_key_external()).decode("utf-8")

var_db_name_for_upl = f_decrypt(var_encrypt_var_db_name_for_upl, load_key_external()).decode("utf-8")


var_db_schema = f_decrypt(var_encrypt_var_db_schema, load_key_external()).decode("utf-8")

API_sbis = f_decrypt(var_encryptvar_API_sbis, load_key_external()).decode("utf-8")
API_sbis_pass = f_decrypt(var_encrypt_API_sbis_pass, load_key_external()).decode("utf-8")



from modules.api_info import var_encrypt_TOKEN_yandex_users, f_decrypt, load_key_external
from modules.api_info import var_encrypt_var_login_da, var_encrypt_var_pass_da
# ____________________________________________________________________________________________

var_TOKEN = f_decrypt(var_encrypt_TOKEN_yandex_users, load_key_external()).decode("utf-8")
login_da = f_decrypt(var_encrypt_var_login_da, load_key_external()).decode("utf-8")
pass_da = f_decrypt(var_encrypt_var_pass_da, load_key_external()).decode("utf-8")




def send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn):
        
        var_login_da = str(login_da)
        var_pass_da = str(pass_da)


        # myuuid_sbis_down = str(uuid.uuid4())
        myuuid_sbis_down = "b57d09dc-a631-4dbc-9e01-f706920fcb29"
        # print('Your UUID is: ' + str(myuuid_sbis_down))
        # _________________________________

        url = "https://online.sbis.ru/auth/service/" 


        method = "СБИС.Аутентифицировать"

        params = {
            "Параметр": {
                "Логин": f"{var_login_da}",
                "Пароль": f"{var_pass_da}",
            }

        }
        parameters = {
        "jsonrpc": "2.0",
        "method": method,
        "params":params,
        "id": 0
        }
            
        response = requests.post(url, json=parameters)
        response.encoding = 'utf-8'


        str_to_dict = json.loads(response.text)
        access_token = str_to_dict["result"]
        # print("access_token:", access_token)

        headers = {
        "X-SBISSessionID": access_token,
        "Content-Type": "application/json",
        }  

        # _____________________________________________________________


        parameters_real = {

        "jsonrpc": "2.0",
        "protocol": 6,
        "method": "PublicMsgApi.MessageSend",
        "params": {
            "dialogID": myuuid_sbis_down,
            "messageID": None,
            "answer": None,
            "text": f"{datetime.now().date()} {var_link}, {var_doc_number}, {var_doc_data_main}, {var_doc_type}, {var_doc_counterparty_inn}",
            "document": "bc70081f-3ccf-4507-b753-0e185191bf8c",
            "files": {
                "fileId": "bc70081f-3ccf-4507-b753-0e185191bf8c",
                "isLink": "true",
            },
            "recipients": [
            "c943a420-f494-4a38-8975-d9db61c3dba7",
            # "dc283f5c-05fc-11ee-812e-3c846acc6838",
            ],
            "options": {
                "d": [
                    "СБИС.API (errors)",
                    0,
                    {}
                ],
                "s": [
                    {
                    "t": "Строка",
                    "n": "Title"
                    },
                    {
                    "t": "Число целое",
                    "n": "TextFormat"
                    },
                    {
                    "t": "JSON-объект",
                    "n": "ServiceObject"
                    }
                ],
                "_type": "record"
                }
            },
            "id": 1
            }



        url_real = "https://online.sbis.ru/msg/service/"

        response_points = requests.post(url_real, json=parameters_real, headers=headers)
        # str_to_dict_points = json.loads(response_points.text)
        # str_to_dict_points["result"]["d"][3]["chat_name"]


# name_unloading = f"sbis_{var_year}{var_month}{var_day}_uploading"
# name_unloading_exc = f"sbis_exc_{var_year}{var_month}{var_day}_uploading"


def sbis_bill_processing_0(date_from, date_to, name_unloading, name_unloading_exc):

    var_now = datetime.now()
    # var_day = f"{var_now.day:02d}"
    # var_month = f"{var_now.month:02d}"
    # var_year = f"{var_now.year:02d}"

    # var_day = '01'
    # var_month = '04'
    # var_year = '2024'
    
    # var_day = var_day
    # var_month = var_month
    # var_year = var_year


    
    date_from = date_from
    date_to = date_to

    # var_now_B = var_now.strftime("%B")

    print("date_from:", date_from)
    print("date_to:", date_to)
    # print("var_now_B:", var_now_B)

    # name_unloading = f"sbis_{var_year}{var_month}{var_day}_uploading"
    # name_unloading_exc = f"sbis_exc_{var_year}{var_month}{var_day}_uploading"


    def doc_append():
        doc_id.append(var_link)
        doc_type.append(var_doc_type)
        doc_number.append(var_doc_number)
        doc_full_name.append(var_doc_full_name)
        doc_data_main.append(var_doc_data_main)
        doc_at_created.append(var_doc_at_created)
        doc_counterparty_inn.append(var_doc_counterparty_inn)
        doc_counterparty_full_name.append(var_doc_counterparty_full_name)
        doc_provider_inn.append(var_doc_provider_inn)
        doc_provider_full_name.append(var_doc_provider_full_name)

        doc_assigned_manager.append(var_doc_assigned_manager)
        doc_department.append(var_doc_department)
        doc_notation.append(var_doc_notation)

    def doc_append_exc():
        doc_id_exc.append(var_link)
        doc_type_exc.append(var_doc_type)
        doc_number_exc.append(var_doc_number)
        doc_full_name_exc.append(var_doc_full_name)
        doc_data_main_exc.append(var_doc_data_main)
        doc_at_created_exc.append(var_doc_at_created)
        doc_counterparty_inn_exc.append(var_doc_counterparty_inn)
        doc_counterparty_full_name_exc.append(var_doc_counterparty_full_name)
        doc_provider_inn_exc.append(var_doc_provider_inn)
        doc_provider_full_name_exc.append(var_doc_provider_full_name)

        doc_assigned_manager_exc.append(var_doc_assigned_manager)
        doc_department_exc.append(var_doc_department)

    def inside_doc_append(var_inside_doc_author,
                        var_inside_dict_total_sell,
                        var_inside_dict_total_payment,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        ):
        
        inside_doc_author.append(var_inside_doc_author)
        inside_dict_total_sell.append(var_inside_dict_total_sell)
        inside_dict_total_payment.append(var_inside_dict_total_payment)
        inside_doc_type.append(var_inside_doc_type)
        inside_doc_item_full_doc_price.append(var_inside_doc_item_full_doc_price)
        
        inside_doc_item_note.append(var_inside_doc_item_note)
        
        inside_doc_item_code.append(var_inside_doc_item_code)
        inside_doc_item_article.append(var_inside_doc_item_article)
        inside_doc_item_article.append(var_inside_doc_item_article)
        inside_doc_item_article.append(var_inside_doc_item_article)
        # inside_doc_item_sn.append(var_inside_doc_item_sn)
        inside_doc_item_name.append(var_inside_doc_item_name)
        
        inside_doc_item_quantity.append(var_inside_doc_item_quantity)
        inside_doc_item_unit.append(var_inside_doc_item_unit)
        
        inside_doc_item_price.append(var_inside_doc_item_price)
        inside_doc_item_full_item_price.append(var_inside_doc_item_full_item_price)
        # inside_agent.append(var_agent)
        # inside_date_from.append(var_date_from)
        
        # inside_license_type.append(var_license_type)


    
    def def_act_vr(xml_a, var_inside_doc_author, var_inside_doc_type):
        
        # _________________________________________________________________________________________________________________________________________________    
        def def_act_vr_print(var_inside_doc_author, var_inside_doc_type):
            
            if type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == dict:
                print(var_inside_doc_type)
                print("dict")
                
                var_quantity = 1 
                
                for x in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["ИнфПолеОписРабот"]:
                    if x["@Идентиф"] == "ПоляНоменклатуры":
                        try:
                            print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", x["@Значен"])[0])
                            print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                        except:
                            try:
                                print("var_inside_doc_item_article", re.findall(f"услуга|услуги", x["@Значен"].lower())[0])
                                print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                            except:
                                print("var_inside_doc_item_article", np.nan)
                    elif x["@Идентиф"] == "Примечание":
                        print("var_inside_doc_item_note", x["@Значен"])            
                    elif x["@Идентиф"] == "НазваниеПоставщика":
                        print("var_inside_doc_item_name", x["@Значен"])            
                    elif x["@Идентиф"] == "КодПоставщика":
                        print("var_inside_doc_item_code", x["@Значен"])    
                                                    
                try:
                    print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Количество"])
                except: 
                    pass
                print("var_inside_doc_item_unit", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимЕдИзм"])
                try:
                    print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимРабот"])
                except:
                    pass
                
                try:
                    print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Цена"])
                except:
                    print("var_inside_doc_item_price", np.nan) 
                
                print("var_inside_doc_item_full_doc_price", xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"])     
                print("_____________________")
    
    
            elif type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == list:
                    
                    sum_inside_doc = 0
                    
                    for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
                        sum_inside_doc += float(k["@СтоимУчНДС"])
            
                    for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
            
                        print(var_inside_doc_type)
                        print("list")
                        
                        var_quantity = 1
                        
                        for x in k["ИнфПолеОписРабот"]:
                            if x["@Идентиф"] == "ПоляНоменклатуры":
                                try:
                                    print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", x["@Значен"])[0])
                                    print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                                    var_quantity = re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', '')  
                                except:
                                    try:
                                        print("var_inside_doc_item_article", re.findall(f"услуга|услуги", x["@Значен"].lower())[0])
                                        print("var_inside_doc_item_quantity", re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', ''))
                                        var_quantity = re.findall(f'\"\d+\"', x["@Значен"])[0].replace('"', '')
                                    except:
                                        print("@Артикул", "отсутствует")
                            elif x["@Идентиф"] == "Примечание":
                                print("var_inside_doc_item_note", x["@Значен"])            
                            elif x["@Идентиф"] == "НазваниеПоставщика":
                                print("var_inside_doc_item_name", x["@Значен"])            
                            elif x["@Идентиф"] == "КодПоставщика":
                                print("var_inside_doc_item_code", x["@Значен"])
                            
                            elif x["@Идентиф"] == "Примечание":
                                var_inside_doc_item_note = x["@Значен"] 
            
                            elif x["@Идентиф"] == "НазваниеПоставщика":
                                var_inside_doc_item_name = x["@Значен"]
            
                            elif x["@Идентиф"] == "КодПоставщика":  
                                var_inside_doc_item_code =  x["@Значен"]
            
                        try:
                            print("var_inside_doc_item_quantity", k["@Количество"])
                        except:
                            pass
                        try:
                            print("var_inside_doc_item_name", k["@НаимРабот"])
                        except:
                            pass
    
                        try:
                            print("var_inside_doc_item_price", k["@Цена"])
                        except:
                            print("var_inside_doc_item_price", np.nan)
                            
                        print("var_inside_doc_item_full_doc_price", k["@СтоимУчНДС"])     
                        print("_____________________")
    
                    
        # _________________________________________________________________________________________________________________________________________________            
        def def_act_vr_set_variable(var_inside_doc_author, var_inside_doc_type):
            if type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == dict:
                
                var_inside_doc_item_quantity = 1  
        
                var_inside_doc_author = var_inside_doc_author
                var_inside_doc_type = var_inside_doc_type
                var_inside_doc_item_full_doc_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"]
                
                var_inside_doc_item_note = ""
                var_inside_doc_item_article = ""
                var_inside_doc_item_code = ""
    
    
                # if type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == dict:
    
                x = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]
                
                if type(x["ИнфПолеОписРабот"]) == dict:
                    if x["ИнфПолеОписРабот"]["@Идентиф"] == "Ид":
                        var_inside_doc_item_code =  x["ИнфПолеОписРабот"]["@Значен"]
    
                elif type(x["ИнфПолеОписРабот"]) == list:
                    for i_info in range(len(x["ИнфПолеОписРабот"])):
                        if x["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Примечание":
                           var_inside_doc_item_note = x["ИнфПолеОписРабот"][i_info]["@Значен"] 
                        elif x["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Ид":
                           var_inside_doc_item_code = x["ИнфПолеОписРабот"][i_info]["@Значен"] 
                        elif x["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Артикул":
                           var_inside_doc_item_article = x["ИнфПолеОписРабот"][i_info]["@Значен"] 
                        
                var_inside_doc_item_sn = ''
    
                try:
                    var_inside_doc_item_full_doc_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"]                    
                except: 
                    var_inside_doc_item_full_doc_price = np.nan
    
                
                try:
                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Цена"]                    
                except: 
                    var_inside_doc_item_price = np.nan
    
                try:
                    var_inside_doc_item_full_item_price = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@СтоимУчНДС"]                    
                except: 
                    var_inside_doc_item_full_item_price = np.nan
    
                try:
                    var_inside_doc_item_quantity =  xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@Количество"]
                except: 
                    pass
        
                try:
                    var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимЕдИзм"]
                except:
                    var_inside_doc_item_unit = np.nan
        
                try:
                    var_inside_doc_item_name = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["@НаимРабот"]
                except:
                    
                    try:
                        var_inside_doc_item_name = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]["Описание"]
                    except:
                        var_inside_doc_item_name = ''
    
                # _____________________________________________________
                var_agent = ''
                var_date_from = ''
        
        
                try:
                    if type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]) == list:
                        str_agent = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]
                        for v_a_d in str_agent:
                            if v_a_d["@Идентиф"].lower() == 'агент':
                                    try:
                                        var_agent = v_a_d["@Значен"].replace("&quot;", "")
                                    except:    
                                        var_agent = v_a_d["@Значен"]
                            
                            elif v_a_d["@Идентиф"].lower() == 'дата' or v_a_d["@Идентиф"].lower() == 'датас':
                                    var_date_from = v_a_d["@Значен"]
                            
                            else:
                                pass
                except:
                    pass
                
                #_________________________________         
                try:  
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''   
                except: 
                    var_license_type = ''               
                    
                # _______________________________________    
    
    
                #_________________________________ 
                
                # _____________________________________________________
                
                
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,
                    )
    
                # print("DICT")
                # print("var_link:", var_link)
                # print("var_doc_type:", var_doc_type)
                # print("var_doc_number:", var_doc_number)
                # print("var_doc_full_name:", var_doc_full_name)
                # print("var_doc_data_main:", var_doc_data_main)
                # print("var_doc_at_created:", var_doc_at_created)
                # print("var_doc_counterparty_inn:", var_doc_counterparty_inn)
                # print("var_doc_counterparty_full_name:", var_doc_counterparty_full_name)
                # print("var_doc_provider_inn:", var_doc_provider_inn)
                # print("var_doc_provider_full_name:", var_doc_provider_full_name)
                # print("var_doc_assigned_manager:", var_doc_assigned_manager)
                # print("var_doc_department:", var_doc_department)
                
                # print("var_inside_doc_author:", var_inside_doc_author)
                # print("var_inside_doc_type:", var_inside_doc_type)
                # print("var_inside_doc_item_full_doc_price:", var_inside_doc_item_full_doc_price)
                # print("var_inside_doc_item_note:", var_inside_doc_item_note)
                # print("var_inside_doc_item_code:", var_inside_doc_item_code)
                # print("var_inside_doc_item_article:", var_inside_doc_item_article)
                # print("var_inside_doc_item_name:", var_inside_doc_item_name)
                # print("var_inside_doc_item_quantity:", var_inside_doc_item_quantity)
                # print("var_inside_doc_item_unit:", var_inside_doc_item_unit)
                # print("var_inside_doc_item_price:", var_inside_doc_item_price)
                # print("var_agent:", var_agent)
                # print("var_date_from:", var_date_from)
                # print("________________________________________________________________________________")
    
    
    
            
            elif type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]) == list:
                
                sum_inside_doc = 0
                        
                for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
                    sum_inside_doc += float(k["@СтоимУчНДС"])
                    
                    
                # _____________________________________________________
                var_agent = ''
                var_date_from = ''
    
                try:
                    if type(xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]) == list:
                        str_agent = xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ИнфПолФХЖ1"]["ТекстИнф"]
                        for v_a_d in str_agent:
                            if v_a_d["@Идентиф"].lower() == 'агент':
                                    try:
                                        var_agent = v_a_d["@Значен"][2:-2].replace("&quot;", "")
                                    except:    
                                        var_agent = v_a_d["@Значен"][2:-2]
                            
                            elif v_a_d["@Идентиф"].lower() == 'дата' or v_a_d["@Идентиф"].lower() == 'датас':
                                    var_date_from = v_a_d["@Значен"]
                            
                            else:
                                pass
                except:
                    pass
                
                #_________________________________         
                try: 
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''                    
                    
                # _______________________________________    
    
                #_________________________________ 
                
                # _____________________________________________________                     
                    
                    
        
                for k in xml_a["Файл"]["Документ"]["СвДокПРУ"]["СодФХЖ1"]["ОписРабот"]["Работа"]:
        
                    var_inside_doc_item_quantity = 1
        
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = sum_inside_doc
                
                    var_inside_doc_item_note = ""
                    var_inside_doc_item_article = ""
                    var_inside_doc_item_code = ""
    
                    
                    if type(k["ИнфПолеОписРабот"]) == dict:
                          if k["ИнфПолеОписРабот"]["@Идентиф"] == "Ид":
                             var_inside_doc_item_code =  k["ИнфПолеОписРабот"]["@Значен"]
                    
                    elif type(k["ИнфПолеОписРабот"]) == list:
                          for i_info in range(len(k["ИнфПолеОписРабот"])):
                             if k["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Примечание":
                                var_inside_doc_item_note = k["ИнфПолеОписРабот"][i_info]["@Значен"] 
                             elif k["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Ид":
                                var_inside_doc_item_code = k["ИнфПолеОписРабот"][i_info]["@Значен"] 
                             elif k["ИнфПолеОписРабот"][i_info]["@Идентиф"] == "Артикул":
                                var_inside_doc_item_article = k["ИнфПолеОписРабот"][i_info]["@Значен"]                          
        
                    var_inside_doc_item_sn = ''
                    
                    try:
                        var_inside_doc_item_full_doc_price = sum_inside_doc                 
                    except: 
                        var_inside_doc_item_full_doc_price = np.nan
    
                    
                    try:
                        var_inside_doc_item_price = k["@Цена"]                    
                    except: 
                        var_inside_doc_item_price = np.nan
    
                    try:
                        var_inside_doc_item_full_item_price = k["@СтоимУчНДС"]                    
                    except: 
                        var_inside_doc_item_full_item_price = np.nan
    
                    try:
                        var_inside_doc_item_quantity =  k["@Количество"]
                    except: 
                        pass
            
                    try:
                        var_inside_doc_item_unit = k["@НаимЕдИзм"]
                    except:
                        var_inside_doc_item_unit = np.nan
            
                    try:
                        var_inside_doc_item_name = k["@НаимРабот"]
                    except:
                        try:
                            var_inside_doc_item_name = k["Описание"]
                        except:
                            var_inside_doc_item_name = ''
        
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_sn,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        var_agent,
                        var_date_from,
                        var_license_type,
                        )
    
                #     print("LIST")
                #     print("var_link:", var_link)
                #     print("var_doc_type:", var_doc_type)
                #     print("var_doc_number:", var_doc_number)
                #     print("var_doc_full_name:", var_doc_full_name)
                #     print("var_doc_data_main:", var_doc_data_main)
                #     print("var_doc_at_created:", var_doc_at_created)
                #     print("var_doc_counterparty_inn:", var_doc_counterparty_inn)
                #     print("var_doc_counterparty_full_name:", var_doc_counterparty_full_name)
                #     print("var_doc_provider_inn:", var_doc_provider_inn)
                #     print("var_doc_provider_full_name:", var_doc_provider_full_name)
                #     print("var_doc_assigned_manager:", var_doc_assigned_manager)
                #     print("var_doc_department:", var_doc_department)
    
                #     print("var_inside_doc_author:", var_inside_doc_author)
                #     print("var_inside_doc_type:", var_inside_doc_type)
                #     print("var_inside_doc_item_full_doc_price:", var_inside_doc_item_full_doc_price)
                #     print("var_inside_doc_item_note:", var_inside_doc_item_note)
                #     print("var_inside_doc_item_code:", var_inside_doc_item_code)
                #     print("var_inside_doc_item_article:", var_inside_doc_item_article)
                #     print("var_inside_doc_item_name:", var_inside_doc_item_name)
                #     print("var_inside_doc_item_quantity:", var_inside_doc_item_quantity)
                #     print("var_inside_doc_item_unit:", var_inside_doc_item_unit)
                #     print("var_inside_doc_item_price:", var_inside_doc_item_price)
                #     print("var_agent:", var_agent)
                #     print("var_date_from:", var_date_from)
                #     print("____________________________________")
                # print("________________________________________________________________________________")
    
        # _________________________________________________________________________________________________________________________________________________            
    
        # def_act_vr_print(var_inside_doc_author, var_inside_doc_type)
        def_act_vr_set_variable(var_inside_doc_author, var_inside_doc_type)
        
    def def_edo_nakl(xml_a, var_inside_doc_author, var_inside_doc_type, var_inside_dict_total_sell, var_inside_dict_total_payment):
        
        # _________________________________________________________________________________________________________________________________________________    
        def def_edo_nakl_print(var_inside_doc_author, var_inside_doc_type):
            
            if type(xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]) == dict:
                var_inside_doc_item_note = ""
                print(var_inside_doc_type)
                print("dict")
            
                try: 
                    print("var_inside_doc_item_article", xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@АртикулТов"])
                except:
                    print("var_inside_doc_item_article", np.nan)
                print("var_inside_doc_item_code", xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@КодТов"])
                print("var_inside_doc_item_name", xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@НаимТов"])
                print("var_inside_doc_item_quantity", xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@НеттоПередано"])
                try:
                    print("var_inside_doc_item_unit", xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@НаимЕдИзм"])
                except: 
                    print("var_inside_doc_item_unit", np.nan)
                print("var_inside_doc_item_price", xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@Цена"])
                print("var_inside_doc_item_full_item_price", xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@СтУчНДС"])
                print("var_inside_doc_item_full_doc_price", xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@СтУчНДС"])
                print("_____________________")
    
    
            elif type(xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]) == list:
                sum_inside_doc = 0
                
                for k in xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]:
                    sum_inside_doc += float(k["@СтУчНДС"])
            
                for k in xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]: 
                    
                    print(var_inside_doc_type)
                    print("list")
                    
                    try:
                        print("var_inside_doc_item_article", k["@АртикулТов"])
                    except:
                        print("var_inside_doc_item_article", "отсутствует")
                    print("var_inside_doc_item_code", k["@КодТов"])
                    print("var_inside_doc_item_name", k["@НаимТов"])
                    print("var_inside_doc_item_quantity", k["@НеттоПередано"])
                    print("var_inside_doc_item_unit", k["@НаимЕдИзм"])
                    print("var_inside_doc_item_price", k["@Цена"])
                    print("var_inside_doc_item_full_item_price", k["@СтУчНДС"])
                    print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                    print("_____________________")
            
                    
        # _________________________________________________________________________________________________________________________________________________            
        def def_edo_nakl_set_variable(var_inside_doc_author, var_inside_doc_type, var_inside_dict_total_sell, var_inside_dict_total_payment):   
            if type(xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]) == dict:
            
                var_inside_doc_item_note = ""
                var_inside_doc_author = var_inside_doc_author
                var_inside_dict_total_sell = var_inside_dict_total_sell
                var_inside_dict_total_payment = var_inside_dict_total_payment
                var_inside_doc_type = var_inside_doc_type
                try: 
                    var_inside_doc_item_full_doc_price = xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@СтТовУчНДС"]
                except: 
                    ; var_inside_doc_item_full_doc_price = np.nan
                    var_inside_doc_item_full_doc_price = xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@СтТовУчНал"]
                try:
                    var_inside_doc_item_code = xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@КодТов"]
                except:
                    var_inside_doc_item_code = np.nan
                try:
                    var_inside_doc_item_article = xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@АртикулТов"]
                except:
                    var_inside_doc_item_article = ''
                 
                
                    
                    
                try:
                    var_inside_doc_item_name = xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@НаимТов"]
                except:
                    var_inside_doc_item_name = np.nan
                try:
                    var_inside_doc_item_quantity = xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@КолТов"]
                except: 
                    var_inside_doc_item_quantity = np.nan
                try:
                    var_inside_doc_item_unit = xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@ЕдИзм"]
                except:
                    var_inside_doc_item_unit = np.nan
                try:
                    try:
                        var_inside_doc_item_price = xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@Цена"]
                    except:
                        var_inside_doc_item_price = xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@ЦенаТовСНДС"]
                except:
                    var_inside_doc_item_price = np.nan
                    
                try:
                    var_inside_doc_item_full_item_price = float(xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@СтТовУчНДС"])
                except:
                    var_inside_doc_item_full_item_price = float(xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]["@СтТовУчНал"])
                      


                
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    var_inside_dict_total_sell,
                    var_inside_dict_total_payment,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,
                    )
                # print("DICT")
                # print("var_link:", var_link)
                # print("var_doc_type:", var_doc_type)
                # print("var_doc_number:", var_doc_number)
                # print("var_doc_full_name:", var_doc_full_name)
                # print("var_doc_data_main:", var_doc_data_main)
                # print("var_doc_at_created:", var_doc_at_created)
                # print("var_doc_counterparty_inn:", var_doc_counterparty_inn)
                # print("var_doc_counterparty_full_name:", var_doc_counterparty_full_name)
                # print("var_doc_provider_inn:", var_doc_provider_inn)
                # print("var_doc_provider_full_name:", var_doc_provider_full_name)
                # print("var_doc_assigned_manager:", var_doc_assigned_manager)
                # print("var_doc_department:", var_doc_department)
                
                # print("var_inside_doc_author:", var_inside_doc_author)
                # print("var_inside_dict_total_sell:", var_inside_dict_total_sell)
                # print("var_inside_dict_total_payment:", var_inside_dict_total_payment)
                # print("var_inside_doc_type:", var_inside_doc_type)
                # print("var_inside_doc_item_full_doc_price:", var_inside_doc_item_full_doc_price)
                # print("var_inside_doc_item_note:", var_inside_doc_item_note)
                # print("var_inside_doc_item_code:", var_inside_doc_item_code)
                # print("var_inside_doc_item_article:", var_inside_doc_item_article)
                # print("var_inside_doc_item_name:", var_inside_doc_item_name)
                # print("var_inside_doc_item_quantity:", var_inside_doc_item_quantity)
                # print("var_inside_doc_item_unit:", var_inside_doc_item_unit)
                # print("var_inside_doc_item_price:", var_inside_doc_item_price)

                # print("________________________________________________________________________________")
                
            elif type(xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]) == list:
            
                sum_inside_doc = 0
                
                for k in xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]:
                    try:
                        sum_inside_doc += float(k["@СтТовУчНДС"])
                    except:
                        try:
                            sum_inside_doc += float(k["@СтТовУчНал"])
                        except:
                            sum_inside_doc += 0            
                    
                for k in xml_a['Файл']["Документ"]["ТаблСчет"]["СведТов"]: 
                    
                    var_inside_doc_item_note = ""
                
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    try:
                        var_inside_doc_item_full_doc_price = sum_inside_doc
                    except: 
                        var_inside_doc_item_full_doc_price = np.nan
                    try:
                        var_inside_doc_item_code =  k["@КодТов"]
                    except: 
                        var_inside_doc_item_code = np.nan
                    try:
                        var_inside_doc_item_article = k["@АртикулТов"]
                    except:
                        var_inside_doc_item_article = ''
                    
                    try:
                        var_inside_doc_item_name = k["@НаимТов"]
                    except:
                        var_inside_doc_item_name = np.nan
                    try:
                        var_inside_doc_item_quantity =  k["@КолТов"]
                    except:
                        var_inside_doc_item_quantity = np.nan
                    try:
                        var_inside_doc_item_unit = k["@ЕдИзм"]
                    except: 
                        var_inside_doc_item_unit = np.nan
                    try:
                        try:                                
                            var_inside_doc_item_price = k["@Цена"]
                        except:
                            var_inside_doc_item_price = k["@ЦенаТовСНДС"]
                    except:
                        var_inside_doc_item_price = np.nan
                    try:
                        var_inside_doc_item_full_item_price = float(k["@СтТовУчНДС"])
                    except:
                        var_inside_doc_item_full_item_price = float(k["@СтТовУчНал"])
                
                
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_dict_total_sell,
                        var_inside_dict_total_payment,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        )
                
                #     print("LIST")
                #     print("var_link:", var_link)
                #     print("var_doc_type:", var_doc_type)
                #     print("var_doc_number:", var_doc_number)
                #     print("var_doc_full_name:", var_doc_full_name)
                #     print("var_doc_data_main:", var_doc_data_main)
                #     print("var_doc_at_created:", var_doc_at_created)
                #     print("var_doc_counterparty_inn:", var_doc_counterparty_inn)
                #     print("var_doc_counterparty_full_name:", var_doc_counterparty_full_name)
                #     print("var_doc_provider_inn:", var_doc_provider_inn)
                #     print("var_doc_provider_full_name:", var_doc_provider_full_name)
                #     print("var_doc_assigned_manager:", var_doc_assigned_manager)
                #     print("var_doc_department:", var_doc_department)
                
                #     print("var_inside_doc_author:", var_inside_doc_author)
                #     print("var_inside_dict_total_sell:", var_inside_dict_total_sell)
                #     print("var_inside_dict_total_payment:", var_inside_dict_total_payment)
                #     print("var_inside_doc_type:", var_inside_doc_type)
                #     print("var_inside_doc_item_full_doc_price:", var_inside_doc_item_full_doc_price)
                #     print("var_inside_doc_item_note:", var_inside_doc_item_note)
                #     print("var_inside_doc_item_code:", var_inside_doc_item_code)
                #     print("var_inside_doc_item_article:", var_inside_doc_item_article)
                #     print("var_inside_doc_item_name:", var_inside_doc_item_name)
                #     print("var_inside_doc_item_quantity:", var_inside_doc_item_quantity)
                #     print("var_inside_doc_item_unit:", var_inside_doc_item_unit)
                #     print("var_inside_doc_item_price:", var_inside_doc_item_price)
                #     print("____________________________________")
                # print("________________________________________________________________________________")
    
    
        # _________________________________________________________________________________________________________________________________________________            
    
        # def_edo_nakl_print(var_inside_doc_author, var_inside_doc_type)
        def_edo_nakl_set_variable(var_inside_doc_author, var_inside_doc_type, var_inside_dict_total_sell, var_inside_dict_total_payment)
    
    def def_act_pp(xml_a, var_inside_doc_author, var_inside_doc_type, dict_total_sell, dict_total_payment):
        
        # _________________________________________________________________________________________________________________________________________________    
        def def_act_pp_print(var_inside_doc_author, var_inside_doc_type):
            
    
            if type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == dict:
                print(var_inside_doc_type)
                print("dict")
    
                var_inside_doc_item_note = ""
            
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
            
                    try:
                        print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0])
                    except:
                        try:
                            print("var_inside_doc_item_article", re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0])
            
                        except:
                            print("var_inside_doc_item_article", np.nan)
                
            
            
                print("var_inside_doc_item_code", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КодПП"])
                print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КолПП"])
                print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@НаимПП"])
                try:
                    print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Цена"])
                except:
                    print("var_inside_doc_item_price", np.nan)               
                print("var_inside_doc_item_full_item_price", float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"]))
                sum_inside_doc = float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"])
                print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                print("_____________________")
    
            
            elif type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == list:
                print(var_inside_doc_type)
                print("list")
    
                print(type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]))
                
                sum_inside_doc = 0
            
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
                    sum_inside_doc += float(k["@Стоим"])
                                                            
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
                    
                    try:
                        print("var_inside_doc_item_article", re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0])
                    except:
                        try:
                            print("var_inside_doc_item_article", re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0])
            
                        except:
                            print("var_inside_doc_item_article", np.nan)
                    
                    print("var_inside_doc_item_code", k["@КодПП"])
                    print("var_inside_doc_item_quantity", k["@КолПП"])
                    print("var_inside_doc_item_name", k["@НаимПП"])
                    try:
                        print("var_inside_doc_item_price", k["@Цена"])
                    except:
                        print("var_inside_doc_item_price", np.nan)
                    print("var_inside_doc_item_full_item_price", float(k["@Стоим"]))
                    print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                    print("_____________________")
    
                
                                
                    
        # _________________________________________________________________________________________________________________________________________________            
        def def_act_pp_set_variable(var_inside_doc_author, var_inside_doc_type):
    
    
            if type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == dict:
                
                var_inside_doc_item_note = ""
    
                var_inside_doc_author = var_inside_doc_author
                var_inside_doc_type = var_inside_doc_type
                var_inside_doc_item_full_doc_price = float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"])
                var_inside_doc_item_code = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КодПП"]
            
            
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
            
                    try:
                        var_inside_doc_item_article = re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0]
                    except:
                        try:
                            var_inside_doc_item_article = re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0]
                        except:
                            var_inside_doc_item_article = ''
                
                var_inside_doc_item_sn = ''
                    
                var_inside_doc_item_name = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@НаимПП"]
                var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@КолПП"]
                var_inside_doc_item_unit = np.nan
                try:
                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Цена"]
                except:
                    var_inside_doc_item_price = np.nan
                var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]["@Стоим"])
                
                
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СведДок"]["ИнфПол"]["@ТекстИнф"]
                    if len(re.findall(f'Имя="Агент"', str_agent)) != 0:
                        start_index = re.search(f'\\[&apos;', str_agent).span()[1]
                        end_index = re.search(f'&apos;\\]', str_agent).span()[0]
                        try:
                            var_agent = str_agent[start_index:end_index].replace("&quot;", "")
                        except:
                            var_agent = str_agent[start_index:end_index]
                    else:
                        var_agent = ''
    
                except:
                    var_agent = ''
    
    
    
                # Дата С__________________________________________________
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СведДок"]["ИнфПол"]["@ТекстИнф"]
                    if len(re.findall(f'Имя="ДАТА"', str_date_from)) != 0:
                        index_start_str = re.search(f'Имя="ДАТА"', str_date_from).span()[1]
                        temp_str = str_date_from[index_start_str:]
                        var_date_from = re.findall(f'\d+\.\d+\.\d+', temp_str)[0]
                    else:
                        var_date_from = ''
                except:
                    var_date_from = ''
                    
                #_________________________________         
                try: 
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''                    
                    
                # _______________________________________    
    
                
                
                
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_sn,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,
                    var_agent,
                    var_date_from,
                    var_license_type,
                    ) 
                
                # print("DICT")
                # print("var_link:", var_link)
                # print("var_doc_type:", var_doc_type)
                # print("var_doc_number:", var_doc_number)
                # print("var_doc_full_name:", var_doc_full_name)
                # print("var_doc_data_main:", var_doc_data_main)
                # print("var_doc_at_created:", var_doc_at_created)
                # print("var_doc_counterparty_inn:", var_doc_counterparty_inn)
                # print("var_doc_counterparty_full_name:", var_doc_counterparty_full_name)
                # print("var_doc_provider_inn:", var_doc_provider_inn)
                # print("var_doc_provider_full_name:", var_doc_provider_full_name)
                # print("var_doc_assigned_manager:", var_doc_assigned_manager)
                # print("var_doc_department:", var_doc_department)
                
                # print("var_inside_doc_author:", var_inside_doc_author)
                # print("var_inside_doc_type:", var_inside_doc_type)
                # print("var_inside_doc_item_full_doc_price:", var_inside_doc_item_full_doc_price)
                # print("var_inside_doc_item_note:", var_inside_doc_item_note)
                # print("var_inside_doc_item_code:", var_inside_doc_item_code)
                # print("var_inside_doc_item_article:", var_inside_doc_item_article)
                # print("var_inside_doc_item_name:", var_inside_doc_item_name)
                # print("var_inside_doc_item_quantity:", var_inside_doc_item_quantity)
                # print("var_inside_doc_item_unit:", var_inside_doc_item_unit)
                # print("var_inside_doc_item_price:", var_inside_doc_item_price)
                # print("var_agent:", var_agent)
                # print("var_date_from:", var_date_from)
                # print("________________________________________________________________________________")
            
            elif type(xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]) == list:      
    
                sum_inside_doc = 0
    
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
                    sum_inside_doc += float(k["@Стоим"]) 
                    
                
                
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СведДок"]["ИнфПол"]["@ТекстИнф"]
                    if len(re.findall(f'Имя="Агент"', str_agent)) != 0:
                        start_index = re.search(f'\\[&apos;', str_agent).span()[1]
                        end_index = re.search(f'&apos;\\]', str_agent).span()[0]
                        try:
                            var_agent = str_agent[start_index:end_index].replace("&quot;", "")
                        except:
                            var_agent = str_agent[start_index:end_index]
                    else:
                        var_agent = ''
    
                except:
                    var_agent = ''
    
    
    
                # Дата С__________________________________________________
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СведДок"]["ИнфПол"]["@ТекстИнф"]
                    if len(re.findall(f'Имя="ДАТА"', str_date_from)) != 0:
                        index_start_str = re.search(f'Имя="ДАТА"', str_date_from).span()[1]
                        temp_str = str_date_from[index_start_str:]
                        var_date_from = re.findall(f'\d+\.\d+\.\d+', temp_str)[0]
                    else:
                        var_date_from = ''
                except:
                    var_date_from = ''                        
                    
                #_________________________________         
                try: 
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''
                    
                # _______________________________________    
                    
                    
                
                for k in xml_a["Файл"]["Документ"]["Таблица"]["СведТабл"]:
    
                    var_inside_doc_item_note = ""
        
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = sum_inside_doc
                    var_inside_doc_item_code = k["@КодПП"]
        
                    try:
                        var_inside_doc_item_article = re.findall(f"\d+\-\d+", k["@ИнфПолСтр"])[0]
                    except:
                        try:
                            var_inside_doc_item_article = re.findall(f"услуга|услуги", k["@ИнфПолСтр"])[0]
        
                        except:
                            var_inside_doc_item_article = ''
        
                    var_inside_doc_item_sn = ''
                        
                    var_inside_doc_item_name = k["@НаимПП"]
                    try:
                        var_inside_doc_item_quantity = k["@КолПП"]
                    except:
                        var_inside_doc_item_quantity = np.nan
    
                    var_inside_doc_item_unit = np.nan
                    try:
                        var_inside_doc_item_price = k["@Цена"]
                    except:
                        var_inside_doc_item_price = np.nan
                        
                    var_inside_doc_item_full_item_price = float(k["@Стоим"])       
                    
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_sn,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        var_agent,
                        var_date_from,
                        var_license_type,
                        ) 
    
                #     print("LIST")
                #     print("var_link:", var_link)
                #     print("var_doc_type:", var_doc_type)
                #     print("var_doc_number:", var_doc_number)
                #     print("var_doc_full_name:", var_doc_full_name)
                #     print("var_doc_data_main:", var_doc_data_main)
                #     print("var_doc_at_created:", var_doc_at_created)
                #     print("var_doc_counterparty_inn:", var_doc_counterparty_inn)
                #     print("var_doc_counterparty_full_name:", var_doc_counterparty_full_name)
                #     print("var_doc_provider_inn:", var_doc_provider_inn)
                #     print("var_doc_provider_full_name:", var_doc_provider_full_name)
                #     print("var_doc_assigned_manager:", var_doc_assigned_manager)
                #     print("var_doc_department:", var_doc_department)
    
                #     print("var_inside_doc_author:", var_inside_doc_author)
                #     print("var_inside_doc_type:", var_inside_doc_type)
                #     print("var_inside_doc_item_full_doc_price:", var_inside_doc_item_full_doc_price)
                #     print("var_inside_doc_item_note:", var_inside_doc_item_note)
                #     print("var_inside_doc_item_code:", var_inside_doc_item_code)
                #     print("var_inside_doc_item_article:", var_inside_doc_item_article)
                #     print("var_inside_doc_item_name:", var_inside_doc_item_name)
                #     print("var_inside_doc_item_quantity:", var_inside_doc_item_quantity)
                #     print("var_inside_doc_item_unit:", var_inside_doc_item_unit)
                #     print("var_inside_doc_item_price:", var_inside_doc_item_price)
                #     print("var_agent:", var_agent)
                #     print("var_date_from:", var_date_from)
                #     print("____________________________________")
                # print("________________________________________________________________________________")
    
    
        # _________________________________________________________________________________________________________________________________________________            
    
        # def_act_pp_print(var_inside_doc_author, var_inside_doc_type)
        def_act_pp_set_variable(var_inside_doc_author, var_inside_doc_type)
    
    def def_upd_dop(xml_a, var_inside_doc_author, var_inside_doc_type):
        
        # _________________________________________________________________________________________________________________________________________________    
        def def_upd_dop_print(var_inside_doc_author, var_inside_doc_type):
    
            if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:
            
                print(var_inside_doc_type)
                print("dict")
            
                var_inside_doc_item_note = ""
                
                try:
                    print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"])
                except:
                    print("var_inside_doc_item_quantity", np.nan)
            
                print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"])
                try:
                    print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"])
                except: 
                    print("var_inside_doc_item_price", np.nan)
                try:
                    print("var_inside_doc_item_article", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"])
                except: 
                    print("var_inside_doc_item_article", np.nan)
            
                print("var_inside_doc_item_code", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"])
                try:
                    print("var_inside_doc_item_unit", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"])
                except:
                    print("var_inside_doc_item_unit", np.nan)
            
                try:
                    print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                except:
                    print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"]))
    
    
            elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:
                                
                print(var_inside_doc_type)
                print("list")
            
                
                sum_inside_doc = 0
                            
                for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                    sum_inside_doc += float(k["@СтТовУчНал"])
                        
                for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:                    
                    
                    print("list")
                    
                    try:                
                        print("var_inside_doc_item_quantity", k["@КолТов"])
                    except:
                        print("var_inside_doc_item_quantity", np.nan)
                        
                    print("var_inside_doc_item_name", k["@НаимТов"])
                    try:
                        print("var_inside_doc_item_price", k["@ЦенаТов"])
                    except:
                        print("var_inside_doc_item_price", np.nan)
                    try:
                        print("var_inside_doc_item_article", k["ДопСведТов"]["@АртикулТов"])
                    except:
                        print("var_inside_doc_item_article", np.nan)
            
                    print("var_inside_doc_item_code", k["ДопСведТов"]["@КодТов"])
                    try:
                        print("var_inside_doc_item_unit", k["ДопСведТов"]["@НаимЕдИзм"])
                    except:
                        print("var_inside_doc_item_unit", np.nan)
                    try:
                        print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                    except: 
                        print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                    print("var_inside_doc_item_full_item_price", float(k["@СтТовУчНал"]))
        # _________________________________________________________________________________________________________________________________________________            
    
        def def_upd_dop_set_variable(var_inside_doc_author, var_inside_doc_type):
            
            if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:
                
                # print('dict')
                var_inside_doc_item_note = ""
                
                sum_inside_doc = 0
                try:
                    sum_inside_doc = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                except:
                    sum_inside_doc = np.nan
                    
                var_inside_doc_author = var_inside_doc_author
                var_inside_doc_type = var_inside_doc_type
                var_inside_doc_item_full_doc_price = sum_inside_doc
                var_inside_doc_item_code = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"]
                
                
                # var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@"]
            
            
                try:
                    var_inside_doc_item_article =  xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"]
                except: 
                    var_inside_doc_item_article = ''
                    
                var_inside_doc_item_sn = ''
                
                try:
                    for l in range(len(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"])): 
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"][l]["@Идентиф"] == 'SerialNumber':
                            var_inside_doc_item_sn = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"][l]["@Значен"]
                        else:
                            pass
                except:
                    var_inside_doc_item_sn = ''                
            
            
            
            
            
                    
                var_inside_doc_item_name = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"]
                try:
                    var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"]
                except: 
                    var_inside_doc_item_quantity = np.nan
                try:
                    var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"]
                except:
                    var_inside_doc_item_unit = np.nan
                try:
                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"]
                except: 
                    var_inside_doc_item_price = np.nan
                    
                try:
                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                except:
                    var_inside_doc_item_full_item_price = np.nan
                
                
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_agent in str_agent:
                        if v_agent["@Идентиф"].lower() == 'агент':
                            try:
                                var_agent = v_agent["@Значен"][2:-2].replace("&quot;", "")
                            except:    
                                var_agent = v_agent["@Значен"][2:-2]
                        else:
                            pass
                except:
                    var_agent = ''
                
                # Дата С_________________________________
                
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_date_from in str_date_from:
                        if v_date_from["@Идентиф"].lower() == 'дата' or v_date_from["@Идентиф"].lower() == 'датас':
                            var_date_from = v_date_from["@Значен"]
                        else:
                            pass
                except:
                    var_date_from = ''                    
                
                #_________________________________         
                 
                # var_license_type = ''
                # for var_i_license_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                #     try:
                #         if var_i_license_type["@Идентиф"] == 'ХарактНоменклатуры':
                #             var_license_type = var_i_license_type["@Значен"]
                #         else:
                #             pass
                #     except:
                #         var_license_type = ''       
    
                try:
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''
    
        
    
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_sn,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,
                    var_agent,
                    var_date_from,
                    var_license_type,
                    ) 
    
                # print("DICT")
                # print("var_link:", var_link)
                # print("var_doc_type:", var_doc_type)
                # print("var_doc_number:", var_doc_number)
                # print("var_doc_full_name:", var_doc_full_name)
                # print("var_doc_data_main:", var_doc_data_main)
                # print("var_doc_at_created:", var_doc_at_created)
                # print("var_doc_counterparty_inn:", var_doc_counterparty_inn)
                # print("var_doc_counterparty_full_name:", var_doc_counterparty_full_name)
                # print("var_doc_provider_inn:", var_doc_provider_inn)
                # print("var_doc_provider_full_name:", var_doc_provider_full_name)
                # print("var_doc_assigned_manager:", var_doc_assigned_manager)
                # print("var_doc_department:", var_doc_department)
                
                # print("var_inside_doc_author:", var_inside_doc_author)
                # print("var_inside_doc_type:", var_inside_doc_type)
                # print("var_inside_doc_item_full_doc_price:", var_inside_doc_item_full_doc_price)
                # print("var_inside_doc_item_note:", var_inside_doc_item_note)
                # print("var_inside_doc_item_code:", var_inside_doc_item_code)
                # print("var_inside_doc_item_article:", var_inside_doc_item_article)
                # print("var_inside_doc_item_name:", var_inside_doc_item_name)
                # print("var_inside_doc_item_quantity:", var_inside_doc_item_quantity)
                # print("var_inside_doc_item_unit:", var_inside_doc_item_unit)
                # print("var_inside_doc_item_price:", var_inside_doc_item_price)
                # print("var_agent:", var_agent)
                # print("var_date_from:", var_date_from)
                # print("________________________________________________________________________________")    
    
            elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:
    
                # print('list')
                
                sum_inside_doc = 0
                            
                try:
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                        sum_inside_doc += float(k["@СтТовУчНал"])
                except:
                    sum_inside_doc += 0
                    
                    
                
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_agent in str_agent:
                        if v_agent["@Идентиф"].lower() == 'агент':
                            try:
                                var_agent = v_agent["@Значен"][2:-2].replace("&quot;", "")
                            except:    
                                var_agent = v_agent["@Значен"][2:-2]
                        else:
                            pass
                except:
                    var_agent = ''
                
                # Дата С_________________________________
                
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_date_from in str_date_from:
                        if v_date_from["@Идентиф"].lower() == 'дата' or v_date_from["@Идентиф"].lower() == 'датас':
                            var_date_from = v_date_from["@Значен"]
                        else:
                            pass
                except:
                    var_date_from = ''
                
                #_________________________________      


                try:
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''

                
                                                            
    
                        
                for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:                     
                
                
                    var_inside_doc_item_note = ""
                
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = sum_inside_doc
                    var_inside_doc_item_code = k["ДопСведТов"]["@КодТов"]
    
                    # __________________________________________
    
               

                    
                    try:
                        var_inside_doc_item_article =  k["ДопСведТов"]["@АртикулТов"]
                    except:
                        var_inside_doc_item_article =  np.nan
                        
                        
                    var_inside_doc_item_sn = ''     
                        
                    try:
                        for l in range(len(k["ИнфПолФХЖ2"])):
                            if k["ИнфПолФХЖ2"][l]["@Идентиф"] == 'SerialNumber':
                                var_inside_doc_item_sn = k["ИнфПолФХЖ2"][l]["@Значен"]
                            else: 
                                pass
                    except:
                        var_inside_doc_item_sn = ''        
                
                        
                    var_inside_doc_item_name = k["@НаимТов"]
                    
                    try:
                        var_inside_doc_item_quantity = k["@КолТов"]
                    except:
                        var_inside_doc_item_quantity = np.nan
            
                    try:
                        var_inside_doc_item_unit = k["ДопСведТов"]["@НаимЕдИзм"]
                    except:
                        var_inside_doc_item_unit = np.nan
                    try:
                        var_inside_doc_item_price = k["@ЦенаТов"]
                    except:
                        var_inside_doc_item_price = np.nan
    
                    try:
                        var_inside_doc_item_full_item_price = float(k["@СтТовУчНал"])
                    except: 
                        var_inside_doc_item_full_item_price = np.nan
                        
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_sn,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        var_agent,
                        var_date_from,
                        var_license_type,
                        )    
    
                #     print("LIST")
                #     print("var_link:", var_link)
                #     print("var_doc_type:", var_doc_type)
                #     print("var_doc_number:", var_doc_number)
                #     print("var_doc_full_name:", var_doc_full_name)
                #     print("var_doc_data_main:", var_doc_data_main)
                #     print("var_doc_at_created:", var_doc_at_created)
                #     print("var_doc_counterparty_inn:", var_doc_counterparty_inn)
                #     print("var_doc_counterparty_full_name:", var_doc_counterparty_full_name)
                #     print("var_doc_provider_inn:", var_doc_provider_inn)
                #     print("var_doc_provider_full_name:", var_doc_provider_full_name)
                #     print("var_doc_assigned_manager:", var_doc_assigned_manager)
                #     print("var_doc_department:", var_doc_department)
    
                #     print("var_inside_doc_author:", var_inside_doc_author)
                #     print("var_inside_doc_type:", var_inside_doc_type)
                #     print("var_inside_doc_item_full_doc_price:", var_inside_doc_item_full_doc_price)
                #     print("var_inside_doc_item_note:", var_inside_doc_item_note)
                #     print("var_inside_doc_item_code:", var_inside_doc_item_code)
                #     print("var_inside_doc_item_article:", var_inside_doc_item_article)
                #     print("var_inside_doc_item_name:", var_inside_doc_item_name)
                #     print("var_inside_doc_item_quantity:", var_inside_doc_item_quantity)
                #     print("var_inside_doc_item_unit:", var_inside_doc_item_unit)
                #     print("var_inside_doc_item_price:", var_inside_doc_item_price)
                #     print("var_agent:", var_agent)
                #     print("var_date_from:", var_date_from)
                #     print("____________________________________")
                # print("________________________________________________________________________________")
    
    
    
        # _________________________________________________________________________________________________________________________________________________            
    
        # def_upd_dop_print(var_inside_doc_author, var_inside_doc_type)
        def_upd_dop_set_variable(var_inside_doc_author, var_inside_doc_type)
    
    def def_upd_s_dop(xml_a, var_inside_doc_author, var_inside_doc_type):
        
        # _________________________________________________________________________________________________________________________________________________    
        def def_upd_s_dop_print(var_inside_doc_author, var_inside_doc_type):
    
            if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:
    
                try:
                    print("var_inside_doc_item_quantity", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"])
                except: 
                    print("var_inside_doc_item_quantity", np.nan)
                print("var_inside_doc_item_name", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"])
                
                try:
                    print("var_inside_doc_item_price", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"])
                except:
                    print("var_inside_doc_item_price", np.nan)
                
                try:
                    print("var_inside_doc_item_article", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"])
                except:
                    print("var_inside_doc_item_article", np.nan)
                
                print("var_inside_doc_item_code", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"])
                
                try:
                    print("var_inside_doc_item_unit", xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"])
                except:
                    print("var_inside_doc_item_unit", np.nan)
            
                try:
                    print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                except:
                    print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"]))
                
                sum_inside_doc = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                print("var_inside_doc_item_full_item_price", sum_inside_doc)
    
    
            elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:
                                
                print(var_inside_doc_type)
                print("list")
                                
                sum_inside_doc = 0
                
                try:
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                        sum_inside_doc += float(k["@СтТовУчНал"])
                except:
                    sum_inside_doc += 0
                                                            
                for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:                    
            
                    print("list")
                    try:
                        print("var_inside_doc_item_quantity",k["@КолТов"])
                    except: 
                        print("var_inside_doc_item_quantity",np.nan)
                    print("var_inside_doc_item_name", k["@НаимТов"])
                    try:
                        print("var_inside_doc_item_price", k["@ЦенаТов"])
                    except:
                        print("var_inside_doc_item_price", np.nan)
                    try:
                        print("var_inside_doc_item_article", k["ДопСведТов"]["@АртикулТов"])
                    except: 
                        print("var_inside_doc_item_article", np.nan)
                    print("var_inside_doc_item_code", k["ДопСведТов"]["@КодТов"])
                    try:
                        print("var_inside_doc_item_unit", k["ДопСведТов"]["@НаимЕдИзм"])
                    except:
                        print("var_inside_doc_item_unit", np.nan)
            
                    try: 
                        print("var_inside_doc_item_full_doc_price", float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["ВсегоОпл"]["@СтТовУчНалВсего"]))
                    except: 
                        print("var_inside_doc_item_full_doc_price", sum_inside_doc)
                    print("var_inside_doc_item_full_item_price", float(k["@СтТовУчНал"]))
                        
        # _________________________________________________________________________________________________________________________________________________            
    
        def def_upd_s_dop_set_variable(var_inside_doc_author, var_inside_doc_type, dict_total_sell, dict_total_payment):
            
            if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == dict:
                
                var_inside_doc_item_note = ""
    
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_agent in str_agent:
                        if v_agent["@Идентиф"].lower() == 'агент':
                                try:
                                    var_agent = v_agent["@Значен"][2:-2].replace("&quot;", "")
                                except:    
                                    var_agent = v_agent["@Значен"][2:-2]
                        else:
                            pass
                except:
                    var_agent = ''
                
                # Дата С_________________________________
                
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_date_from in str_date_from:
                        if v_date_from["@Идентиф"].lower() == 'дата' or v_date_from["@Идентиф"].lower() == 'датас':
                            var_date_from = v_date_from["@Значен"]
                        else:
                            pass
                except:
                    var_date_from = ''
    
                # _______________________________________
                try: 
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
        
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''                    
                    
                # _______________________________________
                
                var_inside_doc_author = var_inside_doc_author
                dict_total_sell = dict_total_sell 
                dict_total_payment = dict_total_payment 
                var_inside_doc_type = var_inside_doc_type

                
                
                try:
                    var_inside_doc_item_full_doc_price = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                except: 
                    var_inside_doc_item_full_doc_price = np.nan
                    
                var_inside_doc_item_code = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@КодТов"]
            
                try:
                    var_inside_doc_item_article = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@АртикулТов"]
                except:
                    var_inside_doc_item_article = ''
                    
                var_inside_doc_item_sn = ''
                 
                try:
                    for l in range(len(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"])): 
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"][l]["@Идентиф"] == 'SerialNumber':
                            var_inside_doc_item_sn = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"][l]["@Значен"]
                        else:
                            pass
                except:
                    var_inside_doc_item_sn = ''     
                    
                    
                    
                                        
                var_inside_doc_item_name = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@НаимТов"]
                
                try:
                    var_inside_doc_item_quantity = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@КолТов"]
                except:
                    var_inside_doc_item_quantity = np.nan
                
                
                try:
                    var_inside_doc_item_unit = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ДопСведТов"]["@НаимЕдИзм"]
                except: 
                    var_inside_doc_item_unit = np.nan
            
                try:
                    var_inside_doc_item_price = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@ЦенаТов"]
                except:
                    var_inside_doc_item_price = np.nan
                
                try:
                    var_inside_doc_item_full_item_price = float(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["@СтТовУчНал"])
                except:
                    var_inside_doc_item_full_item_price = np.nan
                
                doc_append()
                inside_doc_append(var_inside_doc_author,
                    dict_total_sell,
                    dict_total_payment,
                    var_inside_doc_type,
                    var_inside_doc_item_full_doc_price,
                    var_inside_doc_item_note,
                    var_inside_doc_item_code,
                    var_inside_doc_item_article,
                    var_inside_doc_item_sn,
                    var_inside_doc_item_name,
                    var_inside_doc_item_quantity,
                    var_inside_doc_item_unit,
                    var_inside_doc_item_price,
                    var_inside_doc_item_full_item_price,
                    )    
    
                # print("DICT")
                # print("var_link:", var_link)
                # print("var_doc_type:", var_doc_type)
                # print("var_doc_number:", var_doc_number)
                # print("var_doc_full_name:", var_doc_full_name)
                # print("var_doc_data_main:", var_doc_data_main)
                # print("var_doc_at_created:", var_doc_at_created)
                # print("var_doc_counterparty_inn:", var_doc_counterparty_inn)
                # print("var_doc_counterparty_full_name:", var_doc_counterparty_full_name)
                # print("var_doc_provider_inn:", var_doc_provider_inn)
                # print("var_doc_provider_full_name:", var_doc_provider_full_name)
                # print("var_doc_assigned_manager:", var_doc_assigned_manager)
                # print("var_doc_department:", var_doc_department)
                
                # print("var_inside_doc_author:", var_inside_doc_author)
                # print("var_inside_doc_type:", var_inside_doc_type)
                # print("var_inside_doc_item_full_doc_price:", var_inside_doc_item_full_doc_price)
                # print("var_inside_doc_item_note:", var_inside_doc_item_note)
                # print("var_inside_doc_item_code:", var_inside_doc_item_code)
                # print("var_inside_doc_item_article:", var_inside_doc_item_article)
                # print("var_inside_doc_item_name:", var_inside_doc_item_name)
                # print("var_inside_doc_item_quantity:", var_inside_doc_item_quantity)
                # print("var_inside_doc_item_unit:", var_inside_doc_item_unit)
                # print("var_inside_doc_item_price:", var_inside_doc_item_price)
                # print("var_agent:", var_agent)
                # print("var_date_from:", var_date_from)
                # print("________________________________________________________________________________")
            
            elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]) == list:
    
                sum_inside_doc = 0    
    
                try:
                    for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:
                        sum_inside_doc += float(k["@СтТовУчНал"])
                except:
                    sum_inside_doc = np.nan
                    
                    
                var_agent = ''
                try:
                    str_agent = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_agent in str_agent:
                        if v_agent["@Идентиф"].lower() == 'агент':
                                try:
                                    var_agent = v_agent["@Значен"][2:-2].replace("&quot;", "")
                                except:    
                                    var_agent = v_agent["@Значен"][2:-2]
                        else:
                            pass
                except:
                    var_agent = ''
                
                # Дата С_________________________________
                
                var_date_from = ''
                try:
                    str_date_from = xml_a["Файл"]["Документ"]["СвСчФакт"]["ИнфПолФХЖ1"]["ТекстИнф"]
                    for v_date_from in str_date_from:
                        if v_date_from["@Идентиф"].lower() == 'дата' or v_date_from["@Идентиф"].lower() == 'датас':
                            var_date_from = v_date_from["@Значен"]
                        else:
                            pass
                except:
                    var_date_from = ''                        
                    
                # _______________________________________
                try: 
                    if type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == dict:
                        if xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Идентиф"] == 'ХарактНоменклатуры':
                            var_license_type = xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]["@Значен"]
                        else:
                            var_license_type = ''
                    elif type(xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]) == list:
                        for i_lic_type in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]["ИнфПолФХЖ2"]:
                            if i_lic_type["@Идентиф"] == 'ХарактНоменклатуры':
                                var_license_type = i_lic_type["@Значен"]
                    
                            else:
                                var_license_type = ''
                except:
                    var_license_type = ''                    
                    
                # _______________________________________
               
                    
                                                            
                for k in xml_a["Файл"]["Документ"]["ТаблСчФакт"]["СведТов"]:  
                
                    var_inside_doc_item_note = ""
            
                    var_inside_doc_author = var_inside_doc_author
                    dict_total_sell = dict_total_sell
                    dict_total_payment = dict_total_payment
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price = sum_inside_doc
                    var_inside_doc_item_code = k["ДопСведТов"]["@КодТов"]
            
            
                    try:
                        var_inside_doc_item_article =  k["ДопСведТов"]["@АртикулТов"]
                    except:
                        var_inside_doc_item_article =  np.nan
                        
                        
                    var_inside_doc_item_sn = ''
                    
                    try:
                        for l in range(len(k["ИнфПолФХЖ2"])):
                            if k["ИнфПолФХЖ2"][l]["@Идентиф"] == 'SerialNumber':
                                var_inside_doc_item_sn = k["ИнфПолФХЖ2"][l]["@Значен"]
                            else: 
                                pass
                    except:
                        var_inside_doc_item_sn = ''     
            
                        
                    var_inside_doc_item_name = k["@НаимТов"]
                    
                    try:
                        var_inside_doc_item_quantity = k["@КолТов"]
                    except:
                        var_inside_doc_item_quantity = np.nan
                        
                    try:
                        var_inside_doc_item_unit = k["ДопСведТов"]["@НаимЕдИзм"]
                    except:
                        var_inside_doc_item_unit = np.nan
                     
                    try:       
                        var_inside_doc_item_price = k["@ЦенаТов"]
                    except:
                        var_inside_doc_item_price = np.nan
                        
                    try:
                        var_inside_doc_item_full_item_price = float(k["@СтТовУчНал"])
                    except:
                        var_inside_doc_item_full_item_price = np.nan
                
                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                        dict_total_sell,
                        dict_total_payment,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_sn,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity,
                        var_inside_doc_item_unit,
                        var_inside_doc_item_price,
                        var_inside_doc_item_full_item_price,
                        )
                    
                #     print("LIST")
                #     print("var_link:", var_link)
                #     print("var_doc_type:", var_doc_type)
                #     print("var_doc_number:", var_doc_number)
                #     print("var_doc_full_name:", var_doc_full_name)
                #     print("var_doc_data_main:", var_doc_data_main)
                #     print("var_doc_at_created:", var_doc_at_created)
                #     print("var_doc_counterparty_inn:", var_doc_counterparty_inn)
                #     print("var_doc_counterparty_full_name:", var_doc_counterparty_full_name)
                #     print("var_doc_provider_inn:", var_doc_provider_inn)
                #     print("var_doc_provider_full_name:", var_doc_provider_full_name)
                #     print("var_doc_assigned_manager:", var_doc_assigned_manager)
                #     print("var_doc_department:", var_doc_department)
    
                #     print("var_inside_doc_author:", var_inside_doc_author)
                #     print("var_inside_doc_type:", var_inside_doc_type)
                #     print("var_inside_doc_item_full_doc_price:", var_inside_doc_item_full_doc_price)
                #     print("var_inside_doc_item_note:", var_inside_doc_item_note)
                #     print("var_inside_doc_item_code:", var_inside_doc_item_code)
                #     print("var_inside_doc_item_article:", var_inside_doc_item_article)
                #     print("var_inside_doc_item_name:", var_inside_doc_item_name)
                #     print("var_inside_doc_item_quantity:", var_inside_doc_item_quantity)
                #     print("var_inside_doc_item_unit:", var_inside_doc_item_unit)
                #     print("var_inside_doc_item_price:", var_inside_doc_item_price)
                #     print("var_agent:", var_agent)
                #     print("var_date_from:", var_date_from)
                #     print("____________________________________")
                # print("________________________________________________________________________________")
        # _________________________________________________________________________________________________________________________________________________            
    
        # def_upd_s_dop_print(var_inside_doc_author, var_inside_doc_type)
        def_upd_s_dop_set_variable(var_inside_doc_author, var_inside_doc_type, dict_total_sell, dict_total_payment)
            
            
    url = url_sbis

    method = "СБИС.Аутентифицировать"
    params = {
        "Параметр": {
            "Логин": API_sbis,
            "Пароль": API_sbis_pass
        }

    }
    parameters = {
    "jsonrpc": "2.0",
    "method": method,
    "params": params,
    "id": 0
    }

    response = requests.post(url, json=parameters)
    response.encoding = 'utf-8'

    str_to_dict = json.loads(response.text)
    access_token = str_to_dict["result"]
    # print("access_token:", access_token)

    headers = {
    "X-SBISSessionID": access_token,
    "Content-Type": "application/json",
    }  

    # _____________________________________________________________
    doc_id = []
    doc_type = []
    doc_number = []
    doc_full_name = []
    doc_data_main = []
    doc_at_created = []

    doc_counterparty_inn = []
    doc_counterparty_full_name = []

    doc_provider_inn = []
    doc_provider_full_name = []

    doc_assigned_manager = []
    doc_department = []
    doc_notation = []

    inside_doc_author = []
    inside_dict_total_sell = []
    inside_dict_total_payment = []
    inside_doc_type = []
    inside_doc_item_full_doc_price = []

    inside_doc_item_note = []

    inside_doc_item_code = []
    inside_doc_item_article = []
    inside_dict_sell = []
    inside_dict_payment = []
    inside_doc_item_name = []

    inside_doc_item_quantity = []
    inside_doc_item_unit = []

    inside_doc_item_price = []
    inside_doc_item_full_item_price = []
    
   
    # ___________________________________________________________________________________________
    doc_id_exc = []
    doc_type_exc = []
    doc_number_exc = []
    doc_full_name_exc = []
    doc_data_main_exc = []
    doc_at_created_exc = []

    doc_counterparty_inn_exc = []
    doc_counterparty_full_name_exc = []

    doc_provider_inn_exc = []
    doc_provider_full_name_exc = []

    doc_assigned_manager_exc = []
    doc_department_exc = []
    # ___________________________________________________________________________________________

    var_status_has_more = "Да"
    i_page = 0

    while var_status_has_more == "Да":
        
        parameters_real = {
        "jsonrpc": "2.0",
        "method": "СБИС.СписокДокументов",
        "params": {
            "Фильтр": {
            "ДатаС": date_from,
            "ДатаПо": date_to,
            "Тип": "СчетИсх",
            "Регламент": {
                "Название": "Счет"
            },
            "Навигация": {
                "Страница": i_page
            }
            }
        },
        "id": 0
        }
        
        url_real = url_sbis_unloading
        
        response_points = requests.post(url_real, json=parameters_real, headers=headers)

        str_to_dict_points_main = json.loads(response_points.text)
        
        json_data_points = json.dumps(str_to_dict_points_main, ensure_ascii=False, indent=4).encode("utf8").decode()
        
        # with open("DICT_REALIZE.json", 'w') as json_file_points_o:
        #     json_file_points_o.write(json_data_points)
        
        j = 0
        for i in str_to_dict_points_main["result"]["Документ"]:
            # print(j)
            j += 1
            # if (re.findall("реал", i["Регламент"]["Название"].lower())[-1] == "счет") and (i["Расширение"]["Проведен"].lower() == 'да'):
            if (re.findall("счет", i["Регламент"]["Название"].lower())[-1] == "счет"):
    # ___________________________________________________________________________________________
                
                try:
                    var_link = i["Идентификатор"]
                except:
                    var_link = np.nan
                    
                # print("var_link:", var_link)


                try:
                    doc_manager_first_name = str(i["Ответственный"]["Имя"])
                except:
                    doc_manager_first_name = ""
                try:
                    doc_manager_last_name = str(i["Ответственный"]["Фамилия"])
                except:
                    doc_manager_last_name = ""
                try:
                    doc_manager_surname_name = str(i["Ответственный"]["Отчество"])
                except:
                    doc_manager_surname_name = ""

                try:
                    doc_manager_name = " ".join([doc_manager_last_name, doc_manager_first_name, doc_manager_surname_name])
                except:
                    doc_manager_name = np.nan

                try:
                    var_doc_type = i["Регламент"]["Название"]
                except:
                    var_doc_type = np.nan

                try:
                    var_doc_number = i["Номер"] 
                except:
                    var_doc_number = np.nan

                try:
                    var_doc_full_name = i["Название"]
                except:
                    var_doc_full_name = np.nan

                try:
                    var_doc_data_main = i["Дата"]
                except:
                    var_doc_data_main = np.nan

                try:
                    var_doc_at_created = i["ДатаВремяСоздания"]
                except:
                    var_doc_at_created = np.nan
                try:
                    try:
                        var_doc_counterparty_inn = i["Контрагент"]["СвФЛ"]["ИНН"]
                        var_doc_counterparty_full_name = i["Контрагент"]["СвФЛ"]["НазваниеПолное"]
                    except:
                        var_doc_counterparty_inn = i["Контрагент"]["СвЮЛ"]["ИНН"]
                        var_doc_counterparty_full_name = i["Контрагент"]["СвЮЛ"]["НазваниеПолное"]
                except:
                    var_doc_counterparty_inn = np.nan
                    var_doc_counterparty_full_name = np.nan
                try:
                    try:
                        var_doc_provider_inn = i["НашаОрганизация"]["СвФЛ"]["ИНН"]
                        var_doc_provider_full_name = i["НашаОрганизация"]["СвФЛ"]["НазваниеПолное"]
                    except:
                        var_doc_provider_inn = i["НашаОрганизация"]["СвЮЛ"]["ИНН"]
                        var_doc_provider_full_name = i["НашаОрганизация"]["СвЮЛ"]["НазваниеПолное"]
                except:
                    var_doc_provider_inn = np.nan
                    var_doc_provider_full_name = np.nan

                try:
                    var_doc_assigned_manager = doc_manager_name
                except:
                    var_doc_assigned_manager = np.nan
                try:
                    var_doc_department = i["Подразделение"]["Название"]
                except:
                    var_doc_department = np.nan                  
                
    # ___________________________________________________________________________________________
    # ___________________________________________________________________________________________
    # ___________________________________________________________________________________________
                try:
                    var_doc_notation = i["Примечание"]
                except:
                    var_doc_notation = ''      
                    
                
    # ___________________________________________________________________________________________
    # ___________________________________________________________________________________________
    # ___________________________________________________________________________________________

                parameters_real = {
                "jsonrpc": "2.0",
                "method": "СБИС.ПрочитатьДокумент",
                "params": {
                    "Документ": {
                        "Идентификатор": var_link,
                        "ДопПоля": "ДополнительныеПоля"
                    }
                },
                "id": 0
                }
            
                url_real = url_sbis_unloading
            
                response_points = requests.post(url_real, json=parameters_real, headers=headers)
                # print(response_points)
                # print(headers)
                str_to_dict_points = json.loads(response_points.text)
    # ___________________________________________________________________________________________
                
                # author_list = [str_to_dict_points["result"]["Автор"]["Имя"], str_to_dict_points["result"]["Автор"]["Фамилия"], str_to_dict_points["result"]["Автор"]["Отчество"]]
                try:
                    name = str_to_dict_points["result"]["Автор"]["Имя"]
                except:
                    name = ""
                try:
                    second_name = str_to_dict_points["result"]["Автор"]["Фамилия"]
                except:
                    second_name = ""
                try:
                    surname_name = str_to_dict_points["result"]["Автор"]["Отчество"]
                except:
                    surname_name = ""
            
                author_list = [name, second_name, surname_name]
                
                # print("автор:", " ".join(author_list).strip())
                try:
                    var_inside_doc_author = " ".join(author_list).strip()
                except:
                    var_inside_doc_author = np.nan

                
                
    # ___________________________________________________________________________________________
    # ___________________________________________________________________________________________
    # ___________________________________________________________________________________________

                dict_total_sell = {}
                dict_total_payment = {}
                
                try:
                    try_temp = str_to_dict_points["result"]["ДокументСледствие"]
                
                    for m in range(len(str_to_dict_points["result"]["ДокументСледствие"])):
                        # var_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["ВидСвязи"]
                        # print(var_conn)
                    
                        var_temp =  str_to_dict_points["result"]["ДокументСледствие"][m]["ВидСвязи"].lower()
                        var_temp_origin =  str_to_dict_points["result"]["ДокументСледствие"][m]["ВидСвязи"]
                        
                        if "отгрузка" in var_temp:
                    
                            temp_for_dict = {}
                            
                            if type(str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]) == dict:
                                var_type_of_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["ВидСвязи"]
                                var_date_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]["Дата"]
                                var_id_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]["Идентификатор"]
                                var_number_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]["Номер"]
                                var_type_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]["Тип"]
                                var_sum_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Сумма"]
                                # print(var_type_of_conn, var_date_conn, var_id_conn, var_number_conn, var_type_conn, var_sum_conn)
                                # print('______________________________________________')
                    
                                # lst_var_type_of_conn.append(var_type_of_conn)
                                # lst_var_date_conn.append(var_date_conn)
                                # lst_var_id_conn.append(var_id_conn)
                                # lst_var_number_conn.append(var_number_conn)
                                # lst_var_type_conn.append(var_type_conn)
                                # lst_var_sum_conn.append(var_sum_conn)
                    
                                temp_for_dict["ВидСвязи"] = var_type_of_conn
                                temp_for_dict["Дата"] = var_date_conn
                                temp_for_dict["Идентификатор"] = var_id_conn
                                temp_for_dict["Номер"] = var_number_conn
                                temp_for_dict["Тип"] = var_type_conn
                                temp_for_dict["Сумма"] = var_sum_conn
                    
                                dict_total_sell[f"{var_temp_origin}"] = temp_for_dict
                    
                            
                            elif type(str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]) == list:
                    
                                temp_for_list = []
                                temp_dict_for_list = {}
                                for mn in range(len(str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"])):
                                    var_type_of_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["ВидСвязи"]
                                    var_date_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"][mn]["Дата"]
                                    var_id_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"][mn]["Идентификатор"]
                                    var_number_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"][mn]["Номер"]
                                    var_type_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"][mn]["Тип"]
                                    var_sum_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Сумма"]
                                    # print(var_type_of_conn, var_date_conn, var_id_conn, var_number_conn, var_type_conn, var_sum_conn)
                                    # print('______________________________________________')
                    
                                    # lst_var_type_of_conn.append(var_type_of_conn)
                                    # lst_var_date_conn.append(var_date_conn)
                                    # lst_var_id_conn.append(var_id_conn)
                                    # lst_var_number_conn.append(var_number_conn)
                                    # lst_var_type_conn.append(var_type_conn)
                                    # lst_var_sum_conn.append(var_sum_conn)
                    
                                    temp_dict_for_list["ВидСвязи"] = var_type_of_conn
                                    temp_dict_for_list["Дата"] = var_date_conn
                                    temp_dict_for_list["Идентификатор"] = var_id_conn
                                    temp_dict_for_list["Номер"] = var_number_conn
                                    temp_dict_for_list["Тип"] = var_type_conn
                                    temp_dict_for_list["Сумма"] = var_sum_conn
                    
                            
                                    temp_for_list.append(temp_dict_for_list)
                                    
                            
                                
                                dict_total_sell[f"{var_temp_origin}"] = temp_for_list
                        
                        elif "оплата" in var_temp:
                    
                            temp_for_dict = {}
                            
                            if type(str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]) == dict:
                                var_type_of_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["ВидСвязи"]
                                var_date_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]["Дата"]
                                var_id_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]["Идентификатор"]
                                var_number_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]["Номер"]
                                var_type_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]["Тип"]
                                var_sum_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Сумма"]
                                # print(var_type_of_conn, var_date_conn, var_id_conn, var_number_conn, var_type_conn, var_sum_conn)
                                # print('______________________________________________')
                    
                                # lst_var_type_of_conn.append(var_type_of_conn)
                                # lst_var_date_conn.append(var_date_conn)
                                # lst_var_id_conn.append(var_id_conn)
                                # lst_var_number_conn.append(var_number_conn)
                                # lst_var_type_conn.append(var_type_conn)
                                # lst_var_sum_conn.append(var_sum_conn)
                    
                                temp_for_dict["ВидСвязи"] = var_type_of_conn
                                temp_for_dict["Дата"] = var_date_conn
                                temp_for_dict["Идентификатор"] = var_id_conn
                                temp_for_dict["Номер"] = var_number_conn
                                temp_for_dict["Тип"] = var_type_conn
                                temp_for_dict["Сумма"] = var_sum_conn
                    
                                dict_total_payment[f"{var_temp_origin}"] = temp_for_dict
                    
                            
                            elif type(str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"]) == list:
                    
                                temp_for_list = []
                                temp_dict_for_list = {}
                                for mn in range(len(str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"])):
                                    var_type_of_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["ВидСвязи"]
                                    var_date_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"][mn]["Дата"]
                                    var_id_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"][mn]["Идентификатор"]
                                    var_number_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"][mn]["Номер"]
                                    var_type_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Документ"][mn]["Тип"]
                                    var_sum_conn = str_to_dict_points["result"]["ДокументСледствие"][m]["Сумма"]
                                    # print(var_type_of_conn, var_date_conn, var_id_conn, var_number_conn, var_type_conn, var_sum_conn)
                                    # print('______________________________________________')
                    
                                    # lst_var_type_of_conn.append(var_type_of_conn)
                                    # lst_var_date_conn.append(var_date_conn)
                                    # lst_var_id_conn.append(var_id_conn)
                                    # lst_var_number_conn.append(var_number_conn)
                                    # lst_var_type_conn.append(var_type_conn)
                                    # lst_var_sum_conn.append(var_sum_conn)
                    
                                    temp_dict_for_list["ВидСвязи"] = var_type_of_conn
                                    temp_dict_for_list["Дата"] = var_date_conn
                                    temp_dict_for_list["Идентификатор"] = var_id_conn
                                    temp_dict_for_list["Номер"] = var_number_conn
                                    temp_dict_for_list["Тип"] = var_type_conn
                                    temp_dict_for_list["Сумма"] = var_sum_conn
                    
                            
                                    temp_for_list.append(temp_dict_for_list)
                                
                                dict_total_payment[f"{var_temp_origin}"] = temp_for_list
                            else:
                                pass
                except:
                    pass
                        
                
                var_inside_dict_total_sell = str(dict_total_sell)
                var_inside_dict_total_payment = str(dict_total_payment)
                
                # print(var_dict_total_sell)
                # print(var_dict_total_payment)

                
    # ___________________________________________________________________________________________
    # ___________________________________________________________________________________________
    # ___________________________________________________________________________________________

           
                def common_part_print():
                    print(j)
                    try:
                        print("var_link", var_link)
                    except:
                        print("var_link", np.nan)
                    try:
                        print("var_doc_type", i["Регламент"]["Название"])
                    except:
                        print("var_doc_type", np.nan)
                    try:
                        print("var_doc_number", i["Номер"])
                    except:
                        print("var_doc_number", np.nan)
                    try:
                        print("var_doc_full_name", i["Название"])
                    except:
                        print("var_doc_full_name", np.nan)

                    try:
                        print("var_doc_data_main", i["Дата"])
                    except:
                        print("var_doc_data_main", np.nan)
                    try:
                        print("var_doc_at_created", i["ДатаВремяСоздания"])
                    except:
                        print("var_doc_at_created", np.nan)
                    
                    try:
                        try:
                            print("var_doc_counterparty_inn", i["Контрагент"]["СвФЛ"]["ИНН"])
                            print("var_doc_counterparty_full_name", i["Контрагент"]["СвФЛ"]["НазваниеПолное"])
                        except:
                            print("var_doc_counterparty_inn", i["Контрагент"]["СвЮЛ"]["ИНН"])
                            print("var_doc_counterparty_full_name", i["Контрагент"]["СвЮЛ"]["НазваниеПолное"])  
                    except:
                        print("var_doc_counterparty_inn", np.nan)
                        print("var_doc_counterparty_full_name", np.nan)                          

                    try:
                        try:
                            print("var_doc_provider_inn", i["НашаОрганизация"]["СвФЛ"]["ИНН"])
                            print("var_doc_provider_full_name", i["НашаОрганизация"]["СвФЛ"]["НазваниеПолное"])
                        except:
                            print("var_doc_provider_inn", i["НашаОрганизация"]["СвЮЛ"]["ИНН"])
                            print("var_doc_provider_full_name", i["НашаОрганизация"]["СвЮЛ"]["НазваниеПолное"])
                    except:
                        print("var_doc_provider_inn", np.nan)
                        print("var_doc_provider_full_name", np.nan)                        
                
                    print("var_doc_assigned_manager", doc_manager_name)
                    try:
                        print("var_doc_department", i["Подразделение"]["Название"])
                    except:
                        print("var_doc_department", np.nan)

                    try:
                        print("автор:", " ".join(author_list).strip())
                    except:
                        print("автор:", np.nan)
                    
                # common_part_print()

    # ___________________________________________________________________________________________
                
                
                
                try:
                    attachments_id = {}
                    try:
                        for l in range(len(str_to_dict_points["result"]["ВложениеУчета"])):
                            # if str_to_dict_points["result"]["ВложениеУчета"][l]["Тип"].lower() in ("актпп", "актвр", "эдонакл", "упддоп", "упдсчфдоп"):
                            if str_to_dict_points["result"]["ВложениеУчета"][l]["Тип"].lower() in ("эдосч"):
                                # print(str_to_dict_points["result"]["ВложениеУчета"][l]["Тип"].lower())
                                attachments_id[str_to_dict_points["result"]["ВложениеУчета"][l]["Тип"].lower()] = str_to_dict_points["result"]["ВложениеУчета"][l]["Файл"]["Ссылка"]
                            else:
                                pass
                    except:
                        pass
                    
                    try:
                        link_xml = list(attachments_id.values())[0]
                        a_xml = requests.get(link_xml, headers=headers)
                        a_xml.encoding = "cp1251"
                        xml_a_try = xmltodict.parse(a_xml.text)
                        # print(xml_a_try)
                    except:
                        print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                        try:                                
                            send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                            print('sent in sbis')
                            print('_____________')
                        except Exception as e:
                            print(e)
                            print('_____________')   
                
                except:
                    
                    attachments_id = {}
                    try:
                        for l in range(len(str_to_dict_points["result"]["Вложение"])):
                            # if str_to_dict_points["result"]["Вложение"][l]["Тип"].lower() in ("актпп", "актвр", "эдонакл", "упддоп", "упдсчфдоп"):
                            if str_to_dict_points["result"]["Вложение"][l]["Тип"].lower() in ("эдосч"):
                                # print(str_to_dict_points["result"]["Вложение"][l]["Тип"].lower())
                                attachments_id[str_to_dict_points["result"]["Вложение"][l]["Тип"].lower()] = str_to_dict_points["result"]["Вложение"][l]["Файл"]["Ссылка"]
                            else:
                                pass
                    except:
                        pass
                    
                    try:
                        
                        link_xml = list(attachments_id.values())[0]
                        a_xml = requests.get(link_xml, headers=headers)
                        a_xml.encoding = "cp1251"
                        xml_a_try = xmltodict.parse(a_xml.text)       
                    except:
                        print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                        try:                                
                            send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                            print('sent in sbis')
                            print('_____________')
                        except Exception as e:
                            print(e)
                            print('_____________')       
                                
    # ___________________________________________________________________________________________           
                if len(attachments_id) == 0:
                    
                    doc_append_exc()               
    # ___________________________________________________________________________________________            
                elif len(attachments_id) > 0:
                        
                    for b in attachments_id.keys():
                        # print(b)
                        
                        # if  b == "актвр":
                        #     # ___________________________________________________________________________________________    
                        #     a = requests.get(attachments_id["актвр"], headers=headers)
                        #     a.encoding = "cp1251"
                            
                        #     try:
                        #         xml_a = xmltodict.parse(a.text)
                        #         # ___________________________________________________________________________________________    
                        #         var_inside_doc_type = "актвр"

                        #         # ___________________________________________________________________________________________    
                        #         def_act_vr(xml_a, var_inside_doc_author, var_inside_doc_type)
                        #     except:
                        #         print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                        #         try:                                
                        #             send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                        #             print('sent in sbis')
                        #             print('_____________')
                        #         except Exception as e:
                        #             print(e)
                        #             print('_____________')                          
                        # # _______________________________________________________________________________________________________________________________
                            
                        # elif  b == "эдонакл":
                        if  b == "эдосч":
                            a = requests.get(attachments_id["эдосч"], headers=headers)
                            a.encoding = "cp1251"
                            
                            xml_a = xmltodict.parse(a.text)
                            # _______________________________________________________________________________________________________________________________
                            var_inside_doc_type = "эдосч"
                            # _______________________________________________________________________________________________________________________________
                            def_edo_nakl(xml_a, var_inside_doc_author, var_inside_doc_type, var_inside_dict_total_sell, var_inside_dict_total_payment)
                                
                            # a = requests.get(attachments_id["эдосч"], headers=headers)
                            # a.encoding = "cp1251"
                            
                            # try:
                            #     xml_a = xmltodict.parse(a.text)
                            #     # _______________________________________________________________________________________________________________________________
                            #     var_inside_doc_type = "эдосч"
                            #     # _______________________________________________________________________________________________________________________________
                            #     # def_edo_nakl(xml_a, var_inside_doc_author, var_inside_doc_type, dict_total_sell, dict_total_payment)
                            #     def_edo_nakl(xml_a, var_inside_doc_author, var_inside_doc_type, var_inside_dict_total_sell, var_inside_dict_total_payment)
                                
                            # except:
                            #     print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                            #     try:                                
                            #         send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                            #         print('sent in sbis')
                            #         print('_____________')
                            #     except Exception as e:
                            #         print(e)
                            #         print('_____________')                         
                        # _______________________________________________________________________________________________________________________________
                        
                        # elif  b == "актпп":     
                                
                        #     # _______________________________________________________________________________________________________________________________
                        #     a = requests.get(attachments_id["актпп"], headers=headers)
                        #     a.encoding = "cp1251"

                        #     try:
                        #         xml_a = xmltodict.parse(a.text)    
                                
                        #         # _______________________________________________________________________________________________________________________________
                        #         var_inside_doc_type = "актпп"
                        #         # _______________________________________________________________________________________________________________________________
                        
                        #         def_act_pp(xml_a, var_inside_doc_author, var_inside_doc_type)
                            
                        #     except:
                        #         print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                        #         try:                                
                        #             send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                        #             print('sent in sbis')
                        #             print('_____________')
                        #         except Exception as e:
                        #             print(e)
                        #             print('_____________')                                                
                        # # _______________________________________________________________________________________________________________________________
                        
                        # elif  b == "упддоп":   
                            
                        #         # _______________________________________________________________________________________________________________________________
                        #     a = requests.get(attachments_id["упддоп"], headers=headers)
                        #     a.encoding = "cp1251"
                            

                        #     try:
                        #         xml_a = xmltodict.parse(a.text)    

                            
                        #         # _______________________________________________________________________________________________________________________________
                        #         var_inside_doc_type = "упддоп"         
                        #         # _______________________________________________________________________________________________________________________________
                            
                        #         def_upd_dop(xml_a, var_inside_doc_author, var_inside_doc_type)
                            
                        #     except:
                        #         print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                        #         try:                                
                        #             send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                        #             print('sent in sbis')
                        #             print('_____________')
                        #         except Exception as e:
                        #             print(e)
                        #             print('_____________')  
                        # # _______________________________________________________________________________________________________________________________
                        # elif  b == "упдсчфдоп":     
                        #     a = requests.get(attachments_id["упдсчфдоп"], headers=headers)
                        #     a.encoding = "cp1251"
                            

                            
                        #     try:
                        #         xml_a = xmltodict.parse(a.text)    
                                
                        #         # _______________________________________________________________________________________________________________________________
                        #         var_inside_doc_type = "упдсчфдоп"
                        #         # _______________________________________________________________________________________________________________________________
        
                        #         def_upd_s_dop(xml_a, var_inside_doc_author, var_inside_doc_type)
                        #     except :
                        #         print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                        #         try:                                
                        #             send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                        #             print('sent in sbis')
                        #             print('_____________')
                        #         except Exception as e:
                        #             print(e)
                        #             print('_____________')                                   
                else:
                    doc_append_exc()      
                                        
        if var_status_has_more == "Нет":
            break
        elif str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"] == "Да":
            i_page += 1
        else:
            pass
        # var_status_has_more = str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"]
        var_status_has_more = "Нет"
        # print("ЕстьЕще", var_status_has_more)
        # print("___________________________________________________________________________________________________________________________________________________________")
        # print(f"СЛЕДУЮЩАЯ СТРАНИЦА {i_page}")       
            
            
    lst_append = [doc_id,
    doc_type,
    doc_number,
    doc_full_name,
    doc_data_main,
    doc_at_created,

    doc_counterparty_inn,
    doc_counterparty_full_name,

    doc_provider_inn,
    doc_provider_full_name,

    doc_assigned_manager,
    doc_department,

    inside_doc_author,
    inside_dict_total_sell,
    inside_dict_total_payment,
    inside_doc_type,
    inside_doc_item_full_doc_price,

    inside_doc_item_note,

    inside_doc_item_code,
    inside_doc_item_article,
    inside_doc_item_name,

    inside_doc_item_quantity,
    inside_doc_item_unit,

    inside_doc_item_price,
    inside_doc_item_full_item_price,
    
    # inside_agent,
    # inside_date_from,
    
    # inside_license_type,
    
    # inside_doc_item_sn,
    ]

    lst_append_name = [
        "doc_id",
        "doc_type",
        "doc_number",
        "doc_full_name",
        "doc_data_main",
        "doc_at_created",
        
        "doc_counterparty_inn",
        "doc_counterparty_full_name",
        
        "doc_provider_inn",
        "doc_provider_full_name",
        
        "doc_assigned_manager",
        "doc_department",
        "doc_notation",
        
        "inside_doc_author",
        "dict_total_sell",
        "dict_total_payment",
        "inside_doc_type",
        "inside_doc_item_full_doc_price",
        
        "inside_doc_item_note",
        
        "inside_doc_item_code",
        "inside_doc_item_article",
        "inside_doc_item_name",
        
        "inside_doc_item_quantity",
        "inside_doc_item_unit",
        
        "inside_doc_item_price",
        "inside_doc_item_full_item_price",
        
        # "inside_agent",
        # "inside_date_from",
        
        # "inside_license_type",
        
        # "inside_doc_item_sn",

    ]    
            
    lst_append_exc_name = [
        "doc_id",
        "doc_type",
        "doc_number",
        "doc_full_name",
        "doc_data_main",
        "doc_at_created",
        
        "doc_counterparty_inn",
        "doc_counterparty_full_name",
        
        "doc_provider_inn",
        "doc_provider_full_name",
        
        "doc_assigned_manager",
        "doc_department",
    ]     

            
    df = pd.DataFrame(columns=lst_append_name, data=list(zip(
    doc_id,
    doc_type,
    doc_number,
    doc_full_name,
    doc_data_main,
    doc_at_created,

    doc_counterparty_inn,
    doc_counterparty_full_name,

    doc_provider_inn,
    doc_provider_full_name,

    doc_assigned_manager,
    doc_department,
    doc_notation,

    inside_doc_author,
    inside_dict_total_sell,
    inside_dict_total_payment,
    inside_doc_type,
    inside_doc_item_full_doc_price,

    inside_doc_item_note,

    inside_doc_item_code,
    inside_doc_item_article,
    inside_doc_item_name,

    inside_doc_item_quantity,
    inside_doc_item_unit,

    inside_doc_item_price,
    inside_doc_item_full_item_price,
    
    # inside_agent,
    # inside_date_from, 
    
    # inside_license_type,   
    
    # inside_doc_item_sn,
    )))
        
    def doc_append_exc():
        doc_id_exc.append(var_link)
        doc_type_exc.append(var_doc_type)
        doc_number_exc.append(var_doc_number)
        doc_full_name_exc.append(var_doc_full_name)
        doc_data_main_exc.append(var_doc_data_main)
        doc_at_created_exc.append(var_doc_at_created)
        doc_counterparty_inn_exc.append(var_doc_counterparty_inn)
        doc_counterparty_full_name_exc.append(var_doc_counterparty_full_name)
        doc_provider_inn_exc.append(var_doc_provider_inn)
        doc_provider_full_name_exc.append(var_doc_provider_full_name)

        doc_assigned_manager_exc.append(var_doc_assigned_manager)
        doc_department_exc.append(var_doc_department)       
            
    df_exc = pd.DataFrame(columns=lst_append_exc_name, data=list(zip(
    doc_id_exc,
    doc_type_exc,
    doc_number_exc,
    doc_full_name_exc,
    doc_data_main_exc,
    doc_at_created_exc,

    doc_counterparty_inn_exc,
    doc_counterparty_full_name_exc,

    doc_provider_inn_exc,
    doc_provider_full_name_exc,

    doc_assigned_manager_exc,
    doc_department_exc,
    )))
    
    my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/{var_db_name}")
    try: 
        my_conn.connect()
        print('my_conn.connect()')
        my_conn = my_conn.connect()
        df.to_sql(name=f'{name_unloading}', con=my_conn, if_exists="replace")
        print("df.sent()")
        my_conn.close()
        print("my_conn.close()")
    except:
        print('failed')
        print('my_conn.failed()')        
            
    my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/{var_db_name}")
    try: 
        my_conn.connect()
        print('my_conn.connect()')
        my_conn = my_conn.connect()
        df_exc.to_sql(name=f'{name_unloading_exc}', con=my_conn, if_exists="replace")
        print("df_exc.sent()")
        my_conn.close()
        print("my_conn.close()")
    except:
        print('failed')
        print('my_conn.failed()') 


# sbis_bill_processing_0('03.03.2025', '03.03.2025', 'test_name_unloading_bill', 'test_name_unloading_bill_exc')