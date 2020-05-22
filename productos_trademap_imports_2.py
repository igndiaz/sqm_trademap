#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# -*- coding: utf-8 -*-
"""
Created on Mon Sep  2 15:18:25 2019

Process: Trade Map

@author: Doge
"""
import time
import datetime # para controlar tiempo debug
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
#from google.cloud import storage
import os
import pandas as pd
import shutil
import json


# In[ ]:


# linux
#from pyvirtualdisplay import Display
#display = Display(visible=0, size=(1366, 768))
#display.start()


# In[ ]:


# funcion para saber la hora actual. Devuelve Fecha y hora concatenado y sin caracteres especiales.
def get_time():
    now = datetime.datetime.now()
    string_now = now.strftime("%Y-%m-%d %H:%M:%S")
    string_now = string_now.replace("-", "")
    string_now = string_now.replace(":", "")
    string_now = string_now.replace(" ", "_")
    return string_now

def get_date():
    now = datetime.datetime.now()
    string_now = now.strftime("%Y%m%d_%H%M%S")
    return string_now

# Lectura de usuario y contraseña del sitio
try:
    with open("secrets/trademap_cultivos.json", "r", encoding="UTF-8") as json_file:
        data = json.load(json_file)
        user = data["user"]
        pwd = data["pass"]
        criterio = data["criterio"]
        print("Carga de usuario/Contraseña: OK./ ")
except FileNotFoundError:
    print("No se ha podido leer el archivo de credenciales, revise tenerlo en 'secrets/trademap.json'\n")
except:
    print("Ocurrió un error al tratar de leer archivo de credenciales, detalles del error:\n")
    raise


# In[ ]:


# leer el listado de productos de un txt y lo guarda en una lista (editar el txt para cambiar el alcance)
txt_productos = "variables_sitios/productos.txt"
productos = []

f = open(txt_productos, "r")
fl = f.readlines()
[productos.append(x.strip()) for x in fl]
f.close()

print(productos)


# leer el listado de PAISES de un txt y lo guarda en una lista (editar el txt para cambiar el alcance)
txt_paises = "variables_sitios/paises.txt"
paises = []

f = open(txt_paises, "r")
fl = f.readlines()
[paises.append(x.strip()) for x in fl]
f.close()

print(paises)


# In[ ]:


# Inicializar Web Driver

# Opcional: dar la ruta manualmente en caso de que el PATH no este bien configurado
#os.chdir(r"C:\PythonProjects\datapipeline_mining\source\new\src")
#os.chdir(r"C:\dev\SQM\repositorio_gitlab\src")

# obtener ubicación actual del ejecutable
directorio = os.getcwd()
os.chdir("..")

# intenta acceder a directorio Output, si no existe lo crea
try:
    os.chdir("output")
except FileNotFoundError:
    os.mkdir("output")
    print("No existía directorio 'output', creado nuevo directorio output/ en la ruta raiz")
    os.chdir("output")
except:
    print("Se encontró un error al tratar de entrar a output/, no pudo ser creado tampoco. Se muestra error a continuación: \n)")
    raise
    
# intenta acceder a directorio trademap_productos_imp, si no existe lo crea
try:
    os.chdir("trademap_productos_imp")
except FileNotFoundError:
    os.mkdir("trademap_productos_imp")
    print("No existía directorio 'trademap_productos_imp', creado nuevo directorio output/trademap_productos_imp")
    os.chdir("trademap_productos_imp")
except:
    print("Se encontró un error al tratar de entrar a output/trademap_productos_imp, no pudo ser creado tampoco. Se muestra error a continuación: \n)")
    raise
# Imprime posicion actual (directorio donde haremos las descargas)
preferencias = os.getcwd() + "/" + get_time()
try:
    os.mkdir(preferencias)
    os.chdir(preferencias)
except:
    print("no se puede crear directorio de Descarga " + str(preferencias))
    
# visualizamos posicion donde estaremos, directorio activo (==preferencias)
print(os.getcwd())

# Crea carpetas donde moveremos los archivos
try:
    os.mkdir("raw")
    os.mkdir("etapa1")
    os.mkdir("etapa2")
    print("Creados directorios para procesar los datos: raw/, etapa1/, etapa2/ \n")
except:
    print("problemas al crear directorios auxiliares, revisar codigo del error:\n")
    raise


# Crea log con hora de ejecucion
with open("log.txt", "a") as log:
    log.writelines("Inicio de Ejecución (YYYYMMDD_HHMMSS): " + get_time())
    log.writelines("\n")
    log.writelines("Fin")


# In[ ]:


print(preferencias)

chrome_preferences = {"download.default_directory" : preferencias}
ruta = directorio + "/chromedriver"
url = "https://www.trademap.org/" 

# Iniciar Chrome con preferencias
options = webdriver.ChromeOptions()
options.add_experimental_option("prefs", chrome_preferences)
#options.addArguments("--disable-gpu")
options.add_argument("--headless")
driver = webdriver.Chrome(executable_path=ruta, options=options)


# In[ ]:


# activar implicit wait
driver.implicitly_wait(10)


# In[ ]:


# funciones para cronometrar

# variable que acumula los tiempos de todas las ejecuciones en total
tiempo_total_ejecucion = 0.0

def cronometro_start():
    """
    marca el tiempo inicial antes de ejecutar un bloque de codigo y retorna la hora actual
    """
    global start_time
    start_time = time.time()
    return start_time
    

    
def cronometro_end(hora_inicio):
    """
    recibe la hora inicial como parametro, al iniciar un proceso, y retorna la diferencia de tiempo transcurrida
    """
    total_time = (time.time() - start_time)
    print("Tardó: " + str(total_time) + " segundos en realizar acción")
    return total_time


def consulta_tiempo_total():
    print("\n Tiempo acumulado total hasta ahora: " + str(tiempo_total_ejecucion))


# In[ ]:


# FUNCIONES
def login():
    driver.find_element_by_id('ctl00_MenuControl_marmenu_login').click()
    time.sleep(5)
    login_iframe = driver.find_element_by_id('iframe_login')
    driver.switch_to.frame(login_iframe)
    time.sleep(2)
    username = driver.find_element_by_id('PageContent_Login1_UserName')
    username.send_keys(user)
    time.sleep(2)
    password = driver.find_element_by_id('PageContent_Login1_Password')
    password.send_keys(pwd)
    time.sleep(2)
    driver.find_element_by_id('PageContent_Login1_Button').click()
    driver.switch_to.default_content()
    time.sleep(11)


# In[ ]:


def log_download(filename):
    # anota el archivo descargado/transformado en txt: Hora,Archivo
    with open("log.txt", "a") as log:
        log.writelines("\n")
        log.writelines(str(get_time()) + "," + filename)
        log.writelines("\n")


# In[ ]:


def exportaciones(producto, pais):
    inicio = cronometro_start()
    
    time.sleep(2)
    print(str(get_time()) + " se ejecuta proceso para imports de " + str(pais))
 
    
    print(str(get_time()) + " boton imports")
    
    time.sleep(2)
    
    # Ingreso de inputs 
    for intento in range(0,5):
        # Codigo de Producto
        code = driver.find_element_by_id('ctl00_PageContent_RadComboBox_Product_Input')
        print(str(get_time()) + " ingresa producto")
        #compara = str(code.get_attribute("value"))
        #print(str(get_time()) + " Input: " + str(compara) + " y Producto: " + str(producto))

        code.clear()
        time.sleep(1)
        code.send_keys(producto)
        time.sleep(2)
        #code.send_keys(" -")
        #time.sleep(5)
        # driver.find_element_by_class_name('ComboBoxItem_WebBlue').click()
        code.send_keys(Keys.ARROW_DOWN)
        time.sleep(2)
        code.send_keys(Keys.RETURN)
        time.sleep(1)

        # valida que se ingresara el producto
        code = driver.find_element_by_id('ctl00_PageContent_RadComboBox_Product_Input')
        texto_input_code = str(code.get_attribute('value').encode('utf-8'))
        print("Validacion Input Code: " + texto_input_code)
        if producto in texto_input_code:
            break
        else:
            time.sleep(1)
        
    
    # validacion de pais
    for intento in range(0,10):
        country = driver.find_element_by_id('ctl00_PageContent_RadComboBox_Country_Input')
        country.clear()
        time.sleep(1)
        country.send_keys(pais)
        time.sleep(3)
        print("pais es " + str(pais))
        country.send_keys(Keys.RETURN)
        time.sleep(3)

        # valida que se ingresara el pais
        texto_input_country = str(country.get_attribute('value').encode('utf-8'))
        print(type(texto_input_country))
        print(len(texto_input_country))
        print("Validacion Input Country: " + texto_input_country)
        
        time.sleep(1)
        
        if pais in texto_input_country:
            print("Pais bien ingresado")
            print(texto_input_country)
            break
        else:
            print("Pais mal ingresado")
            print(texto_input_country)
            time.sleep(1)

    
        
    
    # Click en Time Series Monthly
    boton_exports = driver.find_element_by_id('ctl00_PageContent_Button_TimeSeries_M')
    boton_exports.click()
    print("Ingresando a tablas")
    time.sleep(2)
    
    # En caso de que las pagina no nos mandara  Monthly Series
    try:
        periodo = Select(driver.find_element_by_xpath('//*[@id="ctl00_NavigationControl_DropDownList_OutputType"]'))
        print(len(periodo.options))
        print("revalidando monthly")
        [per.click() for per in periodo.options if "Monthly" in per.text]
        print("Cambiado a Monthly")
        time.sleep(2)
    except:
        print("Asegurado Monthly Time Series")
        
        
    # Valida que veamos Exports
    try:
        select_category = Select(driver.find_element_by_xpath('//*[@id="ctl00_NavigationControl_DropDownList_TradeType"]'))
        print(len(select_category.options))
        [cat.click() for cat in select_category.options if "Imports" in cat.text]
        print("Confirmado Imports")
        time.sleep(2)
    except:
        print("Asegurado Imports")
        
    
    # ciclo para validar CATEGORY Values
    for i in range(10):
        try:
            select_category = Select(driver.find_element_by_id('ctl00_NavigationControl_DropDownList_TS_Indicator'))
            category_active = select_category.first_selected_option
            print("Categoria actual: " + str(category_active.text))
            break
        except NoSuchElementException:
            print("reintentando en 1 segundo...")
            time.sleep(1)
    
    if category_active.text == "Values":
        pass
    else:
        category_active
    
    # Seleccionar 20 meses
    select_period_dwnld = Select(driver.find_element_by_id('ctl00_PageContent_GridViewPanelControl_DropDownList_NumTimePeriod'))
    options_period_dwnld = select_period_dwnld.options
    last_opt_dwnld = len(options_period_dwnld) - 1
    
    try:
        options_period_dwnld[last_opt_dwnld].click()
        print("Se selecciono perido de descarga: 20 meses")
    except:
        print("Error al tomar periodo de descarga de 20 meses")
        raise
    time.sleep(5)
        

    # ciclo que se repite 6 veces para descargar 10 años
    
    for i in range(1,2):
                
        # Continua para descargar los datos en formatos: "EXCEL" y "TXT"
#        export_excel = driver.find_element_by_id('ctl00_PageContent_GridViewPanelControl_ImageButton_ExportExcel')
        export_txt = driver.find_element_by_id('ctl00_PageContent_GridViewPanelControl_ImageButton_Text')
        
        #export_excel.click()
        export_txt.click()
        time.sleep(3)
        
        # lee archivos post descarga
        # si encuentra que el arch temporal de descarga, agrega 1 segundo, si no, pasa rapidamente abajo
        for intento in range(0,10):
            archivos_actuales = os.listdir()
            for archivo in archivos_actuales:
                if archivo.endswith(".crdownload"):
                    time.sleep(1)
                elif archivo.endswith(".tmp"):
                    time.sleep(1)
                else:
                    pass
            
        
            
        # Renombra descarga
        for arch in archivos_actuales:
            if arch not in archivos_procesados:
                print("nuevo detectado " + str(arch))
                nombre = "V_" + pais + "_" + producto + "_" + str(i) + ".txt"
                # cambiara nombre cuando archivo este disponible
                for intento in range(0,10):
                    try:
                        os.rename(arch, nombre)
                        archivos_procesados.append(nombre)
                        print("renombrado " + str(nombre))
                        break
                    except PermissionError:
                        time.sleep(1)
                    except FileNotFoundError:
                        time.sleep(1)
                        continue                    
              
        # registra archivo en log
        log_download(nombre)
        
        # ver periodo anterior (FLECHA IZQUIERDA)
    #    flecha_izquierda = driver.find_element_by_id('ctl00_PageContent_GridViewPanelControl_ImageButton_Previous')
    #    flecha_izquierda.click()
    #    print("click izquierda " + str(i))
    #    time.sleep(1)
        
    # SE TERMINA DESCARGA DE 6 ARCHIVOS
    
    
    # Retroceder la fecha hasta la mas reciente de nuevo (click en derecha)
    #for i in range(1,2):
    #    flecha_derecha = driver.find_element_by_id('ctl00_PageContent_GridViewPanelControl_ImageButton_Next')
    #    flecha_derecha.click()
    #    print("click derecha " + str(i))
    #    time.sleep(1.5)
    cronometro_end(inicio)         
              
   
    #
    #
    # Descarga de QUANTITIES           
    inicio = cronometro_start()          
    
    # Configuramos QUANTITY
    # ciclo para validar CATEGORY Q
    
    # LEER Categoria
    for i in range(10):
        try:
            select_category = Select(driver.find_element_by_id('ctl00_NavigationControl_DropDownList_TS_Indicator'))
            category_active = select_category.first_selected_option
            print("Categoria actual: " + str(category_active.text))
            break
        except NoSuchElementException:
            print("reintentando en 1 segundo...")
            time.sleep(1)
    
    # Cambiar Categoria a Quantity si estuviera en Value
    if category_active.text == "Values":
        select_category.options[1].click()
        time.sleep(1)
        select_category = Select(driver.find_element_by_id('ctl00_NavigationControl_DropDownList_TS_Indicator'))
        category_active = select_category.first_selected_option
        print("Configurada Categoria:")
        print(category_active.text)
    else:
        print("estamos en " + str(category_active.text))

    

    
    # ciclo que se repite 6 veces para descargar 10 años
    
    for i in range(1,2):
                
        # Continua para descargar los datos en formatos: "EXCEL" y "TXT"
        #export_excel = driver.find_element_by_id('ctl00_PageContent_GridViewPanelControl_ImageButton_ExportExcel')
        export_txt = driver.find_element_by_id('ctl00_PageContent_GridViewPanelControl_ImageButton_Text')
        #export_excel.click()
        export_txt.click()
        time.sleep(3)
        
        # lee archivos post descarga
        # lee archivos post descarga
        # si encuentra que el arch temporal de descarga, agrega 1 segundo, si no, pasa rapidamente abajo
        for intento in range(0,10):
            archivos_actuales = os.listdir()
            for archivo in archivos_actuales:
                if archivo.endswith(".crdownload"):
                    time.sleep(1)
                elif archivo.endswith(".tmp"):
                    time.sleep(1)
                else:
                    pass
                
        # Renombra descarga
        for arch in archivos_actuales:
            if arch not in archivos_procesados:
                print("nuevo detectado " + str(arch))
                nombre = "Q_" + pais + "_" + producto + "_" + str(i) + ".txt"
                # cambiara nombre cuando archivo este disponible
                for intento in range(0,10):
                    try:
                        os.rename(arch, nombre)
                        archivos_procesados.append(nombre)
                        print("renombrado " + str(nombre))
                        break
                    except PermissionError:
                        time.sleep(1)
        
              
        # registra archivo en log
        log_download(nombre)
        
        # ver periodo anterior (FLECHA IZQUIERDA)
        #flecha_izquierda = driver.find_element_by_id('ctl00_PageContent_GridViewPanelControl_ImageButton_Previous')
        #flecha_izquierda.click()
        #print("click izquierda " + str(i))
        #time.sleep(1)
        
    # SE TERMINA DESCARGA DE 6 ARCHIVOS
    
    
    # Retroceder la fecha hasta la mas reciente de nuevo (click en derecha)
    #for i in range(1,2):
    #    flecha_derecha = driver.find_element_by_id('ctl00_PageContent_GridViewPanelControl_ImageButton_Next')
    #    flecha_derecha.click()
    #    print("click derecha " + str(i))
    #    time.sleep(1.5)
              
    
    
    # tiempo en completar esta tarea
    cronometro_end(inicio)
    
    # cuando necesitamos regresar al home          
    try:
        driver.find_element_by_id('ctl00_MenuControl_Label_Menu_Home').click()
    except:
        driver.find_element_by_xpath('//*[@id="ctl00_MenuControl_Label_Menu_Home"]').click()
    print(str(get_time()) + " Descarga realizada, se regresa a la pagina de inicio")
    consulta_tiempo_total()
    time.sleep(5)


# In[ ]:


def salir():
    driver.quit()


# # Acceder

# In[ ]:


# ACCIONES
# Abrir pagina de South Africa Stats
driver.get(url)
time.sleep(5)
assert "Trade Map - Trade statistics for international business development" in driver.title, "Error en carga de pagina, verificar direccion correcta"
print(driver.current_url)


# # Sesion

# In[ ]:


# Login
try:
    login()
    print(str(get_time()) + " inicio de sesion exitoso")
    time.sleep(5)
except:
    print(str(get_time()) + " algo ocurrio al tratar de iniciar sesion")
    raise



# # Carga de Variables

# In[ ]:


# listado de paises y productos
lista_de_productos = productos
lista_de_paises = paises


print("total de paises: " + str(len(lista_de_productos)))
print("total de paises: " + str(len(lista_de_paises)))

print("Elementos en total a descargar: " + str(len(lista_de_paises) * len(lista_de_productos)))


# # Ejecucion del proceso de descarga
# 

# In[ ]:


# obtiene archivos y carpetas previa descarga y crea lista
# genera lista antes de descargar
archivos_procesados = os.listdir()
print("archivos iniciales : " + str(archivos_procesados))


# In[ ]:


for p in lista_de_productos:
    for c in lista_de_paises:
        print(p + " " + c)
        exportaciones(p, c)
        time.sleep(5)


# In[ ]:


time.sleep(5)
print("Se terminó ejecución del proceso")

salir()
print("Se cierra navegador")


# In[ ]:


# obtiene cantidad de archivos descargados
cantidad_descarga_final = len(os.listdir()) - 4
listado_descarga_final = os.listdir()
print("Descargados: " + str(cantidad_descarga_final))

# Guarda info de la ejecucion en log txt
with open("log.txt", "a") as log:
    log.writelines("\n")
    log.writelines("___")
    log.writelines("Fin de Ejecución (YYYYMMDD_HHMMSS): " + get_time())
    log.writelines("\n")
    log.writelines("Cantidad de archivos descargados: " + str(cantidad_descarga_final))
    log.writelines("\n")
    for linea in listado_descarga_final:
        log.writelines(linea)
        log.writelines("\n")
    log.writelines("Fin del documento")

time.sleep(10)

# ## Proceso de transformación de los datos

# In[ ]:


import os
import pandas as pd
import datetime
import time
import shutil
import glob

# aportes de Henry - febrero, 2020


# In[ ]:


#cwd = os.getcwd()
#preferencias = r"C:\dev\SQM\repositorio_gitlab\output\trademap\test-trademap-remake2"

home_dir = preferencias
cwd = home_dir


# In[ ]:


# Usaremos imports para testing
data_folder = os.path.join(preferencias)
data_files = os.listdir(data_folder)

print(cwd)
print(data_folder)
print(len(data_files))
try:
    os.chdir(data_folder)
except:
    print("ya nos movimos")


# In[ ]:


def borra_world(df_):
    # Elimina la fila World
    try:
        df_ = df_[df_.Exporters != "World"]
    except:
        print("No se pudo borrar World")
    df_ = df_.reset_index(drop=True)
    return df_

#def upload_blob(bucket_name, source_file_name, destination_blob_name):
    # Uploads a file to the bucket.
#    storage_client = storage.Client()
#    bucket = storage_client.get_bucket(bucket_name)
#    blob = bucket.blob(destination_blob_name)
#    blob.upload_from_filename(source_file_name)
#    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name))
# In[ ]:


def obten_pais_prod(archivo_):
    # Obtener nombre del país actual y código de producto del nombre de archivo
    country = str(archivo_.split("_")[1])
    product =str(archivo_.split("_")[2])
    return(country,product)


# In[ ]:


def unpivot_v(df_):
    # columnas que quedan intactas
    id_vars = ["Exporters", "Country", "Product"]
    
    # columnas a las que se les hará unpivot
    value_vars = []
    [value_vars.append(col) for col in df_.columns if col not in id_vars]
    
    # Realiza UNPIVOT
    df3 = pd.melt(frame=df_, id_vars=id_vars, value_vars = value_vars, var_name="Year_Month", value_name="Values")

    #print(df3.head(1))
    return df3


# In[ ]:


def unpivot_q(df_):
    # columnas que quedan intactas
    id_vars = ["Exporters", "Country", "Product"]
    
    # columnas a las que se les hará unpivot
    value_vars = []
    [value_vars.append(col) for col in df_.columns if col not in id_vars]
    
    # Realiza UNPIVOT
    df3 = pd.melt(frame=df_, id_vars=id_vars, value_vars = value_vars, var_name="Year_Month", value_name="Quantity")

    #print(df3.head(1))
    return df3


# In[ ]:


# VALUE

def unpivot_value(df2_, country, product):
    """
    Transforma un archivo Values dinámicamente en modo unpivot 
    """

    df2 = df2_
    df2["Country"] = country
    df2["Product"] = product

    id_vars = ["Exporters", "Country", "Product"]
    
    # columnas a las que se les hará unpivot
    value_vars = []
    [value_vars.append(col) for col in df2_.columns if col not in id_vars]

    # Realiza UNPIVOT
    df3 = pd.melt(frame=df2_, id_vars=id_vars, value_vars = value_vars, var_name="Year_Month", value_name="Values")
    
    return df3


# In[ ]:


def divide_fecha(df_):
    """
    separa la columna month_year en 2 y borra la columna original (debe estar en indice 3)
    """
    df = df_
    
    # Eliminamos texto innecesario con un replace
    try:
        df["Year_Month"] = df.Year_Month.str.replace("Exported value in ", "")

        # Separamos Año y Mes
        df["Year"] = df["Year_Month"].str.split("-M").str[0]
        df["Month"] = df["Year_Month"].str.split("-").str[1]
        df["Unit"] = df["Month"].str.split("~").str[1]
        df["Month"] = df["Month"].str.split("~").str[0]
        
        # limpia columna de unit
        df["Unit"] = df["Unit"].str.replace("Exported quantity, ", "")
        df["Unit"] = df["Unit"].str.strip()

        # Eliminamos columnas que ya no necesitamos
        df = df.drop(columns=["Year_Month"])
        
    except:
        print("No se pudo separar año y mes, error:\n ")
        raise


    return df


# In[ ]:


def divide_fecha_2(df_):
    """
    separa la columna month_year en 2 y borra la columna original (debe estar en indice 3)
    """
    df = df_.copy()
    
    # Eliminamos texto innecesario con un replace
    try:
        df["Year_Month"] = df.Year_Month.str.replace("Exported value in ", "")

        # Separamos Año y Mes
        df["Year"] = df["Year_Month"].str.split("-").str[0]
        df["Month"] = df["Year_Month"].str.split("-").str[1]
        
        df["Unit"] = df["Year_Month"].str.split(",").str[1].astype("str")
        df["Unit"] = df["Unit"].str.replace("Imported quantity, ", "")
        df["Unit"] = df["Unit"].str.strip()
        
        # Eliminamos columnas que ya no necesitamos
        df = df.drop(columns=["Year_Month"])
        

        
    except:
        print("No se pudo separar año y mes, error:\n ")
        raise


    return df


# In[ ]:


def separa_unit(df_):
    try:
        df_["Units"] = df_.Year_Month.str.split("~").str[1]
    except:
        print("Problema al extraer unit")
        raise
        
    return df_


# In[ ]:


quantity_xls = glob.glob("Q*.txt")
value_xls = glob.glob("V*.txt")

quantity_xls = sorted(quantity_xls)
value_xls = sorted(value_xls)

print(len(quantity_xls), len(value_xls))


# In[ ]:


def read_txt(filename):
    df_ =  pd.read_csv(filename, sep='\t')
    del df_[df_.columns[df_.shape[1]-1]]
    return df_


# In[ ]:


def reparaMalo(_df_):
    """
    Esta funcion se encarga de normalizar las tablas con esquema incorrecto, rescatando
    la unidad cuando esta viene en una columna auxiliar
    """
    cols = _df_.columns
    to_drop = []
    new_columns = []
    
    i = 0
    for i in range(len(cols)-1):
        if _df_.columns[i][:8] == _df_.columns[i+1][:8]:
            _df_[_df_.columns[i]] = _df_[_df_.columns[i]].astype(str) + "~" + _df_[_df_.columns[i+1]].astype(str)
            to_drop.append(_df_.columns[i+1])            
        else:
            new_columns.append(_df_.columns[i])

    for col in to_drop:
        _df_ = _df_.drop([col], axis=1)
    
    return _df_


# ## Procesar QUANTITIES

# In[ ]:


# lista para saber dfs buenos y malos QUANTITY
df_q_malos = 0
df_q_buenos = 0
dataframes_q = []
filas_total_q = 0
lista_q_malos = []
dataframes_malos_q = []

lista_vacios_q = []

df_q_reparados = 0


# lectura de dataframes
pos = 0
for table in quantity_xls:
    
    #print("table:",table)
    _df = read_txt(table)
    
    if _df.shape[0] <= 1:  ## Vacios
        lista_vacios_q.append(table)
    
    else:   ## Buenos
        
        if _df.shape[1] == 21:
            df_q_buenos += 1
            filas_total_q += _df.shape[0]
            dataframes_q.append(_df)
        else:
            df_q_reparados += 1
            _df_rep = reparaMalo(_df)
            if _df_rep.shape[1] != 21:
                print("ERROR: esto no deberia pasar, luego de reparado deberia querdar en 21 columnas")
                break
                
            filas_total_q += _df_rep.shape[0]
            dataframes_q.append(_df_rep)
    
    pos += 1
        
#ff = read_txt(table).head(3)
#print(ff.head(3))
#print(reparaMalo(ff).head(3))
try:

    print(f"df buenos: {df_q_buenos}")
    print(f"df reparados: {df_q_reparados}")
    print(f"df agregados: {len(dataframes_q)}")
    print(f"vacios: {len(lista_vacios_q)}")
    print(f"Número de filas: {filas_total_q}")
    print(f"Número de filas Q: {filas_total_q}")
except:
    pass


# In[ ]:


# mover archivos "malos" a  la carpeta "malos" en lista_q_malosque exista al menos un malo ( o más)

data_folder_malos = os.path.join(preferencias, "etapa1")
if not os.path.isdir(data_folder_malos) and len(lista_q_malos) > 0:    
    try:
        os.mkdir("etapa1")
    except:
        raise
    [shutil.move(file, data_folder_malos) for file in lista_q_malos]
    [shutil.move(str("V") + file[1:], data_folder_malos) for file in lista_q_malos]

    
data_folder_vacios = os.path.join(preferencias, "etapa2")
if not os.path.isdir(data_folder_vacios) and len(lista_vacios_q) > 0:    
    try:
        os.mkdir("etapa2")
    except:
        raise
    [shutil.move(file, data_folder_vacios) for file in lista_vacios_q]
    [shutil.move(str("V") + file[1:], data_folder_vacios) for file in lista_vacios_q]


# In[ ]:


## Ya que moví algunos ficheros de carpeta, actualizo los ficheros 

quantity_xls = glob.glob("Q*.txt")
value_xls = glob.glob("V*.txt")


quantity_xls = sorted(quantity_xls)
value_xls = sorted(value_xls)

print(len(quantity_xls), len(value_xls))


# ## Quantity Final (para left del JOIN)

# In[ ]:


test = dataframes_q[0].copy()
test.head()


# In[ ]:


df_q_listos = dataframes_q
#read_txt("/media/henry/Datos/_Zenta/Proyectos/SQM-alejandro/sqm-market-share/historico/data_processing/data/imports_txt/etapa1/imports/Q_Australia_070960_3.xls")


# ## Procesar Values

# In[ ]:


# lista para saber dfs buenos y malos VALUE

cwd = home_dir

try:
    os.chdir(data_folder)
except:
    print("ya nos movimos")


df_v_malos = 0
df_v_buenos = 0
dataframes_v = []
filas_total_v = 0
lista_v_malos = []
dataframes_malos_v = []

pos = 0


# lectura de dataframes
for table in value_xls:
    #print(table)
    df = read_txt(table)
    # Si COLs es correcto O
    if df.shape[1] == 21:
        df_v_buenos += 1
        filas_total_v += df.shape[0]
        dataframes_v.append(df)
    # Si COls es incorrecto X
    else:
        df_v_malos += 1
        lista_v_malos.append(table)
        dataframes_malos_v.append(df)
        
    pos += 1
        
try:
    print(f"df buenos: {df_v_buenos}")
    print(f"df malos: {df_v_malos}")
    print(f"df agregados: {len(dataframes_v)}")
    print(f"Número de filas: {filas_total_v}")
except:
    pass


# In[ ]:


try:
    print(f"Total Q: {len(dataframes_q)}")
    print(f"Número de filas Q: {filas_total_q}")
    print("-----------")
    print(f"Total V: {len(dataframes_v)}")
    print(f"Número de filas V: {filas_total_v}")
except:
    pass


# In[ ]:


ntest = 1


# In[ ]:


def cleanDF(_df_):
    #print(_df_.columns)
    _df_["Quantity"] = _df_["Quantity"].astype("str")
    for pos in range(_df_["Quantity"].shape[0]):
        coord = _df_["Quantity"][pos]
        #print(coord)
        if "~" in coord:
            #print(";)")
            # reemplazamos solo en filas reparadas la unidad
            rescue_unit = coord.split("~")[1]
            rescue_unit = rescue_unit.replace("nan", "No Quantity")
            _df_["Year_Month"][pos] = _df_["Year_Month"][pos] + ", "+rescue_unit
            
            # sobreescribimos ahora Quantity con el valor limpio (sin texto de unidad)
            rescue_value = coord.split("~")[0]
            _df_["Quantity"][pos] = rescue_value
    return _df_

#d = {'Year_Month': ['Apples', 'Oranges', 'Puppies', 'Ducks'],'Quantity': ["1", "2~Tons", "3~nan", "4"]}
#df = pd.DataFrame(d, columns=d.keys())          
#cleanDF(df)


# In[ ]:


def DealWithMissingUnitsInColumnsName(df):
    dt = {}
    for col in df.columns:
        if col.strip()[-1] == ",":
            dt[col] = col.replace(",",", No Quantity")
    df = df.rename(columns=dt)
    
    return df


# ## Join de DFs buenos
# 

# In[ ]:


start = time.time()

# lista que almacenará DFs procesados
join_izq = []

# Procesa IZQUIERDA
for indice in range(len(df_q_listos)):
    df = df_q_listos[indice] # selecciona el df con el que trabajaremos
    df = borra_world(df) # elimina la fila "world" del dataset
    string_q = quantity_xls[indice] # texto del nombre de archivo
    
    #print(string_q)

    # agrega country y product
    country, product = obten_pais_prod(string_q)
    df["Country"] = country
    df["Product"] = product
    
    
    df1 = DealWithMissingUnitsInColumnsName(df)
    
    # unpivot
    df2 = unpivot_q(df1)
    
    # separa fecha MES y AÑO
    df2_1 = cleanDF(df2)
    
    df3 = divide_fecha_2(df2_1)
    
    join_izq.append(df3)
    
    
end = time.time()
print(end - start)   
print("Listos para join: " + str(len(join_izq)))
print("quantity_xls["+str(ntest)+"]:", quantity_xls[ntest])


# In[ ]:


try:
    print("-------------\nantes",df_q_listos[ntest].shape)
    print(df_q_listos[ntest].head(3))
    print("-------------\ndespues", join_izq[ntest].shape)
    print(join_izq[ntest].head(3))
except:
    pass


# In[ ]:


start = time.time()

# lista que almacenará DFs procesados

join_der = []

# Procesa DERECHA
for indice in range(len(dataframes_v)):
    df = dataframes_v[indice] # selecciona el df con el que trabajaremos
    df = borra_world(df) # elimina la fila "world" del dataset
    
    #print(df.head())
    
    string_v = value_xls[indice] # texto del nombre de archivo

    # agrega country y product
    country, product = obten_pais_prod(string_v)
    df["Country"] = country
    df["Product"] = product
    
    # unpivot
    df2 = unpivot_v(df)

    # separa fecha MES y AÑO
    df3 = divide_fecha_2(df2)
    
    #df3["Year"] = df3["Year"].str.replace("Exported value in ", "")
    del df3['Unit']
    join_der.append(df3)
    
    
end = time.time()
print(end - start)   
print("Listos para join: " + str(len(join_der)))

try:
    join_izq[1].head(3)
except:
    pass


# In[ ]:


try:
    print("-------------\nantes",dataframes_v[ntest].shape)
    print(dataframes_v[ntest].head(3))
    print("-------------\ndespues", join_der[ntest].shape)
    print(join_der[ntest].head(15))
except:
    print("-------------\nantes",dataframes_v[ntest].shape)
    print(dataframes_v[ntest].head(3))
    print("-------------\ndespues", join_der[ntest].shape)
    print(join_der[ntest].head(3))


# In[ ]:


try:
    print("ntest:",ntest)
    print("\n----------------\n",quantity_xls[ntest])
    print(dataframes_q[ntest].head())
    print("\n----------------\n",value_xls[ntest])
    print(dataframes_v[ntest].head())
except:
    pass


# In[ ]:


print("Dimensiones de la tabla izquierda, tomada al azar")
print(join_izq[ntest].shape)
print(join_der[ntest].shape)


# In[ ]:


print("Cantidad de tablas izquierda: " + str(len(join_izq)))
print("Cantidad de tablas derecha: " + str(len(join_der)))

print(join_izq[ntest].head())
print(join_der[ntest].head())

meses = []
left_rows = 0
right_rows = 0


for izq in join_izq:
    izq_meses = izq["Month"].unique()
    left_rows += izq.shape[0]
    for im in izq_meses:
        if im in meses:
            pass
        else:
            meses.append(im)
            
for der in join_der:
    right_rows += der.shape[0]
    der_meses = der["Month"].unique()
    for dm in der_meses:
        if dm in meses:
            pass
        else:
            meses.append(dm)


if len(join_izq) == len(join_der):
    cantidad_joins = len(join_der)
    
meses.sort()

print("Categorias encontradas en JOIN:IZQ, Meses: "+ str(meses))
print("Categorias encontradas en JOIN:DER, Meses: "+ str(meses))

print("Filas en todo Left: " + str(left_rows))
print("Filas en todo Right: " + str(right_rows))


# In[ ]:


# Unir todos los df
listado_unidos = []

# hace inner Join uno por uno a todos los pares
for i in range(cantidad_joins):
    joined_df = pd.merge(left=join_izq[i], right=join_der[i], on=["Exporters", "Country", "Product", "Year", "Month"])
    listado_unidos.append(joined_df)
    
print("Cantidad de tablas ya unidas generadas: " + str(len(listado_unidos)))

#print("Muestras: ")
#print(listado_unidos[0])
#print("\n")
#print(listado_unidos[1])


# In[ ]:


len(listado_unidos)
listado_unidos[0]


# In[ ]:


# Unir verticalmente (UNION)

union_df = pd.concat(listado_unidos, ignore_index=True, sort=False)

print(union_df.info())

# Visualizar previamente secciones del df resultante
print(union_df.head(3))
try:
    print(union_df[50:55])
    print(union_df[100:110])
except:
    pass


# In[ ]:


# Eliminación de filas duplicadas
print(union_df.shape)
union_no_duplicates = union_df.drop_duplicates(subset=["Exporters", "Country", "Product", "Year", "Month"], keep='first')

print(union_no_duplicates.shape)


# In[ ]:


# Exportación a formato Excel de tabla de buenos:
#union_no_duplicates.to_excel("../output_imports.xlsx")
union_no_duplicates.to_csv("trademap_imports_productos.csv")
upload_blob("sqm_trademap_productos_imports","trademap_imports_productos.csv","trademap_imports_productos.csv")


# In[ ]:


# Exportación a formato Excel de tabla de buenos:
#union_no_duplicates.to_excel("../output_imports.xlsx")
union_no_duplicates.to_excel("trademap_imports_productos.xlsx")


# In[ ]:


# mover los raw
try:
    [shutil.move(file, "raw") for file in quantity_xls]
    print("Se movió los quanyity a raw")
except:
    print("No se pudo mover los quantity a raw")
    
try:
    [shutil.move(file, "raw") for file in value_xls]
    print("Se movió los value a raw")
except:
    print("No se pudo mover los value a raw")


# In[ ]:


# Ejecucion completa, gracias.

