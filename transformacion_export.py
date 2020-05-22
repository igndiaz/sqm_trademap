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
preferencias= os.getcwd()
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
        df_ = df_[df_.Importers != "World"]
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
    id_vars = ["Importers", "Country", "Product"]
    
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
    id_vars = ["Importers", "Country", "Product"]
    
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

    id_vars = ["Importers", "Country", "Product"]
    
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
else:
    cantidad_joins = len(join_izq)
    
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
    joined_df = pd.merge(left=join_izq[i], right=join_der[i], on=["Importers", "Country", "Product", "Year", "Month"])
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
union_no_duplicates = union_df.drop_duplicates(subset=["Importers", "Country", "Product", "Year", "Month"], keep='first')

print(union_no_duplicates.shape)


# In[ ]:


# Exportación a formato Excel de tabla de buenos:
#union_no_duplicates.to_excel("../output_imports.xlsx")
union_no_duplicates.to_csv("trademap_export_productos.csv")
#upload_blob("sqm_trademap_productos_exports","trademap_export_productos.csv","trademap_export_productos.csv")


# In[ ]:


# Exportación a formato Excel de tabla de buenos:
#union_no_duplicates.to_excel("../output_imports.xlsx")
union_no_duplicates.to_excel("trademap_export_productos.xlsx")


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

