import re
import os
from dotenv import load_dotenv
import imaplib # Biblioteca para interactuar con servidores de correo electrónico usando el protocolo IMAP
import email # Biblioteca para manejar correos electrónicos
from email.header import decode_header # Función para decodificar encabezados de correo electrónico

import pandas as pd # Biblioteca para manipulación y análisis de datos
from tqdm import tqdm # Biblioteca para añadir barras de progreso

import numpy as np # Biblioteca para operaciones numéricas y matrices
from PIL import Image # Biblioteca para abrir, manipular y guardar imágenes

import matplotlib.pyplot as plt # Biblioteca para crear gráficos y visualizaciones
from matplotlib.colors import LinearSegmentedColormap # Función para crear mapas de colores segmentados
import seaborn as sns
from collections import Counter # Se utiliza para contar objetos hashables


################# RESUMEN #################

# Tutorial: https://www.youtube.com/watch?v=a5EmgTZva-4&ab_channel=Sandreke%7CData%26Programaci%C3%B3n
# Link Contraseña de App: https://myaccount.google.com/apppasswords?pli=1&rapt=AEjHL4OE19VRRIYsgK0WrYU_lDAdiRxms1O_vONPdWy2EPewxOXT7aCCabQHk03hYW2lPCIn9kJMgy8-YJWNSEzSdAGT1X21CDt0bHAoQGdGxQK8BaFO5DI

#- Scrapeo/Obtencion de datos
#- Limpieza de datos
#- Gestion ficheros
#- Visualizacion
###########################################



############## CONFIGURACION ##############

# Cargar el archivo .env
load_dotenv()

# Obtener la ruta desde el archivo .env
sMAIL_ACCOUNT = os.getenv('MAIL_ACCOUNT') # sMAIL_ACCOUNT = Cuenta que queremos analizar
sAPP_PASSWORD_GOOGLE = os.getenv('APP_PASSWORD_GOOGLE') # sAPP_PASSWORD_GOOGLE = Contraseña de aplicacion (Se llama asi) generada para este proyecto


bQuieroScrapear = True # Si quiero que empieza a scrapear 'True', y si no quiero porque lo tengo en un csv 'False'
# Configuramos la ruta donde queremos que se guarde los datos sin limpiar y el nombre del .csv 
sRUTA_DATOS_EN_BRUTO = os.getenv('RUTA_DATOS_EN_BRUTO')
# Configuramos la ruta donde queremos que se guarde los datos limpios y el nombre del .csv 
sRUTA_DATOS_LIMPIOS = os.getenv('RUTA_DATOS_LIMPIOS') # Si bQuieroScrapear es False, carga los datos de este .csv en el dataframe para analizar

# Graficos:
iNumPalabrasClave = 50

# Configuramos la ruta donde queremos que se guarde los datos limpios y el nombre del .csv 
sRUTA_TOP_PALABRAS_CLAVE = os.getenv('RUTA_TOP_PALABRAS_CLAVE')


# Palabras a excluir en el grafico de TopPalabrasClave
lStopWords={'-','_','|','.',',',':',';','"',"'", # Signos
            "a","ante","bajo","cabe","con","contra","de","desde","durante","en","entre","hacia","hasta","mediante","para","por","según","sin","sobre","tras", # Preposiciones
            'un','una','al','la','las','del','el','los','que','se','es','y','ha','te','tus','tu','tu:','ti','Re:','Re','de:','to','com', # Generico
            'Nueva','Nuevo','tarea:','Project:','tarea','of','Notification','rank','change','mañana','envío','Fecha','entrega',
            '1','2','3','4','5','6','7','8','9','0'} # Especifico

###########################################



########  SCRAPEO/OBTENCION-DATOS #########

def fConectarIMAP(sMAIL_ACCOUNT, sAPP_PASSWORD_GOOGLE, proveedor):
   """
   Conecta a la cuenta de correo según el proveedor (Gmail, Outlook, Yahoo).
   :param sMAIL_ACCOUNT: Usuario (correo electrónico).
   :param sAPP_PASSWORD_GOOGLE: Contraseña de la aplicación.
   :param proveedor: Nombre del proveedor (Gmail, Outlook, Yahoo).
   :return: oMyMail (objeto IMAP para interactuar con el correo).
   """
   if proveedor == 'Gmail':
      sImapUrl = 'imap.gmail.com'
   elif proveedor == 'Outlook':
      sImapUrl = 'imap-mail.outlook.com'
   elif proveedor == 'Yahoo':
      sImapUrl = 'imap.mail.yahoo.com'
   else:
      raise ValueError("Proveedor no soportado")

   try:
      oMyMail = imaplib.IMAP4_SSL(sImapUrl)
      oMyMail.login(sMAIL_ACCOUNT, sAPP_PASSWORD_GOOGLE)
      print(f'- Conexión exitosa a {proveedor}!')
      return oMyMail
   except Exception as e:
      print(f'- ERROR: Error al conectarse a {proveedor}: {e}')
      return None


def fScrapearCorreos(sMAIL_ACCOUNT, sAPP_PASSWORD_GOOGLE, proveedor='Gmail'): 
   """
   Función para scrapear correos desde diferentes proveedores de correo.
   :param sMAIL_ACCOUNT: Usuario (correo electrónico)
   :param sAPP_PASSWORD_GOOGLE: Contraseña de la aplicación
   :param proveedor: Proveedor de correo ('Gmail', 'Outlook', 'Yahoo')
   :return: DataFrame con los correos.
   """
   
   # Paso 1: Seleccionar el servidor IMAP según el proveedor
   if proveedor == 'Gmail':
      sImapUrl = 'imap.gmail.com'
   elif proveedor == 'Outlook':
      sImapUrl = 'imap-mail.outlook.com'
   elif proveedor == 'Yahoo':
      sImapUrl = 'imap.mail.yahoo.com'
   else:
      print(f"- ERROR: Proveedor {proveedor} no soportado.")
      return pd.DataFrame()

   # Conectarse al servidor IMAP
   try:
      oMyMail = imaplib.IMAP4_SSL(sImapUrl)
      print(f"- INFO: Conexión a {sImapUrl} exitosa.")
   except Exception as e:
      print(f"- ERROR: No se pudo conectar a {sImapUrl}: {e}")
      return pd.DataFrame()

   bContinue = True
   dfEmail = pd.DataFrame()

   try:
      oMyMail.login(sMAIL_ACCOUNT, sAPP_PASSWORD_GOOGLE)
   except Exception as e:
      print(f'- ERROR: Error al loguearse: {e} \n')
      bContinue = False

   if bContinue:
      # Ver cuantos correos tengo en la bandeja de entrada
      try:
         iTotalCorreos = int(oMyMail.select('Inbox')[1][0].decode('utf-8'))
         print(f'- iTotalCorreos: {iTotalCorreos} \n')
      except Exception as e:
         print(f'- ERROR: Error al seleccionar la bandeja de entrada: {e} \n')
         bContinue = False

   if bContinue:
      # Paso 2: Obtener correos electrónicos
      # Crear un DataFrame vacío
      dfEmail = pd.DataFrame(columns=['Date', 'From', 'Subject'])

      # Lista para almacenar los datos
      lRows = []

      for i in tqdm(range(iTotalCorreos), desc="Processing", unit="item", ncols=60, bar_format='{l_bar}{bar} | Time: {elapsed} | {n_fmt}/{total_fmt}'):
         try:
               sData = oMyMail.fetch(str(i), '(UID RFC822)')
               tArray = sData[1][0]

               if isinstance(tArray, tuple):
                  try:
                     sMsg = email.message_from_string(str(tArray[1], 'utf-8'))
                  except UnicodeDecodeError:
                     sMsg = email.message_from_string(str(tArray[1], 'latin-1'))

                  lRows.append({"Date": sMsg['Date'], "From": sMsg['from'], "Subject": sMsg['subject']})
         except Exception as e:
               print(f'\n - ERROR: Error al obtener el correo {i}: {e} \n')

      # Convertir la lista de filas a un DataFrame
      dfEmail = pd.DataFrame(lRows, columns=['Date', 'From', 'Subject'])

      # Limpiar filas vacías
      dfEmail = dfEmail.dropna(how='all').reset_index(drop=True)

      # Para ver las variables que podemos extraer
      print(f'- sMsg.keys(): \n {sMsg.keys()} \n')

   return dfEmail

###########################################



############ LIMPIEZA DE DATOS ############

def fLimpiarFecha(x):
   if ',' not in x: x = ', ' + x
   if '(' in x: x = ' '.join(x.split(' ')[:-1])
   x = ' '.join(x.split(' ')[:-1])
   return x


def fObtenerCorreoDeFrom(sMail):
   """Extrae la dirección de correo electrónico de una cadena de texto con formato 'Nombre <correo@example.com>'."""
   try:
      return sMail.split('<')[-1].split('>')[0]
   except Exception as e:
      print(f'\n - Error al extraer el correo: {e} \n')
      return ""


def fObtenerNombreDeFrom(sName):
   """Extrae el nombre de una cadena de texto con formato 'Nombre <correo@example.com>'."""
   try:
      sTexto, encoding = decode_header(sName)[0]
      if not encoding and isinstance(sTexto, str):
         sTexto = ' '.join(sTexto.split(' ')[:-1])
      else:
         sTexto = sTexto.decode('utf-8', errors='ignore')
      return sTexto.replace('"', '')
   except Exception as e:
      print(f'\n - Error al extraer el nombre: {e} \n')
      return ""


def fLimpiarSubject(sSubject):
   """Limpia el campo de 'Subject' decodificando y eliminando caracteres no deseados."""
   if isinstance(sSubject, float):
      # Maneja valores NaN o cualquier float
      return ""  
   if sSubject:
      try:
         sTexto, encoding = decode_header(sSubject)[0]
         sTexto = sTexto.decode('utf-8', errors='ignore') if encoding else sTexto
      except Exception as e:
         print(f'\n - Error al limpiar el subject: {e} \n')
         sTexto = sSubject
   else:
      sTexto = sSubject
   return sTexto


def fMainLimpiarDatos(dfEmail):
   # Transformar Date "Wed, 14 Sep 2022 17:38:23 +0000 (UTC)" 
   # Obtener columna 'H_M_S'
   dfEmail['Date'] = dfEmail['Date'].apply(fLimpiarFecha)      # Se obtiene "Wed, 14 Sep 2022 17:38:23"
   dfEmail['Date'] = dfEmail['Date'].str.split(', ').str[-1]   # Se obtiene "14 Sep 2022 17:38:23"
   dfEmail['H_M_S'] = dfEmail['Date'].apply(lambda x: x[-8:])  # Se obtiene "17:38:23"
   # Obtener columna 'Hour'
   dfEmail['Hour'] = dfEmail['H_M_S'].apply(lambda x: x[:2]+'h-'+str(int(x[:2])+1).zfill(2)+'h')    # Se obtiene "17h-18h"
   # Obtener columna 'Date'
   dfEmail['Date'] = dfEmail['Date'].apply(lambda x: x[:-9] if len(x[:-9])==11 else '0'+x[:-9] )    # Se obtiene "14 Sep 2022"
   dfEmail['Date'] = pd.to_datetime(dfEmail['Date'], format='%d %b %Y')                             # Se obtiene "2022-09-14"
   # Obtener columna 'WeekDay'
   dfEmail['WeekDay'] = dfEmail['Date'].dt.strftime('%A')                                           # Se obtiene "Wednesday"  

   #########

   # Contar subjects que son NaN
   iSubjectNaN = dfEmail['Subject'].isna().sum()
   print(f'- Número de subjects que son NaN: {iSubjectNaN} \n')

   # Extrae la dirección de correo electrónico de una cadena de texto con formato 'Nombre <correo@example.com>'
   dfEmail['Mail'] = dfEmail['From'].apply(fObtenerCorreoDeFrom)
   # Extrae el nombre de una cadena de texto con formato 'Nombre <correo@example.com>'
   dfEmail['Name'] = dfEmail['From'].apply(fObtenerNombreDeFrom)
   # Limpia el campo de 'Subject' decodificando y eliminando caracteres no deseados
   dfEmail['Subject'] = dfEmail['Subject'].apply(fLimpiarSubject)
   # Se eliminan las columnas 'From' y se reordenan las columnas restantes
   dfEmail = dfEmail.drop(columns=['From'])[['Date','H_M_S','Hour','WeekDay','Mail','Name','Subject']]

###########################################



######### VISUALIZACION DE DATOS ##########

# - Print Datos Basicos
# - Cantidad de Correos por año
# - Cantidad de Correos por mes
# - Cantidad de Correos por dia
# - Cantidad de Correos por hora
# - Top 10 de quien manda mas correos
# - Palabras mas Usadas


def fDatosBasicos(dfEmail):
   pd.set_option('display.max_rows', None)
   pd.set_option('display.max_columns', None)
   print(f"1. dfEmail.iloc: \n {dfEmail.iloc[1:4]} \n") #Ver las primeras 4 lineas del dataframe

   print("---------------------------- \n")

   print(f"2. dfEmail.columns: \n {dfEmail.columns} \n") #Ver las columnas del dataframe

   print("---------------------------- \n")

   print(f"3. dfEmail.describe: \n {dfEmail.describe(include='all')} \n") #Ver datos estadisticos del dataframe

   print("---------------------------- \n")


def fHistplotAnual(dfEmail, iTotalCorreos):
   # Cantidad de Correos por año
   dfEmail['Date'] = pd.to_datetime(dfEmail['Date'])
   lCorreosPorAño = dfEmail.groupby(dfEmail['Date'].dt.year)['Date'].count()
   lCorreosPorAño.plot(kind='bar', xlabel='Año', ylabel='Cantidad de Correos', title=f'Cantidad de Correos por Año - Total: {iTotalCorreos}')
   plt.show()


def fHistplotMensual(dfEmail, iTotalCorreos):
   
   # Crear un diccionario para mapear los números de mes a sus nombres
   dNombreMeses = {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
                  7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}

   # Crear una nueva columna con el nombre del mes
   dfEmail['NombreMes'] = dfEmail['Date'].dt.month.map(dNombreMeses)

   # Crear una nueva columna con el año
   dfEmail['Año'] = dfEmail['Date'].dt.year

   # Agrupar por mes y año, y contar la cantidad de correos
   lCorreosPorMesYAño = dfEmail.groupby(['NombreMes', 'Año']).size().unstack(fill_value=0)

   # Crear un orden para los meses
   orden_meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                  'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
   
   # Reindexar el DataFrame para que los meses estén en el orden correcto
   lCorreosPorMesYAño = lCorreosPorMesYAño.reindex(orden_meses)

   # Graficar
   lCorreosPorMesYAño.plot(kind='bar', stacked=True, cmap='viridis')
   plt.xlabel('Mes')
   plt.ylabel('Cantidad de Correos')
   plt.title(f'Cantidad de Correos por Mes (Dividido por Año) - Total: {iTotalCorreos}')
   plt.title('Cantidad de Correos por Mes (Dividido por Año)')
   plt.xticks(rotation=45)
   plt.legend(title='Año', bbox_to_anchor=(1, 1), loc='upper left') 
   plt.show()


def fHistplotSemanal(dfEmail, iTotalCorreos):
   # Diccionario de traducción de días de la semana
   lTraduccionDias = {
      'Monday': 'Lunes',
      'Tuesday': 'Martes',
      'Wednesday': 'Miércoles',
      'Thursday': 'Jueves',
      'Friday': 'Viernes',
      'Saturday': 'Sábado',
      'Sunday': 'Domingo'
   }

   # Orden de los días de la semana
   lOrdenDiasSemana = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']

   # Aplicar la traducción a la columna WeekDay
   dfEmail['WeekDay'] = dfEmail['WeekDay'].map(lTraduccionDias)
   # Cantidad de Correos por día de la semana
   lCorreosPorDiaSemana = dfEmail.groupby(dfEmail['WeekDay'])['Date'].count()
   # Reindexar la Serie con el orden deseado
   lCorreosPorDiaSemana = lCorreosPorDiaSemana.reindex(lOrdenDiasSemana)
   lCorreosPorDiaSemana.plot(kind='bar', xlabel='Día de la Semana', ylabel='Cantidad de Correos', title=f'Cantidad de Correos por Día de la Semana - Total: {iTotalCorreos}')
   plt.show()


def fHistplotHoras(dfEmail, iTotalCorreos):
   # Extraer la hora de la columna 'H_M_S' y agregar "h" al final
   dfEmail['Hora'] = dfEmail['H_M_S'].str.split(':').str[0].map(lambda x: x + 'h')

   # Cantidad de Correos por hora
   lCorreosPorHora = dfEmail.groupby(dfEmail['Hora'])['Date'].count()
   lCorreosPorHora.plot(kind='bar', xlabel='Hora', ylabel='Cantidad de Correos', title=f'Cantidad de Correos por Hora - Total: {iTotalCorreos}')
   plt.show()


def fHistplotTopRemitentes(dfEmail, iTotalCorreos):
   # Top 10 de quien manda más correos
   lTopRemitentes = dfEmail['Mail'].value_counts().nlargest(20)
   lTopRemitentes.plot(kind='barh', xlabel='Cantidad de Correos', ylabel='Remitente', title=f'Top 20 de Remitentes que Envían más Correos - Total: {iTotalCorreos}')
   plt.gca().invert_yaxis()
   plt.show()


def fTopPalabrasClave(dfEmail):
   # Eliminar emojis y otros caracteres no deseados de la columna "Subject"
   dfEmail['Subject'] = dfEmail['Subject'].astype(str).apply(lambda x: re.sub(r'\W+', ' ', x))

   # Obtener todos los textos de la columna "Subject" y combinarlos en una sola cadena
   sTextoCompleto = ' '.join(dfEmail['Subject'].astype(str).tolist())

   # Pasar las StopWords a minuscula
   lStopWordsLower = {sPalabra.lower() for sPalabra in lStopWords}

   lPalabras = []
   for sPalabra in sTextoCompleto.split():
      if sPalabra.lower() not in lStopWordsLower:
         lPalabras.append(sPalabra)

   # Contar la frecuencia de cada palabra
   lFrecuenciaPalabras = Counter(lPalabras)

   # Seleccionar las palabras más frecuentes
   lTopPalabras = lFrecuenciaPalabras.most_common(iNumPalabrasClave)

   # Extraer palabras y frecuencias
   lPalabrasTop = [p[0] for p in lTopPalabras]
   lFrecuenciasTop = [p[1] for p in lTopPalabras]

   # Graficar
   plt.figure(figsize=(10, 6))
   plt.barh(lPalabrasTop, lFrecuenciasTop)
   plt.xlabel('Frecuencia')
   plt.ylabel('Palabra')
   plt.title('Top Palabras más Frecuentes en Subject (Excluyendo Stopwords)')
   plt.gca().invert_yaxis()  # Invertir el eje y para mostrar las palabras más frecuentes arriba
   plt.show()


   # Escribir las palabras en un archivo de texto
   with open(sRUTA_TOP_PALABRAS_CLAVE, 'w', encoding='utf-8') as f:
      for sPalabra, iFrecuencia in lTopPalabras:
         try:
               f.write(f'{sPalabra}: {iFrecuencia}\n')
         except UnicodeEncodeError:
               pass

#########################################



################# MAIN ##################
bTodoCorrecto = False

if bQuieroScrapear:
   print("\n - Scrapeador Activado - \n")
   if not (os.path.exists(sRUTA_DATOS_EN_BRUTO) and os.path.exists(sRUTA_DATOS_LIMPIOS)): # Compruebo si existe el fichero en bruto y limpio, solo entra en el if cuando no existen los 2
      
      dfEmail = fScrapearCorreos(sMAIL_ACCOUNT, sAPP_PASSWORD_GOOGLE) # Scrapeo Mails
      
      try:
         dfEmail.to_csv(sRUTA_DATOS_EN_BRUTO, index=False, encoding='utf-8')
      except Exception as e:
         print(f'- ERROR: Error al crear el archivo CSV: {e} \n')

      fMainLimpiarDatos(dfEmail) #Limpio Mails

      try:
         dfEmail.to_csv(sRUTA_DATOS_LIMPIOS, index=False, encoding='utf-8')
         bTodoCorrecto = True
      except Exception as e:
         print(f'- ERROR: Error al crear el archivo CSV: {e} \n')

   else:
      print(f"- WARNING: Existe sRUTA_DATOS_EN_BRUTO o sRUTA_DATOS_LIMPIOS. Para usar el modo de Scrapeo la Carpeta 'Data' debe estar limpia.\n Revise estas rutas:\n {sRUTA_DATOS_EN_BRUTO} \n {sRUTA_DATOS_LIMPIOS} \n")


else:
   print("\n - Scrapeador Desactivado - \n")
   if os.path.exists(sRUTA_DATOS_LIMPIOS): # Compruebo si existe el Fichero Limpio

      try:
         dfEmail = pd.read_csv(sRUTA_DATOS_LIMPIOS) # Recojo los datos Limpios
         iTotalCorreos = len(dfEmail)
         print(f'- iTotalCorreos: {iTotalCorreos} \n')
         bTodoCorrecto = True
      except Exception as e:
         print(f'- ERROR: Error al cargar el archivo CSV: {e} \n')

   else:
      print(f"- WARNING: NO se encontro sRUTA_DATOS_LIMPIOS. Para usar este modo debe tener el fichero con los datos Limpios. \n Revise la ruta:\n {sRUTA_DATOS_LIMPIOS} \n")



if bTodoCorrecto:
   fDatosBasicos(dfEmail)
   fHistplotAnual(dfEmail, iTotalCorreos)
   fHistplotMensual(dfEmail, iTotalCorreos)
   fHistplotSemanal(dfEmail, iTotalCorreos)
   fHistplotHoras(dfEmail, iTotalCorreos)
   fHistplotTopRemitentes(dfEmail, iTotalCorreos)
   fTopPalabrasClave(dfEmail)
else:
   print(f"- WARNING: Ocurrio un error y no es posible mostrar las graficas\n")
