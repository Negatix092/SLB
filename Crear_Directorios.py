import pandas as pd
import pyodbc
import os
import re
import shutil
import datetime

# Año de interés definido aquí
now = datetime.datetime.now()
año_interes = now.year
mes_actual = now.month
trimestre_actual = (mes_actual - 1) // 3 + 1
nombre_archivo_errores = f"Archivos_Faltantes_Trimestre_#{trimestre_actual}_{año_interes}.txt "
errores_de_copia = [] #lista para almacenar los errores de copia

# Información de conexión
server = 'ec0038app05'
database = 'SHAYA'
username = 'python_user'
password = 'python_user'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Función para obtener los DataFrames
def obtener_dataframe(query, conn):
    return pd.read_sql(query, conn)

# Función para filtrar el DataFrame por trimestre y WO_NUMBER
def filtrar_por_trimestreCapex(df, meses):
    return df[df['END_WO'].dt.month.isin(meses) & df['WO_NUMBER'].notna() & (df['WO_NUMBER'] != 0)].copy()

# Función para filtrar el DataFrame por trimestre para el caso de CPI
def filtrar_por_trimestreCPI_Opex(df, meses):
    return df[df['END_WO'].dt.month.isin(meses)].copy()

directorio_base_copia = rf'\\dir.slb.com\NSA\SAM_Collaborate\EC0037\00_Well_File'

# Función para obtener rutas de origen 
def obtener_ruta_origen(pozo):
    # Defino un diccionario con los prefijos de los pozos y sus carpetas correspondientes
    carpetas = {'ANC': 'ANACONDA', 'ANR': 'ANURA', 'ACA': 'AUCA CENTRAL', 'ACS': 'AUCA SUR', 'BOA': 'BOA',
                'CHE': 'CHONTA ESTE', 'CHS': 'CHONTA SUR', 'CG': 'CONGA', 'CNO': 'CONONACO', 'CLB': 'CULEBRA',
                'PTL': 'PITALALA', 'RMY': 'RUMIYACU', 'TTS': 'TORTUGA', 'YCA': 'YUCA', 'YLB': 'YULEBRA'}
    
    carpetas_especiales = {'CGSA': os.path.join('CONGA', 'CONGA SUR')}

    # Primero verifica los casos especiales
    if pozo.startswith('CGSA'):
        return os.path.join(directorio_base_copia, carpetas_especiales['CGSA'], pozo)

    # Luego verifica los casos normales
    for prefijo, carpeta in carpetas.items():
        if pozo.startswith(prefijo):
            return os.path.join(directorio_base_copia, carpeta, pozo)
    
    return None

# Funcion para obtener un directorio utilizando el numero de workover
def obtener_directorio_por_workover(ruta_base, wo_number):
    
    # Asegurarse de que wo_number siempre tenga al menos dos dígitos
    #wo_number = str(wo_number).zfill(2)
    wo_number = str(wo_number)
    # Crear la expresión regular
    regex = re.compile(rf'WO.*{wo_number}')

    # Buscar en la carpeta base
    for root, dirs, _ in os.walk(ruta_base):
        for dir in dirs:
            if regex.search(dir):
                return os.path.join(root, dir)
    
    return None

# Implementación de la función para encontrar una carpeta por prefijo
def encontrar_carpeta_por_prefijo(ruta_base, prefijo):
    carpetas = next(os.walk(ruta_base))[1]
    for carpeta in carpetas:
        if carpeta.startswith(prefijo):
            return os.path.join(ruta_base, carpeta)
    return None

# Función modificada para obtener el directorio específico AIS/ARS
def obtener_directorio_AIS(ruta_base, pozo, wo_number):
    wo_number = str(wo_number)
    # Crear la expresión regular    
    regex = re.compile(rf'.*{pozo}.*WO.*{wo_number}.*')

    for root, dirs, _ in os.walk(ruta_base):
        for dir in dirs:
            if regex.search(dir):
                return os.path.join(root, dir)
    
    return None

def obtener_directorio_ARS(ruta_base, pozo):
    # Crear la expresión regular    
    regex = re.compile(rf'.*{pozo}.*ABANDONO.*')

    for root, dirs, _ in os.walk(ruta_base):
        for dir in dirs:
            if regex.search(dir):
                return os.path.join(root, dir)
    
    return None

def obtener_directorio_AIS_CPI(ruta_base, pozo):
    # Crear la expresión regular    
    regex = re.compile(rf'.*{pozo}.*')

    for root, dirs, _ in os.walk(ruta_base):
        for dir in dirs:
            if regex.search(dir):
                return os.path.join(root, dir)
    
    return None

# Funciones para copiar archivos de origen a destino

# Caso en que se desea copiar un archivo pdf (Soporte para Inicio de Trabajos) para Capex y Opex
def copiar_pdf(pozo, wo_number, ruta_destino, tipo_pozo):
    ruta_base_origen = obtener_ruta_origen(pozo)
    ruta_origen_workover = obtener_directorio_por_workover(ruta_base_origen, wo_number)

    if ruta_origen_workover:
        # Encuentra la primera subcarpeta que comienza con "1."
        ruta_primera_subcarpeta = encontrar_carpeta_por_prefijo(ruta_origen_workover, '1.')
        if ruta_primera_subcarpeta:
            # Encuentra la segunda subcarpeta que comienza con "3." dentro de la primera subcarpeta
            ruta_segunda_subcarpeta = encontrar_carpeta_por_prefijo(ruta_primera_subcarpeta, '3.')
            if ruta_segunda_subcarpeta:
                # Encuentra la tercera subcarpeta que comienza con "1." dentro de la segunda subcarpeta
                ruta_tercera_subcarpeta = encontrar_carpeta_por_prefijo(ruta_segunda_subcarpeta, '1.')
                if ruta_tercera_subcarpeta:
                    copiado = False
                    for archivo in os.listdir(ruta_tercera_subcarpeta):
                        if archivo.endswith('.pdf') or archivo.endswith('.PDF'):
                            shutil.copy(os.path.join(ruta_tercera_subcarpeta, archivo), ruta_destino)
                            print(f"Archivo {archivo} copiado al pozo {pozo} con WO {wo_number}")
                            copiado = True
                            break
                    if not copiado:
                        errores_de_copia.append(f"No se encontró un archivo de Notificación para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
                
                else:
                    errores_de_copia.append(f"No se encontró la subcarpeta '1. Permisos PAM-SHE' para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")

            else:
                errores_de_copia.append(f"No se encontró la subcarpeta '3. Actas y Oficios' para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
        else:
            errores_de_copia.append(f"No se encontró la subcarpeta '1. Propuesta Tecnica' para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
    else:
        errores_de_copia.append(f"No se encontró la carpeta del Workover {wo_number} para el pozo {pozo}. Tipo de actividad: {tipo_pozo}.")

# Caso en que se desea copiar un archivo pdf (Soporte para Inicio de Trabajos) para CPI
def copiar_pdf_cpi(pozo, ruta_destino):
    ruta_base_origen = obtener_ruta_origen(pozo)
    ruta_ocpi = os.path.join(ruta_base_origen, 'CPI')

    if ruta_ocpi:
        # Encuentra la primera subcarpeta que comienza con "1."
        ruta_primera_subcarpeta = encontrar_carpeta_por_prefijo(ruta_ocpi, '1.')
        if ruta_primera_subcarpeta:
            # Encuentra la segunda subcarpeta que comienza con "3." dentro de la primera subcarpeta
            ruta_segunda_subcarpeta = encontrar_carpeta_por_prefijo(ruta_primera_subcarpeta, '3.')
            if ruta_segunda_subcarpeta:
                # Encuentra la tercera subcarpeta que comienza con "1." dentro de la segunda subcarpeta
                ruta_tercera_subcarpeta = encontrar_carpeta_por_prefijo(ruta_segunda_subcarpeta, '1.')
                if ruta_tercera_subcarpeta:
                    copiado = False
                    for archivo in os.listdir(ruta_tercera_subcarpeta):
                        if archivo.endswith('.pdf') or archivo.endswith('.PDF'):
                            shutil.copy(os.path.join(ruta_tercera_subcarpeta, archivo), ruta_destino)
                            #print(f"Archivo {archivo} copiado a {ruta_destino}")
                            print(f"Archivo {archivo} copiado al pozo {pozo}. Tipo de actividad: CPI.")
                            copiado = True
                            break
                    if not copiado:
                        errores_de_copia.append(f"No se encontró un archivo de Notificación para el pozo {pozo}. Tipo de actividad: CPI.")
                else:
                    errores_de_copia.append(f"No se encontró la subcarpeta '1. Permisos PAM-SHE' para el pozo {pozo}. Tipo de actividad: CPI.")
            else:
                errores_de_copia.append(f"No se encontró la subcarpeta '3. Actas y Oficios' para el pozo {pozo}. Tipo de actividad: CPI.")
        else:
            errores_de_copia.append(f"No se encontró la subcarpeta '1. Propuesta Tecnica' para el pozo {pozo}. Tipo de actividad: CPI.")
    else:
        errores_de_copia.append(f"No se encontró la carpeta CPI para el pozo {pozo}. Tipo de actividad: CPI.")

# Caso en que se desea copiar un archivo word (Prognosis y Programas) para Capex y Opex
def copiar_prognosis_capex(pozo, wo_number, ruta_destino, tipo_pozo):
    ruta_base_origen = obtener_ruta_origen(pozo)
    ruta_origen_workover = obtener_directorio_por_workover(ruta_base_origen, wo_number)

    if ruta_origen_workover:
        # Encuentra la primera subcarpeta que comienza con "1."
        ruta_primera_subcarpeta = encontrar_carpeta_por_prefijo(ruta_origen_workover, '1.')
        if ruta_primera_subcarpeta:
            # Encuentra la segunda subcarpeta que comienza con "2." dentro de la primera subcarpeta
            ruta_segunda_subcarpeta = encontrar_carpeta_por_prefijo(ruta_primera_subcarpeta, '2.')
            if ruta_segunda_subcarpeta:
                copiado = False
                for archivo in os.listdir(ruta_segunda_subcarpeta):
                    #print(os.path.join(ruta_segunda_subcarpeta, archivo))
                    if archivo.endswith('.docx') or archivo.endswith('.doc') or archivo.endswith('.DOCX') or archivo.endswith('.DOC'):
                        shutil.copy(os.path.join(ruta_segunda_subcarpeta, archivo), ruta_destino)
                        #print(f"Archivo {archivo} copiado a {ruta_destino}")
                        print(f"Archivo {archivo} copiado al pozo {pozo} con WO {wo_number}")
                        copiado = True
                        break
                if not copiado:
                    errores_de_copia.append(f"No se encontró un archivo de Propuesta Técnica para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
            else:
                errores_de_copia.append(f"No se encontró la subcarpeta '2. Propuesta Documentos Finales' para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
        else:
            errores_de_copia.append(f"No se encontró la subcarpeta '1. Propuesta Tecnica' para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
    else:
        errores_de_copia.append(f"No se encontró la carpeta del Workover {wo_number} para el pozo {pozo}. Tipo de actividad: {tipo_pozo}.")

# Caso en que se desea copiar un archivo word (Prognosis y Programas) para CPI
def copiar_prognosis_cpi(pozo, ruta_destino):
    ruta_base_origen = obtener_ruta_origen(pozo)
    ruta_ocpi = os.path.join(ruta_base_origen, 'CPI')

    if ruta_ocpi:
        # Encuentra la primera subcarpeta que comienza con "1."
        ruta_primera_subcarpeta = encontrar_carpeta_por_prefijo(ruta_ocpi, '1.')
        if ruta_primera_subcarpeta:
            # Encuentra la segunda subcarpeta que comienza con "2." dentro de la primera subcarpeta
            ruta_segunda_subcarpeta = encontrar_carpeta_por_prefijo(ruta_primera_subcarpeta, '2.')
            if ruta_segunda_subcarpeta:
                copiado = False
                for archivo in os.listdir(ruta_segunda_subcarpeta):
                    if archivo.endswith('.docx') or archivo.endswith('.doc') or archivo.endswith('.DOCX') or archivo.endswith('.DOC'):
                        shutil.copy(os.path.join(ruta_segunda_subcarpeta, archivo), ruta_destino)
                        #print(f"Archivo {archivo} copiado a {ruta_destino}")
                        print(f"Archivo {archivo} copiado al pozo {pozo}.")
                        copiado = True
                        break
                if not copiado:
                    errores_de_copia.append(f"No se encontró un archivo de Propuesta Técnica para el pozo {pozo}. Tipo de actividad: CPI.")
            else:
                errores_de_copia.append(f"No se encontró la subcarpeta '2. Propuesta Documentos Finales' para el pozo {pozo}. Tipo de actividad: CPI.")
        else:
            errores_de_copia.append(f"No se encontró la subcarpeta '1. Propuesta Tecnica' para el pozo {pozo}. Tipo de actividad: CPI.")
    else:
        errores_de_copia.append(f"No se encontró la carpeta CPI para el pozo {pozo}. Tipo de actividad: CPI.")

# Caso en que se desea copiar todos los archivos pdf (Reportes Diarios de Ejecución) para Capex y Opex
def copiar_reportes_diarios(pozo, wo_number, ruta_destino, tipo_pozo):
    num_archivos_copiados = 0
    ruta_base_origen = obtener_ruta_origen(pozo)
    ruta_origen_workover = obtener_directorio_por_workover(ruta_base_origen, wo_number)

    if ruta_origen_workover:
        # Encuentra la primera subcarpeta que comienza con "2."
        ruta_primera_subcarpeta = encontrar_carpeta_por_prefijo(ruta_origen_workover, '2.')
        if ruta_primera_subcarpeta:
            # Encuentra la segunda subcarpeta que comienza con "3." dentro de la primera subcarpeta
            ruta_segunda_subcarpeta = encontrar_carpeta_por_prefijo(ruta_primera_subcarpeta, '3.')
            if ruta_segunda_subcarpeta:
                copiado = False
                for archivo in os.listdir(ruta_segunda_subcarpeta):
                    if archivo.endswith('.pdf') or archivo.endswith('.PDF'):
                        shutil.copy(os.path.join(ruta_segunda_subcarpeta, archivo), ruta_destino)
                        copiado = True
                        num_archivos_copiados += 1
                print(f"{num_archivos_copiados} Archivos de Registro Diario copiados al pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
                if not copiado:
                    errores_de_copia.append(f"No se encontró un archivo de Reportes Diarios para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
            else:
                errores_de_copia.append(f"No se encontró la subcarpeta '3. Reportes Diarios' para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
        else:
            errores_de_copia.append(f"No se encontró la subcarpeta '2. Ejecucion' para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
    else:
        errores_de_copia.append(f"No se encontró la carpeta del Workover {wo_number} para el pozo {pozo}. Tipo de actividad: {tipo_pozo}.")

# Caso en que se desea copiar todos los archivos pdf (Reportes Diarios de Ejecución) para CPI
def copiar_reportes_diarios_cpi(pozo, ruta_destino):
    num_archivos_copiados = 0
    ruta_base_origen = obtener_ruta_origen(pozo)
    ruta_ocpi = os.path.join(ruta_base_origen, 'CPI')

    if ruta_ocpi:
        # Encuentra la primera subcarpeta que comienza con "2."
        ruta_primera_subcarpeta = encontrar_carpeta_por_prefijo(ruta_ocpi, '2.')
        if ruta_primera_subcarpeta:
            # Encuentra la segunda subcarpeta que comienza con "3." dentro de la primera subcarpeta
            ruta_segunda_subcarpeta = encontrar_carpeta_por_prefijo(ruta_primera_subcarpeta, '3.')
            if ruta_segunda_subcarpeta:
                copiado = False
                for archivo in os.listdir(ruta_segunda_subcarpeta):
                    if archivo.endswith('.pdf') or archivo.endswith('.PDF'):
                        shutil.copy(os.path.join(ruta_segunda_subcarpeta, archivo), ruta_destino)
                        copiado = True
                        num_archivos_copiados += 1
                print(f"{num_archivos_copiados} Archivos de registro diario copiados al pozo {pozo}. Tipo de actividad: CPI.")
                if not copiado:
                    errores_de_copia.append(f"No se encontró un archivo de Reportes Diarios para el pozo {pozo}. Tipo de actividad: CPI.")
            else:
                errores_de_copia.append(f"No se encontró la subcarpeta '3. Reportes Diarios' para el pozo {pozo}. Tipo de actividad: CPI.")
        else:
            errores_de_copia.append(f"No se encontró la subcarpeta '2. Ejecucion' para el pozo {pozo}. Tipo de actividad: CPI.")
    else:
        errores_de_copia.append(f"No se encontró la carpeta CPI para el pozo {pozo}.")

# Caso en que se desea copiar un archivo excel (Sumarios Finales) para Capex y Opex
def copiar_sumario(pozo, wo_number, ruta_destino, tipo_pozo):
    ruta_base_origen = obtener_ruta_origen(pozo)
    ruta_origen_workover = obtener_directorio_por_workover(ruta_base_origen, wo_number)

    if ruta_origen_workover:
        # Encuentra la primera subcarpeta que comienza con "2."
        ruta_primera_subcarpeta = encontrar_carpeta_por_prefijo(ruta_origen_workover, '2.')
        if ruta_primera_subcarpeta:
            # Encuentra la segunda subcarpeta que comienza con "6." dentro de la primera subcarpeta
            ruta_segunda_subcarpeta = encontrar_carpeta_por_prefijo(ruta_primera_subcarpeta, '6.')
            if ruta_segunda_subcarpeta:
                copiado = False
                for archivo in os.listdir(ruta_segunda_subcarpeta):
                    if archivo.endswith('.xlsx') or archivo.endswith('.xls') or archivo.endswith('.XLSX') or archivo.endswith('.XLS'):
                        shutil.copy(os.path.join(ruta_segunda_subcarpeta, archivo), ruta_destino)
                        print(f"Sumario {archivo} copiado al pozo {pozo} con WO {wo_number}")
                        copiado = True
                        break
                if not copiado:
                    errores_de_copia.append(f"No se encontró un archivo Sumario para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
            else:
                errores_de_copia.append(f"No se encontró la subcarpeta '6. Sumario' para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}..")
        else:
            errores_de_copia.append(f"No se encontró la subcarpeta '2. Ejecucion' para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
    else:
        errores_de_copia.append(f"No se encontró la carpeta del Workover {wo_number} para el pozo {pozo}. Tipo de actividad: {tipo_pozo}.")

# Caso en que se desea copiar un archivo excel (Sumarios Finales) para CPI
def copiar_sumario_cpi(pozo, ruta_destino):
    ruta_base_origen = obtener_ruta_origen(pozo)
    ruta_ocpi = os.path.join(ruta_base_origen, 'CPI')

    if ruta_ocpi:
        # Encuentra la primera subcarpeta que comienza con "2."
        ruta_primera_subcarpeta = encontrar_carpeta_por_prefijo(ruta_ocpi, '2.')
        if ruta_primera_subcarpeta:
            # Encuentra la segunda subcarpeta que comienza con "6." dentro de la primera subcarpeta
            ruta_segunda_subcarpeta = encontrar_carpeta_por_prefijo(ruta_primera_subcarpeta, '6.')
            if ruta_segunda_subcarpeta:
                copiado = False
                for archivo in os.listdir(ruta_segunda_subcarpeta):
                    if archivo.endswith('.xlsx') or archivo.endswith('.xls') or archivo.endswith('.XLSX') or archivo.endswith('.XLS'):
                        shutil.copy(os.path.join(ruta_segunda_subcarpeta, archivo), ruta_destino)
                        print(f"Archivo {archivo} copiado al pozo {pozo}.")
                        copiado = True
                        break
                if not copiado:
                    errores_de_copia.append(f"No se encontró un archivo de Sumario para el pozo {pozo}. Tipo de actividad: CPI.")
            else:
                errores_de_copia.append(f"No se encontró la subcarpeta '6. Sumario' para el pozo {pozo}. Tipo de actividad: CPI.")
        else:
            errores_de_copia.append(f"No se encontró la subcarpeta '2. Ejecucion' para el pozo {pozo}. Tipo de actividad: CPI.")
    else:
        errores_de_copia.append(f"No se encontró la carpeta CPI para el pozo {pozo}. Tipo de actividad: CPI.")

# Caso en que se desea copiar archivos pdf de AIS desde OneDrive
        
def copiar_AIS(pozo, año_inicio, wo_number, ruta_destino, tipo_pozo):
    años = [str(año_inicio), str(año_inicio -1)] # Se busca en el año de interés y en el anterior
    encontrado = False # Bandera para saber si se encontró el archivo

    for año in años:
        if encontrado: # Si ya se encontró el archivo, se sale del bucle
            break

        if tipo_pozo == 'Capex' or tipo_pozo == 'Opex':
            wo_number = str(wo_number)
    
        # Obtener el nombre de usuario del sistema operativo
        usuario = os.getlogin()
        ruta_origen_onedrive = rf'C:\Users\{usuario}\OneDrive - SLB\Actas de Inicio y Recepcion Operaciones'
        ruta_base_AIS = os.path.join(ruta_origen_onedrive, f'AIS OPR {año}')
    
        # Caso en que el pozo es del tipo Capex
        if tipo_pozo == 'Capex':
            ruta_siguiente = encontrar_carpeta_por_prefijo(ruta_base_AIS, '3.')
            ruta_especifica = obtener_directorio_AIS(ruta_siguiente, pozo, wo_number)

        elif tipo_pozo == 'Opex':
            ruta_siguiente = encontrar_carpeta_por_prefijo(ruta_base_AIS, '4.')
            ruta_especifica = obtener_directorio_AIS(ruta_siguiente, pozo, wo_number)

        elif tipo_pozo == 'CPI':
            ruta_siguiente = encontrar_carpeta_por_prefijo(ruta_base_AIS, '2.')
            ruta_especifica = obtener_directorio_AIS_CPI(ruta_siguiente, pozo)
        
        if ruta_especifica:
            archivos = [archivo for archivo in os.listdir(ruta_especifica) if archivo.endswith('.pdf') or archivo.endswith('.PDF')]
        
            # Filtramos por 'consolidado' y 'signed' en el nombre del archivo, y ordenamos por la cantidad de 'signed' en el nombre
            archivos_filtrados = sorted(
                [archivo for archivo in archivos if 'consolidado' in archivo.lower() and 'signed' in archivo.lower()],
                key = lambda x: x.count('signed'), reverse=True
            )

            if archivos_filtrados:
                archivo = archivos_filtrados[0] # Tomamos el archivo con más 'signed' en el nombre
                shutil.copy(os.path.join(ruta_especifica, archivo), ruta_destino)
                print(f"AIS {archivo} copiado al pozo {pozo} con WO {wo_number}.")
                encontrado = True

            else:
                if tipo_pozo == 'CPI':
                    errores_de_copia.append(f"No se encontró un archivo de AIS consolidado y firmado para el pozo {pozo}. Tipo de actividad: CPI.")
                else:
                    errores_de_copia.append(f"No se encontró un archivo de AIS consolidado y firmado para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")

        else:
            if tipo_pozo == 'CPI':
                errores_de_copia.append(f"No se encontró la carpeta de AIS para el pozo {pozo}. Tipo de actividad: CPI.")
            else:
               errores_de_copia.append(f"No se encontró la carpeta de AIS para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
    
    if not encontrado:
        if tipo_pozo == 'CPI':
            errores_de_copia.append(f"No se encontró un archivo de AIS consolidado y firmado para el pozo {pozo} en los años {año_inicio} o {año_inicio -1}. Tipo de actividad: CPI.")
        else:
            errores_de_copia.append(f"No se encontró un archivo de AIS consolidado y firmado para el pozo {pozo} con WO {wo_number} en los años {año_inicio} o {año_inicio -1}. Tipo de actividad: {tipo_pozo}.")
        

# Caso en que se desea copiar archivos pdf de ARS desde OneDrive
def copiar_ARS_Opex(pozo, año_fin, wo_number, ruta_destino, abandono):
    años = [str(año_fin), str(año_fin + 1)] # Se busca en el año de interés y en el siguiente
    encontrado = False # Bander para saber si se encontró el archvio 
    for año in años:
        if encontrado: # Si ya se encontró el archivo, se sale del bucle
            break

        wo_number = str(wo_number)
    
        # Obtener el nombre de usuario del sistema operativo
        usuario = os.getlogin()
        ruta_origen_onedrive = rf'C:\Users\{usuario}\OneDrive - SLB\Actas de Inicio y Recepcion Operaciones'
        ruta_base_ARS = os.path.join(ruta_origen_onedrive, f'ACTAS DE RECEPCION DE OPERACIONES {año}')
    
        # Caso en que el pozo es del tipo Capex
        if 'adecuacion' in abandono.lower():
            ruta_siguiente = obtener_directorio_ARS(ruta_base_ARS, pozo)
        else:
            ruta_siguiente = obtener_directorio_AIS(ruta_base_ARS, pozo, wo_number)
            
        
        if ruta_siguiente:
            archivos = [archivo for archivo in os.listdir(ruta_siguiente) if archivo.endswith('.pdf') or archivo.endswith('.PDF')]
        
            # Filtramos por 'consolidado' y 'signed' en el nombre del archivo, y ordenamos por la cantidad de 'signed' en el nombre
            archivos_filtrados = sorted(
                [archivo for archivo in archivos if 'consolidado' in archivo.lower() and 'signed' in archivo.lower()],
                key = lambda x: x.count('signed'), reverse=True
            )

            if archivos_filtrados:
                archivo = archivos_filtrados[0] # Tomamos el archivo con más 'signed' en el nombre
                shutil.copy(os.path.join(ruta_siguiente, archivo), ruta_destino)
                print(f"ARS {archivo} copiado al pozo {pozo} con WO {wo_number}.")
                encontrado = True

            else:
                if 'adecuacion' in abandono.lower():
                    errores_de_copia.append(f"No se encontró un archivo de ARS consolidado y firmado para el pozo {pozo}. Tipo de actividad: Adecuación.")
                else:
                    errores_de_copia.append(f"No se encontró un archivo de ARS consolidado y firmado para el pozo {pozo} con WO {wo_number}. Tipo de actividad: Opex.")

        else:
            if 'adecuacion' in abandono.lower():
                errores_de_copia.append(f"No se encontró la carpeta de ARS para el pozo {pozo}. Tipo de actividad: Adecuación.")
            else:
                errores_de_copia.append(f"No se encontró la carpeta de ARS para el pozo {pozo} con WO {wo_number}. Tipo de actividad: Opex.")
    
    if not encontrado:
        if 'adecuacion' in abandono.lower():
            errores_de_copia.append(f"No se encontró un archivo de ARS consolidado y firmado para el pozo {pozo} en los años {año_fin} o {año_fin + 1}. Tipo de actividad: Adecuación.")
        else:
            errores_de_copia.append(f"No se encontró un archivo de ARS consolidado y firmado para el pozo {pozo} con WO {wo_number} en los años {año_fin} o {año_fin + 1}. Tipo de actividad: Opex.")

def copiar_ARS(pozo, año_fin, wo_number, ruta_destino, tipo_pozo):
    años = [str(año_fin), str(año_fin + 1)] # Se busca en el año de interés y en el siguiente
    encontrado = False # Bander para saber si se encontró el archvio 
    for año in años:
        if encontrado: # Si ya se encontró el archivo, se sale del bucle
            break
    
        # Obtener el nombre de usuario del sistema operativo
        usuario = os.getlogin()
        ruta_origen_onedrive = rf'C:\Users\{usuario}\OneDrive - SLB\Actas de Inicio y Recepcion Operaciones'
        ruta_base_ARS = os.path.join(ruta_origen_onedrive, f'ACTAS DE RECEPCION DE OPERACIONES {año}')
    
        if tipo_pozo == 'Capex':
            wo_number = str(wo_number)
            ruta_siguiente = obtener_directorio_AIS(ruta_base_ARS, pozo, wo_number)
        
        elif tipo_pozo == 'CPI':
            ruta_siguiente = obtener_directorio_AIS_CPI(ruta_base_ARS, pozo)
        
        if ruta_siguiente:
            archivos = [archivo for archivo in os.listdir(ruta_siguiente) if archivo.endswith('.pdf') or archivo.endswith('.PDF')]
        
            # Filtramos por 'consolidado' y 'signed' en el nombre del archivo, y ordenamos por la cantidad de 'signed' en el nombre
            archivos_filtrados = sorted(
                [archivo for archivo in archivos if 'consolidado' in archivo.lower() and 'signed' in archivo.lower()],
                key = lambda x: x.count('signed'), reverse=True
            )

            if archivos_filtrados:
                archivo = archivos_filtrados[0] # Tomamos el archivo con más 'signed' en el nombre
                shutil.copy(os.path.join(ruta_siguiente, archivo), ruta_destino)
                print(f"ARS {archivo} copiado al pozo {pozo} con WO {wo_number}.")
                encontrado = True

            else:
                if tipo_pozo == 'CPI':
                    errores_de_copia.append(f"No se encontró un archivo de ARS consolidado y firmado para el pozo {pozo}. Tipo de actividad: CPI.")
                else:
                    errores_de_copia.append(f"No se encontró un archivo de ARS consolidado y firmado para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")

        else:
            if tipo_pozo == 'CPI':
                errores_de_copia.append(f"No se encontró la carpeta de ARS para el pozo {pozo}. Tipo de actividad: CPI.")
            else:
                errores_de_copia.append(f"No se encontró la carpeta de ARS para el pozo {pozo} con WO {wo_number}. Tipo de actividad: {tipo_pozo}.")
    
    if not encontrado:
        if tipo_pozo == 'CPI':
            errores_de_copia.append(f"No se encontró un archivo de ARS consolidado y firmado para el pozo {pozo} en los años {año_fin} o {año_fin + 1}. Tipo de actividad: CPI.")
        else:
            errores_de_copia.append(f"No se encontró un archivo de ARS consolidado y firmado para el pozo {pozo} con WO {wo_number} en los años {año_fin} o {año_fin + 1}. Tipo de actividad: {tipo_pozo}.")


# Función para crear directorios
def crear_directorios(df, ruta_trimestre, tipo_actividad):
    os.makedirs(ruta_trimestre, exist_ok=True)

    if tipo_actividad == 'CPI':
        ruta_completacion = os.path.join(ruta_trimestre, 'CPI')
        os.makedirs(ruta_completacion, exist_ok=True)
        for _, fila in df.iterrows():
            año_inicio = fila['START_WO'].year
            año_fin = fila['END_WO'].year
            pozo = fila['ITEM_NAME']
            nombre_carpeta_pozo = f'{pozo}'
            ruta_carpeta_pozo = os.path.join(ruta_completacion, nombre_carpeta_pozo)
            os.makedirs(ruta_carpeta_pozo, exist_ok=True)

            # Crear subcarpetas dentro de la carpeta del pozo
            subcarpetas = ['1.1 Prognosis y Programas', '1.2 Soporte para inicio de Trabajos', 
                       '1.3 Reportes diarios de ejecución', '1.4 Sumarios Finales CPI', 
                       '1.5 Actas de Inicio y Recepción']
            for subcarpeta in subcarpetas:
                os.makedirs(os.path.join(ruta_carpeta_pozo, subcarpeta), exist_ok=True)

            # Llamar a copiar_prognosis_cpi para copiar el archivo en la subcarpeta 1.1 Prognosis y Programas
            ruta_destino_prognosis = os.path.join(ruta_carpeta_pozo, '1.1 Prognosis y Programas')
            #print(f"Ruta destino: {ruta_destino_prognosis}")
            copiar_prognosis_cpi(pozo, ruta_destino_prognosis)

            # Llamar a copiar_pdf_cpi para copiar el archivo en la subcarpeta 1.2 Soporte para inicio de Trabajos
            ruta_destino_soporte = os.path.join(ruta_carpeta_pozo, '1.2 Soporte para inicio de Trabajos')
            copiar_pdf_cpi(pozo, ruta_destino_soporte)

            # Llamar a copiar_reportes_diarios_cpi para copiar el archivo en la subcarpeta 1.3 Reportes diarios de ejecución
            ruta_destino_reportes = os.path.join(ruta_carpeta_pozo, '1.3 Reportes diarios de ejecución')
            copiar_reportes_diarios_cpi(pozo, ruta_destino_reportes)

            # Llamar a copiar_sumario_cpi para copiar el archivo en la subcarpeta 1.4 Sumarios Finales CPI
            ruta_destino_sumario = os.path.join(ruta_carpeta_pozo, '1.4 Sumarios Finales CPI')
            copiar_sumario_cpi(pozo, ruta_destino_sumario)

            # Llamar a copiar_AIS para copiar el archivo en la subcarpeta 1.5 Actas de Inicio y Recepción
            ruta_destino_AIS = os.path.join(ruta_carpeta_pozo, '1.5 Actas de Inicio y Recepción', 'AIS')
            os.makedirs(ruta_destino_AIS, exist_ok=True)
            copiar_AIS(pozo, año_inicio, None, ruta_destino_AIS, 'CPI')

            # Llamar a copiar_ARS para copiar el archivo en la subcarpeta 1.5 Actas de Inicio y Recepción
            ruta_destino_ARS = os.path.join(ruta_carpeta_pozo, '1.5 Actas de Inicio y Recepción', 'ARS')
            os.makedirs(ruta_destino_ARS, exist_ok=True)
            copiar_ARS(pozo, año_fin, None, ruta_destino_ARS, 'CPI')

    elif tipo_actividad == 'OPEX':
        for _, fila in df.iterrows():
            año_inicio = fila['START_WO'].year
            año_fin = fila['END_WO'].year
            pozo = fila['ITEM_NAME']
            wo_number = fila['WO_NUMBER'] if pd.notna(fila['WO_NUMBER']) else None
            abandono = fila['WO_OPEX_TEXT'] if pd.notna(fila['WO_OPEX_TEXT']) else ""

            if 'adecuación' in abandono.lower():
                nombre_carpeta_pozo = f'{pozo} (Adecuación)'
            elif wo_number is not None:
                nombre_carpeta_pozo = f'{pozo} WO {int(wo_number)}'
            else:
                nombre_carpeta_pozo = f'{pozo}'
            ruta_carpeta_pozo = os.path.join(ruta_trimestre, nombre_carpeta_pozo)
            os.makedirs(ruta_carpeta_pozo, exist_ok=True)

            # Crear subcarpetas dentro de la carpeta del pozo
            subcarpetas = ['3.1 Prognosis y Programas', '3.2 Soporte para inicio de Trabajos', 
                           '3.3 Reportes diarios de ejecución', '3.4 Diagramas Mecánicos', 
                           '3.5 Registros', '3.6 Sumarios Finales', '3.7 Post Mortem Pozos', 
                           '3.8 Actas de Inicio y Recepción']
            for subcarpeta in subcarpetas:
                ruta_subcarpeta = os.path.join(ruta_carpeta_pozo, subcarpeta)
                os.makedirs(ruta_subcarpeta, exist_ok=True)

            # Llamar a copiar_archivo_prognosis para copiar el archivo en la subcarpeta 3.1 Prognosis y Programas
            ruta_destino_prognosis = os.path.join(ruta_carpeta_pozo, '3.1 Prognosis y Programas')
            copiar_prognosis_capex(pozo, int(wo_number), ruta_destino_prognosis, "Opex")

            # Llamar a copiar_pdf para copiar el archivo en la subcarpeta 3.2 Soporte para inicio de Trabajos
            ruta_destino_soporte = os.path.join(ruta_carpeta_pozo, '3.2 Soporte para inicio de Trabajos')
            copiar_pdf(pozo, int(wo_number), ruta_destino_soporte, "Opex")

            # Llamar a copiar_reportes_diarios para copiar el archivo en la subcarpeta 3.3 Reportes diarios de ejecución
            ruta_destino_reportes = os.path.join(ruta_carpeta_pozo, '3.3 Reportes diarios de ejecución')
            copiar_reportes_diarios(pozo, int(wo_number), ruta_destino_reportes, "Opex")

            # Llamar a copiar_sumario para copiar el archivo en la subcarpeta 3.6 Sumarios Finales
            ruta_destino_sumario = os.path.join(ruta_carpeta_pozo, '3.6 Sumarios Finales')
            copiar_sumario(pozo, int(wo_number), ruta_destino_sumario, "Opex")

            # Llamar a copiar_AIS para copiar el archivo en la subcarpeta 3.8 Actas de Inicio y Recepción
            ruta_destino_AIS = os.path.join(ruta_carpeta_pozo, '3.8 Actas de Inicio y Recepción', 'AIS')
            os.makedirs(ruta_destino_AIS, exist_ok=True)
            copiar_AIS(pozo, año_inicio, int(wo_number), ruta_destino_AIS, 'Opex')

            # Llamar a copiar_ARS para copiar el archivo en la subcarpeta 3.8 Actas de Inicio y Recepción
            ruta_destino_ARS = os.path.join(ruta_carpeta_pozo, '3.8 Actas de Inicio y Recepción', 'ARS')
            os.makedirs(ruta_destino_ARS, exist_ok=True)
            copiar_ARS_Opex(pozo, año_fin, int(wo_number), ruta_destino_ARS, abandono)
    
    elif tipo_actividad == 'CAPEX':
        for _, fila in df.iterrows():
            año_inicio = fila['START_WO'].year
            año_fin = fila['END_WO'].year
            pozo = fila['ITEM_NAME']
            wo_number = int(fila['WO_NUMBER'])
            nombre_carpeta_pozo = f'{pozo} WO {wo_number}'
            ruta_carpeta_pozo = os.path.join(ruta_trimestre, nombre_carpeta_pozo)
            os.makedirs(ruta_carpeta_pozo, exist_ok=True)

            # Crear subcarpetas dentro de la carpeta del pozo
            subcarpetas = ['2.1 Prognosis y Programas', '2.2 Soporte para inicio de Trabajos', 
                       '2.3 Reportes diarios de ejecución',  '2.4 Sumarios Finales WO', 
                       '2.5 Actas de Inicio y Recepción']
            for subcarpeta in subcarpetas:
                ruta_subcarpeta = os.path.join(ruta_carpeta_pozo, subcarpeta)
                os.makedirs(ruta_subcarpeta, exist_ok=True)

            # Llamar a copiar_archivo_prognosis para copiar el archivo en la subcarpeta 2.1 Prognosis y Programas
            ruta_destino_prognosis = os.path.join(ruta_carpeta_pozo, '2.1 Prognosis y Programas')
            copiar_prognosis_capex(pozo, wo_number, ruta_destino_prognosis, "Capex")

            # Llamar a copiar_pdf para copiar el archivo en la subcarpeta 2.2 Soporte para inicio de Trabajos
            ruta_destino_soporte = os.path.join(ruta_carpeta_pozo, '2.2 Soporte para inicio de Trabajos')
            copiar_pdf(pozo, wo_number, ruta_destino_soporte, "Capex")

            # Llamar a copiar_reportes_diarios para copiar el archivo en la subcarpeta 2.3 Reportes diarios de ejecución
            ruta_destino_reportes = os.path.join(ruta_carpeta_pozo, '2.3 Reportes diarios de ejecución')
            copiar_reportes_diarios(pozo, wo_number, ruta_destino_reportes, "Capex")

            # Llamar a copiar_sumario para copiar el archivo en la subcarpeta 2.4 Sumarios Finales WO
            ruta_destino_sumario = os.path.join(ruta_carpeta_pozo, '2.4 Sumarios Finales WO')
            copiar_sumario(pozo, wo_number, ruta_destino_sumario, "Capex")

            # Llamar a copiar_AIS para copiar el archivo en la subcarpeta 2.5 Actas de Inicio y Recepción
            ruta_destino_AIS = os.path.join(ruta_carpeta_pozo, '2.5 Actas de Inicio y Recepción', 'AIS')
            os.makedirs(ruta_destino_AIS, exist_ok=True)
            copiar_AIS(pozo, año_inicio, wo_number, ruta_destino_AIS, 'Capex')

            # Llamar a copiar_ARS para copiar el archivo en la subcarpeta 2.5 Actas de Inicio y Recepción
            ruta_destino_ARS = os.path.join(ruta_carpeta_pozo, '2.5 Actas de Inicio y Recepción', 'ARS')
            os.makedirs(ruta_destino_ARS, exist_ok=True)
            copiar_ARS(pozo, año_fin, wo_number, ruta_destino_ARS, 'Capex')

# Conexión a la base de datos y obtención de los DataFrames
conn = pyodbc.connect(conn_str)

# Consulta SQL
queryO = f""" 
SELECT ITEM_NAME, START_WO, END_WO, PLAN_TYPE_TEXT, WO_NUMBER, WO_OPEX_TEXT
FROM VT_WELLJOBLOG_en_US
WHERE YEAR(END_WO) = {año_interes} AND PLAN_TYPE_TEXT = 'Opex' AND VERIFIED = 'True'
"""

queryC = f""" 
SELECT ITEM_NAME, START_WO, END_WO, PLAN_TYPE_TEXT, WO_NUMBER
FROM VT_WELLJOBLOG_en_US
WHERE YEAR(END_WO) = {año_interes} AND PLAN_TYPE_TEXT = 'Capex' AND VERIFIED = 'True'
"""

queryCompletacion = f"""
SELECT ITEM_NAME, START_WO, END_WO, WELL_STATUS_TEXT, WO_NUMBER
FROM VT_WELLJOBLOG_en_US
WHERE YEAR(END_WO) = {año_interes} AND WELL_STATUS_TEXT = 'CPI' AND VERIFIED = 'True'
"""

dfO = obtener_dataframe(queryO, conn)
dfC = obtener_dataframe(queryC, conn)
dfCo = obtener_dataframe(queryCompletacion, conn)
conn.close()

# Directorio base
directorio_base = rf'\\dir.slb.com\NSA\SAM_Collaborate\EC0037\90_Public\Tests\V_1\01 INFORMES TRIMESTRALES'
ruta_año = os.path.join(directorio_base, f'RT {año_interes}')

# Filtrar DataFrames por trimestre
trimestres = [(1, 2, 3), (4, 5, 6), (7, 8, 9), (10, 11, 12)]
nombres_trimestres = [f'RT #1 {año_interes}', f'RT #2 {año_interes}', 
                      f'RT #3 {año_interes}', f'RT #4 {año_interes}']

for i, meses in enumerate(trimestres):
    dfO_trimestre = filtrar_por_trimestreCPI_Opex(dfO, meses)
    dfC_trimestre = filtrar_por_trimestreCapex(dfC, meses)
    dfCo_trimestre = filtrar_por_trimestreCPI_Opex(dfCo, meses)

    # Crear directorio para Apoyo a la Operación
    ruta_apoyo_operacion = os.path.join(ruta_año, nombres_trimestres[i], '4. Apoyo a la Operación')
    os.makedirs(ruta_apoyo_operacion, exist_ok=True)
    # Crear un archivo Excel vacío en la carpeta reciéntemente creada
    nombre_archivo_excel = f"Apoyo Operación Q{i+1} {año_interes}.xlsx"
    ruta_archivo_excel = os.path.join(ruta_apoyo_operacion, nombre_archivo_excel)
    # Crear un DataFrame vacío para generar el archivo Excel
    df_vacio = pd.DataFrame()
    df_vacio.to_excel(ruta_archivo_excel, index=False)

    ruta_trimestre_CPI = os.path.join(ruta_año, nombres_trimestres[i], '1. Perforación & Completación de Pozos Nuevos')
    ruta_trimestre_Opex = os.path.join(ruta_año, nombres_trimestres[i], '3. Intervenciones con Torre (Pulling)')
    ruta_trimestre_Capex = os.path.join(ruta_año, nombres_trimestres[i], '2. Reacondicionamiento de Pozos (Workover CAPEX)')

    print("Creando directorios... pozos CPI:")
    crear_directorios(dfCo_trimestre, ruta_trimestre_CPI, 'CPI')
    print("------------------------------------------------------------")
    print("Creando directorios... pozos OPEX:")
    crear_directorios(dfO_trimestre, ruta_trimestre_Opex, 'OPEX')
    print("------------------------------------------------------------")
    print("Creando directorios... pozos CAPEX:")
    crear_directorios(dfC_trimestre, ruta_trimestre_Capex, 'CAPEX')

"""
def escribir_informe_errores(errores_de_copia, ruta_archivo):
    # se escribe en el archivo .txt los errores de copia
    with open(ruta_archivo, 'w') as f:
        for error in errores_de_copia:
            f.write(f"{error}\n")
    
    print(f"Informe de errores guardado en {ruta_archivo}")
"""

def extraer_nombre_pozo(error):
    # Suponiendo que los nombres de los pozos siguen un patrón identificable y consistente en los mensajes de error
    # incluir todo el nombre del pozo en el mensaje de error
    match = re.search(r"para el pozo ([\w-]+)", error)
    if match:
        return match.group(1)  # Retorna el nombre del pozo encontrado
    return "Desconocido"  # Retorna un valor por defecto si no se encuentra el nombre del pozo

def escribir_informe_errores(errores_de_copia, ruta_archivo):
    # Agrupar errores por pozo
    errores_agrupados = {}
    for error in errores_de_copia:
        pozo = extraer_nombre_pozo(error)
        if pozo not in errores_agrupados:
            errores_agrupados[pozo] = []
        errores_agrupados[pozo].append(error)
    
    # Escribir errores agrupados en el archivo
    with open(ruta_archivo, 'w') as f:
        for pozo, lista_errores in errores_agrupados.items():
            f.write(f"Errores para el pozo {pozo}:\n")
            for error in lista_errores:
                f.write(f"  - {error}\n")
            f.write("\n")

directorio_informe = rf'\\dir.slb.com\NSA\SAM_Collaborate\EC0037\90_Public\Tests\V_1\01 INFORMES TRIMESTRALES'
ruta_archivo_errores = os.path.join(directorio_informe, nombre_archivo_errores)
escribir_informe_errores(errores_de_copia, ruta_archivo_errores)