import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import os

# --- Configuración de la página ---
st.set_page_config(
    page_title="Dashboard de Productividad y Legalizaciones",
    page_icon="📊",
    layout="wide"
)

# --- 1. Configuración de Persistencia de Datos ---
PERSISTED_DATA_DIR = "../persisted_data"
# Asegúrate de que la carpeta para datos persistentes exista
os.makedirs(PERSISTED_DATA_DIR, exist_ok=True)

# Nombres de archivo para los DataFrames persistentes
PPL_FILE = os.path.join(PERSISTED_DATA_DIR, "df_ppl.parquet")
CONVENIOS_FILE = os.path.join(PERSISTED_DATA_DIR, "df_convenios.parquet")
RIPS_FILE = os.path.join(PERSISTED_DATA_DIR, "df_rips.parquet")
FACTURACION_FILE = os.path.join(PERSISTED_DATA_DIR, "df_facturacion.parquet")

# --- 2. Inicializar st.session_state ---
# Estas claves ahora también indican si los datos se cargaron desde archivos o se subieron.
if 'ppl_uploaded' not in st.session_state:
    st.session_state.ppl_uploaded = False
if 'convenios_uploaded' not in st.session_state:
    st.session_state.convenios_uploaded = False
if 'rips_uploaded' not in st.session_state:
    st.session_state.rips_uploaded = False
if 'facturacion_uploaded' not in st.session_state:
    st.session_state.facturacion_uploaded = False

# Inicializar DataFrames en session_state a None
if 'df_ppl' not in st.session_state:
    st.session_state.df_ppl = None
if 'df_convenios' not in st.session_state:
    st.session_state.df_convenios = None
if 'df_rips' not in st.session_state:
    st.session_state.df_rips = None
if 'df_facturacion' not in st.session_state:
    st.session_state.df_facturacion = None


# --- 3. Funciones para guardar y cargar DataFrames con Parquet ---
def save_dataframe(df, filepath):
    """Guarda un DataFrame en un archivo Parquet."""
    filename = os.path.basename(filepath)
    if df is not None and not df.empty:
        try:
            df.to_parquet(filepath, index=False)
            st.info(f"💾 Guardado exitoso: {filename}")
            return True
        except Exception as e:
            st.error(f"❌ Error al guardar el archivo {filename}: {e}")
            return False
    else:
        st.info(f"ℹ️ No hay datos para guardar en {filename}. (DataFrame vacío o None)")
        return True # Retorna True porque no es un error, simplemente no hay nada que guardar.

def load_dataframe(filepath):
    """Carga un DataFrame desde un archivo Parquet."""
    if os.path.exists(filepath):
        try:
            return pd.read_parquet(filepath)
        except Exception as e:
            st.warning(f"No se pudo cargar el archivo {os.path.basename(filepath)} previamente guardado. "
                       f"Por favor, súbelo de nuevo o verifica el archivo. Error: {e}")
            # Considerar eliminar el archivo corrupto si el error es grave.
            # os.remove(filepath) # Opcional: Eliminar archivo corrupto
            return None
    return None

# --- 4. Función para cargar archivos subidos (con caché para eficiencia) ---
@st.cache_data
def load_uploaded_data(uploaded_file):
    """
    Carga un archivo CSV o Excel en un DataFrame de Pandas.
    Maneja errores y retorna None si la carga falla.
    """
    if uploaded_file is not None:
        try:
            uploaded_file.seek(0) # Siempre resetear el puntero antes de intentar leer

            # Intentar leer como CSV primero
            try:
                df_loaded = pd.read_csv(uploaded_file, encoding='utf-8', errors='ignore', on_bad_lines='skip')
                return df_loaded
            except Exception as csv_error:
                # Si falla CSV, intentar como Excel
                uploaded_file.seek(0) # Resetear el puntero del archivo para Excel
                # Intentar leer con openpyxl, que es más robusto para .xlsx
                try:
                    df_loaded = pd.read_excel(uploaded_file, engine='openpyxl')
                    return df_loaded
                except Exception as excel_error:
                    st.error(f"Error al cargar el archivo como Excel. Detalles: {excel_error}")
                    return None
        except Exception as e:
            st.error(f"Error general al cargar el archivo. Asegúrate de que sea un archivo CSV o Excel válido. Detalles: {e}")
            return None
    return None

# --- 5. Cargar datos persistentes al inicio si existen ---
# Se verifica si el DataFrame no se ha cargado aún por subida de archivo
# y si existe un archivo persistente.
if not st.session_state.ppl_uploaded and os.path.exists(PPL_FILE):
    st.session_state.df_ppl = load_dataframe(PPL_FILE)
    if st.session_state.df_ppl is not None:
        # Asegurar tipo de dato para la columna NUMERO_IDENTIFICACION si se carga de disco
        if 'NUMERO_IDENTIFICACION' in st.session_state.df_ppl.columns:
            st.session_state.df_ppl['NUMERO_IDENTIFICACION'] = st.session_state.df_ppl['NUMERO_IDENTIFICACION'].astype(str)
        # Asegurar tipo de dato para la columna PROCEDIMIENTO si se carga de disco
        if 'PROCEDIMIENTO' in st.session_state.df_ppl.columns:
            st.session_state.df_ppl['PROCEDIMIENTO'] = st.session_state.df_ppl['PROCEDIMIENTO'].astype(str)
        # Asegurar tipo de dato para la columna CodigoEspecialidad si se carga de disco
        if 'CodigoEspecialidad' in st.session_state.df_ppl.columns:
            st.session_state.df_ppl['CodigoEspecialidad'] = st.session_state.df_ppl['CodigoEspecialidad'].astype(str)
        # Asegurar tipo de dato para la columna Usuario si se carga de disco
        if 'Usuario' in st.session_state.df_ppl.columns:
            st.session_state.df_ppl['Usuario'] = st.session_state.df_ppl['Usuario'].astype(str)

        st.session_state.ppl_uploaded = True
        st.session_state.df_ppl['Tipo_Legalizacion'] = 'PPL' # Asegurar la columna si se carga de disco

if not st.session_state.convenios_uploaded and os.path.exists(CONVENIOS_FILE):
    st.session_state.df_convenios = load_dataframe(CONVENIOS_FILE)
    if st.session_state.df_convenios is not None:
        # Asegurar tipo de dato para la columna NUMERO_IDENTIFICACION si se carga de disco
        if 'NUMERO_IDENTIFICACION' in st.session_state.df_convenios.columns:
            st.session_state.df_convenios['NUMERO_IDENTIFICACION'] = st.session_state.df_convenios['NUMERO_IDENTIFICACION'].astype(str)
        # Asegurar tipo de dato para la columna PROCEDIMIENTO si se carga de disco
        if 'PROCEDIMIENTO' in st.session_state.df_convenios.columns:
            st.session_state.df_convenios['PROCEDIMIENTO'] = st.session_state.df_convenios['PROCEDIMIENTO'].astype(str)
        # Asegurar tipo de dato para la columna CodigoEspecialidad si se carga de disco
        if 'CodigoEspecialidad' in st.session_state.df_convenios.columns:
            st.session_state.df_convenios['CodigoEspecialidad'] = st.session_state.df_convenios['CodigoEspecialidad'].astype(str)
        # Asegurar tipo de dato para la columna Usuario si se carga de disco
        if 'Usuario' in st.session_state.df_convenios.columns:
            st.session_state.df_convenios['Usuario'] = st.session_state.df_convenios['Usuario'].astype(str)

        st.session_state.convenios_uploaded = True
        st.session_state.df_convenios['Tipo_Legalizacion'] = 'Convenios' # Asegurar la columna

if not st.session_state.rips_uploaded and os.path.exists(RIPS_FILE):
    st.session_state.df_rips = load_dataframe(RIPS_FILE)
    if st.session_state.df_rips is not None:
        # Asegurar tipo de dato para la columna NOMBRE si se carga de disco
        if 'NOMBRE' in st.session_state.df_rips.columns:
            st.session_state.df_rips['NOMBRE'] = st.session_state.df_rips['NOMBRE'].astype(str)
        # Asegurar tipo de dato para la columna ESTADO si se carga de disco
        if 'ESTADO' in st.session_state.df_rips.columns:
            st.session_state.df_rips['ESTADO'] = st.session_state.df_rips['ESTADO'].astype(str)

        st.session_state.rips_uploaded = True

if not st.session_state.facturacion_uploaded and os.path.exists(FACTURACION_FILE):
    st.session_state.df_facturacion = load_dataframe(FACTURACION_FILE)
    if st.session_state.df_facturacion is not None:
        st.session_state.facturacion_uploaded = True
        # El preprocesamiento de mayúsculas y Tipo_Facturacion ya debe haberse hecho al guardar,
        # pero es buena práctica asegurarse si el proceso de guardado no garantiza esto.
        if 'PREFIJO' in st.session_state.df_facturacion.columns:
            st.session_state.df_facturacion['PREFIJO'] = st.session_state.df_facturacion['PREFIJO'].astype(str) # Asegurar tipo string para .upper()
            st.session_state.df_facturacion['Tipo_Facturacion'] = st.session_state.df_facturacion['PREFIJO'].apply(
                lambda x: 'PPL' if x.strip().upper() == 'SM' else
                          ('Convenios' if x.strip().upper() == 'E' else 'Otro')
            )
        else:
            st.warning("Columna 'PREFIJO' no encontrada en el archivo de Facturación persistente. No se podrá filtrar por tipo (PPL/Convenios).")
            st.session_state.df_facturacion['Tipo_Facturacion'] = 'Desconocido' # Valor por defecto
        # Asegurar tipo de dato para la columna IDENTIFICACION si se carga de disco
        if 'IDENTIFICACION' in st.session_state.df_facturacion.columns:
            st.session_state.df_facturacion['IDENTIFICACION'] = st.session_state.df_facturacion['IDENTIFICACION'].astype(str)
        # Asegurar tipo de dato para la columna USUARIO si se carga de disco
        if 'USUARIO' in st.session_state.df_facturacion.columns:
            st.session_state.df_facturacion['USUARIO'] = st.session_state.df_facturacion['USUARIO'].astype(str)


# --- 6. Título y encabezados del Dashboard ---
st.title("📊 Dashboard de Productividad de Legalizaciones, Rips y Facturación")
st.markdown("Sube tus archivos para analizar la productividad de los facturadores por periodos de tiempo.")

# --- 7. Carga de Múltiples Archivos desde la barra lateral ---
st.sidebar.header("Cargar Archivos")

# Lista para almacenar mensajes de estado de carga
upload_status_messages = []

# Referencias a los DataFrames en session_state para facilitar el acceso
df_ppl = st.session_state.df_ppl
df_convenios = st.session_state.df_convenios
df_rips = st.session_state.df_rips
df_facturacion = st.session_state.df_facturacion

# Lógica de uploaders condicionales
# Legalizaciones PPL
if not st.session_state.ppl_uploaded:
    uploaded_file_ppl_widget = st.sidebar.file_uploader("Sube archivo de Legalizaciones PPL (CSV/Excel)", type=["csv", "xlsx"], key="ppl_uploader")
    if uploaded_file_ppl_widget is not None:
        df_ppl_new = load_uploaded_data(uploaded_file_ppl_widget)
        if df_ppl_new is not None:
            # --- CONVERSIÓN DE TIPO DE DATO: columnas críticas a str para PPL ---
            for col in ['NUMERO_IDENTIFICACION', 'PROCEDIMIENTO', 'CodigoEspecialidad', 'Usuario']:
                if col in df_ppl_new.columns:
                    df_ppl_new[col] = df_ppl_new[col].astype(str)

            df_ppl_new['Tipo_Legalizacion'] = 'PPL'
            st.session_state.ppl_uploaded = True
            st.session_state.df_ppl = df_ppl_new
            upload_status_messages.append(("success", "Archivo PPL cargado correctamente."))
            # Forzar rerun para que se actualice el estado y se oculte el uploader
            st.rerun()
        else:
            upload_status_messages.append(("error", "Fallo al cargar archivo PPL."))
else:
    upload_status_messages.append(("info", "Archivo PPL ya cargado (desde subida o persistencia)."))

# Legalizaciones Convenios
if not st.session_state.convenios_uploaded:
    uploaded_file_convenios_widget = st.sidebar.file_uploader("Sube archivo de Legalizaciones Convenios (CSV/Excel)", type=["csv", "xlsx"], key="convenios_uploader")
    if uploaded_file_convenios_widget is not None:
        df_convenios_new = load_uploaded_data(uploaded_file_convenios_widget)
        if df_convenios_new is not None:
            # --- CONVERSIÓN DE TIPO DE DATO: columnas críticas a str para Convenios ---
            for col in ['NUMERO_IDENTIFICACION', 'PROCEDIMIENTO', 'CodigoEspecialidad', 'Usuario']:
                if col in df_convenios_new.columns:
                    df_convenios_new[col] = df_convenios_new[col].astype(str)

            df_convenios_new['Tipo_Legalizacion'] = 'Convenios'
            st.session_state.convenios_uploaded = True
            st.session_state.df_convenios = df_convenios_new
            upload_status_messages.append(("success", "Archivo Convenios cargado correctamente."))
            st.rerun()
        else:
            upload_status_messages.append(("error", "Fallo al cargar archivo Convenios."))
else:
    upload_status_messages.append(("info", "Archivo Convenios ya cargado (desde subida o persistencia)."))

# RIPS
if not st.session_state.rips_uploaded:
    uploaded_file_rips_widget = st.sidebar.file_uploader("Sube archivo de RIPS (CSV/Excel)", type=["csv", "xlsx"], key="rips_uploader")
    if uploaded_file_rips_widget is not None:
        df_rips_new = load_uploaded_data(uploaded_file_rips_widget)
        if df_rips_new is not None:
            # --- CONVERSIÓN DE TIPO DE DATO: columnas críticas a str para RIPS ---
            for col in ['NOMBRE', 'ESTADO']:
                if col in df_rips_new.columns:
                    df_rips_new[col] = df_rips_new[col].astype(str)

            st.session_state.rips_uploaded = True
            st.session_state.df_rips = df_rips_new
            upload_status_messages.append(("success", "Archivo RIPS cargado correctamente."))
            st.rerun()
        else:
            upload_status_messages.append(("error", "Fallo al cargar archivo RIPS."))
else:
    upload_status_messages.append(("info", "Archivo RIPS ya cargado (desde subida o persistencia)."))

# Facturación
if not st.session_state.facturacion_uploaded:
    uploaded_file_facturacion_widget = st.sidebar.file_uploader("Sube archivo de Facturación (CSV/Excel)", type=["csv", "xlsx"], key="facturacion_uploader")
    if uploaded_file_facturacion_widget is not None:
        df_facturacion_new = load_uploaded_data(uploaded_file_facturacion_widget)
        if df_facturacion_new is not None:
            st.session_state.facturacion_uploaded = True
            st.session_state.df_facturacion = df_facturacion_new

            # Convertir todas las columnas del DataFrame a mayúsculas para la comparación
            st.session_state.df_facturacion.columns = st.session_state.df_facturacion.columns.str.upper()

            # --- CONVERSIÓN DE TIPO DE DATO: columnas críticas a str para Facturación ---
            for col in ['IDENTIFICACION', 'PREFIJO', 'USUARIO']:
                if col in st.session_state.df_facturacion.columns:
                    st.session_state.df_facturacion[col] = st.session_state.df_facturacion[col].astype(str)

            # --- Preprocesamiento para Tipo_Facturacion ---
            if 'PREFIJO' in st.session_state.df_facturacion.columns:
                st.session_state.df_facturacion['Tipo_Facturacion'] = st.session_state.df_facturacion['PREFIJO'].apply(
                    lambda x: 'PPL' if x.strip().upper() == 'SM' else
                              ('Convenios' if x.strip().upper() == 'E' else 'Otro')
                )
            else:
                st.warning("Columna 'PREFIJO' no encontrada en el archivo de Facturación. No se podrá filtrar por tipo (PPL/Convenios).")
                st.session_state.df_facturacion['Tipo_Facturacion'] = 'Desconocido' # Valor por defecto

            upload_status_messages.append(("success", "Archivo de Facturación cargado correctamente."))
            st.rerun()
        else:
            upload_status_messages.append(("error", "Fallo al cargar archivo de Facturación."))
else:
    upload_status_messages.append(("info", "Archivo de Facturación ya cargado (desde subida o persistencia)."))


# --- 8. Botones de Acción: Guardar y Limpiar ---
st.sidebar.markdown("---")
st.sidebar.subheader("Acciones de Datos")

# Botón para guardar datos procesados a disco
if st.sidebar.button("Guardar datos para futura carga", key="save_data_button"):
    save_success_ppl = save_dataframe(st.session_state.df_ppl, PPL_FILE)
    save_success_convenios = save_dataframe(st.session_state.df_convenios, CONVENIOS_FILE)
    save_success_rips = save_dataframe(st.session_state.df_rips, RIPS_FILE)
    save_success_facturacion = save_dataframe(st.session_state.df_facturacion, FACTURACION_FILE)

    if save_success_ppl and save_success_convenios and save_success_rips and save_success_facturacion:
        st.sidebar.success("Todos los datos procesados se han guardado correctamente.")
    else:
        st.sidebar.error("Hubo un error al guardar algunos datos. Revisa los mensajes en la aplicación para más detalles.")


# Botón para limpiar archivos cargados
def clear_uploaded_files():
    # Resetear el estado de carga y DataFrames en session_state
    st.session_state.ppl_uploaded = False
    st.session_state.convenios_uploaded = False
    st.session_state.rips_uploaded = False
    st.session_state.facturacion_uploaded = False

    st.session_state.df_ppl = None
    st.session_state.df_convenios = None
    st.session_state.df_rips = None
    st.session_state.df_facturacion = None

    # Eliminar los archivos persistentes también
    for filepath in [PPL_FILE, CONVENIOS_FILE, RIPS_FILE, FACTURACION_FILE]:
        if os.path.exists(filepath):
            os.remove(filepath)
            st.sidebar.info(f"Archivo persistente {os.path.basename(filepath)} eliminado.")

    st.cache_data.clear() # Limpiar la caché de la función load_uploaded_data
    st.rerun() # Forzar un rerun para que los uploaders reaparezcan

st.sidebar.button("Limpiar archivos cargados y persistentes", on_click=clear_uploaded_files, key="clear_files_button")

# --- 9. Combinar los DataFrames de Legalizaciones ---
df_legalizaciones = None
if st.session_state.df_ppl is not None and st.session_state.df_convenios is not None:
    df_legalizaciones = pd.concat([st.session_state.df_ppl, st.session_state.df_convenios], ignore_index=True)
elif st.session_state.df_ppl is not None:
    df_legalizaciones = st.session_state.df_ppl
elif st.session_state.df_convenios is not None:
    df_legalizaciones = st.session_state.df_convenios
else:
    upload_status_messages.append(("info", "Esperando que cargues al menos un archivo de legalizaciones."))

# --- 10. Validaciones Iniciales de Datos ---

# Manejo de ausencia de datos general
if df_legalizaciones is None and st.session_state.df_rips is None and st.session_state.df_facturacion is None:
    st.info("Para comenzar el análisis, por favor **sube al menos un archivo** (Legalizaciones, RIPS o Facturación) usando los botones en la **barra lateral izquierda**, o **carga los datos guardados** si ya existen.")
    st.stop()


# Validación de columnas clave para Legalizaciones
if df_legalizaciones is not None:
    required_cols_legalizaciones = ['Usuario', 'FECHA_REAL']
    if not all(col in df_legalizaciones.columns for col in required_cols_legalizaciones):
        st.error(f"¡Atención! Para el análisis de productividad de legalizaciones, tus archivos deben contener las columnas: **{', '.join(required_cols_legalizaciones)}**.")
        st.error("Por favor, corrige los nombres de las columnas en tus archivos de legalizaciones y vuelve a cargarlos.")
        df_legalizaciones = None # Invalida el DF si faltan columnas
    else:
        # Asegurarse de que 'Usuario' sea string antes de cualquier operación
        df_legalizaciones['Usuario'] = df_legalizaciones['Usuario'].astype(str)
        df_legalizaciones['FECHA_REAL'] = pd.to_datetime(df_legalizaciones['FECHA_REAL'], errors='coerce')
        df_legalizaciones.dropna(subset=['FECHA_REAL'], inplace=True)
        df_legalizaciones.sort_values(by='FECHA_REAL', inplace=True)

# Validación de columnas clave para RIPS
if st.session_state.df_rips is not None:
    required_cols_rips = ['NOMBRE', 'ESTADO', 'ULTIMA_MODIFICACION']
    if not all(col in st.session_state.df_rips.columns for col in required_cols_rips):
        st.error(f"¡Atención! Para el análisis de RIPS, tu archivo debe contener las columnas: **{', '.join(required_cols_rips)}**.")
        st.error("Por favor, corrige los nombres de las columnas en tu archivo RIPS y vuelve a cargarlo.")
        st.session_state.df_rips = None # Invalida el DataFrame de RIPS si faltan columnas
    else:
        # Asegurarse de que 'NOMBRE' y 'ESTADO' sean string antes de cualquier operación
        st.session_state.df_rips['NOMBRE'] = st.session_state.df_rips['NOMBRE'].astype(str)
        st.session_state.df_rips['ESTADO'] = st.session_state.df_rips['ESTADO'].astype(str)
        st.session_state.df_rips['ULTIMA_MODIFICACION'] = pd.to_datetime(st.session_state.df_rips['ULTIMA_MODIFICACION'], errors='coerce')
        st.session_state.df_rips.dropna(subset=['ULTIMA_MODIFICACION'], inplace=True)
        st.session_state.df_rips.sort_values(by='ULTIMA_MODIFICACION', inplace=True)

# Validación de columnas clave para Facturación
if st.session_state.df_facturacion is not None:
    required_cols_facturacion = ['USUARIO', 'FECHA FACTURA', 'PREFIJO']
    # Aseguramos que las columnas estén en mayúsculas (ya se hace al cargar pero es una buena práctica aquí también)
    st.session_state.df_facturacion.columns = st.session_state.df_facturacion.columns.str.upper()

    if not all(col in st.session_state.df_facturacion.columns for col in required_cols_facturacion):
        st.error(f"¡Atención! Para el análisis de Facturación, tu archivo debe contener las columnas: **{', '.join(required_cols_facturacion)}**.")
        st.error("Por favor, corrige los nombres de las columnas en tu archivo de Facturación y vuelve a cargarlo.")
        st.session_state.df_facturacion = None # Invalida el DataFrame si faltan columnas
    else:
        # Asegurarse de que 'USUARIO' y 'PREFIJO' sean string antes de cualquier operación
        st.session_state.df_facturacion['USUARIO'] = st.session_state.df_facturacion['USUARIO'].astype(str)
        st.session_state.df_facturacion['PREFIJO'] = st.session_state.df_facturacion['PREFIJO'].astype(str)
        st.session_state.df_facturacion['FECHA FACTURA'] = pd.to_datetime(st.session_state.df_facturacion['FECHA FACTURA'], errors='coerce')
        st.session_state.df_facturacion.dropna(subset=['FECHA FACTURA'], inplace=True)
        st.session_state.df_facturacion.sort_values(by='FECHA FACTURA', inplace=True)


# --- 11. Filtro de Análisis (GLOBAL) ---
st.sidebar.subheader("Filtros de Análisis")

# Calcular min y max fechas disponibles de todos los datasets cargados
all_min_dates = []
all_max_dates = []

if df_legalizaciones is not None and not df_legalizaciones.empty:
    all_min_dates.append(df_legalizaciones['FECHA_REAL'].min().date())
    all_max_dates.append(df_legalizaciones['FECHA_REAL'].max().date())
if st.session_state.df_rips is not None and not st.session_state.df_rips.empty:
    all_min_dates.append(st.session_state.df_rips['ULTIMA_MODIFICACION'].min().date())
    all_max_dates.append(st.session_state.df_rips['ULTIMA_MODIFICACION'].max().date())
if st.session_state.df_facturacion is not None and not st.session_state.df_facturacion.empty:
    all_min_dates.append(st.session_state.df_facturacion['FECHA FACTURA'].min().date())
    all_max_dates.append(st.session_state.df_facturacion['FECHA FACTURA'].max().date())


# Asignar valores por defecto si no hay archivos cargados para evitar errores
if not all_min_dates:
    min_date_global = datetime.date.today() - datetime.timedelta(days=365) # Un año atrás
    max_date_global = datetime.date.today()
    st.sidebar.info("No hay archivos cargados para determinar un rango de fechas. Usando un rango predeterminado del último año.")
else:
    min_date_global = min(all_min_dates)
    max_date_global = max(all_max_dates)

date_range_selection = st.sidebar.date_input(
    "Selecciona Rango de Fechas",
    value=(min_date_global, max_date_global),
    min_value=min_date_global,
    max_value=max_date_global,
    key="date_range_filter_global"
)

if len(date_range_selection) == 2:
    start_date = min(date_range_selection)
    end_date = max(date_range_selection)
elif len(date_range_selection) == 1:
    start_date = date_range_selection[0]
    end_date = date_range_selection[0]
else:
    start_date = min_date_global
    end_date = max_date_global

if start_date > end_date:
    st.sidebar.error("Error: La fecha de inicio no puede ser posterior a la fecha de fin. Por favor, corrige tu selección.")
    st.stop()

# --- Filtro de Facturador (Usuario/Nombre) ---
facturador_options_union = []
if df_legalizaciones is not None and not df_legalizaciones.empty:
    facturador_options_union.extend(df_legalizaciones['Usuario'].dropna().astype(str).unique())
if st.session_state.df_rips is not None and not st.session_state.df_rips.empty:
    facturador_options_union.extend(st.session_state.df_rips['NOMBRE'].dropna().astype(str).unique())
if st.session_state.df_facturacion is not None and not st.session_state.df_facturacion.empty:
    facturador_options_union.extend(st.session_state.df_facturacion['USUARIO'].dropna().astype(str).unique())

if facturador_options_union:
    facturador_options_union = sorted(list(set(facturador_options_union)))
    facturador_options = ['Todos'] + facturador_options_union
    facturador_seleccionado = st.sidebar.multiselect(
        'Filtrar por Facturador (Usuario/Nombre)',
        options=facturador_options,
        default=['Todos'] if 'Todos' in facturador_options else [], # Por defecto 'Todos' para multiselect
        key="filter_facturador"
    )

    # Lógica para manejar 'Todos' en multiselect
    if 'Todos' in facturador_seleccionado and len(facturador_seleccionado) > 1:
        # Si 'Todos' está seleccionado junto con otros, solo considera 'Todos'
        facturador_seleccionado = ['Todos']
        st.sidebar.info("Cuando 'Todos' está seleccionado, se ignoran las otras selecciones de facturador.")
    elif not facturador_seleccionado:
        # Si no se selecciona nada, por defecto se actúa como si fuera 'Todos'
        facturador_seleccionado = ['Todos']
        st.sidebar.info("No se ha seleccionado ningún facturador. Mostrando datos para todos los facturadores.")
else:
    st.sidebar.info("Carga archivos para acceder a los filtros de facturador.")
    facturador_seleccionado = ['Todos']


# --- FILTROS ADICIONALES DE LEGALIZACIONES (DENTRO DE UN EXPANDER) ---
st.sidebar.header("Filtros Adicionales de Legalizaciones")
with st.sidebar.expander("Expandir Filtros Adicionales de Legalizaciones"):
    if df_legalizaciones is not None and not df_legalizaciones.empty:
        # Filtro por Tipo de Legalización (PPL / Convenios)
        if 'Tipo_Legalizacion' in df_legalizaciones.columns:
            tipo_legalizacion_options = ['Todos'] + list(df_legalizaciones['Tipo_Legalizacion'].unique())
            tipo_legalizacion_seleccionado = st.multiselect(
                'Filtrar por Tipo de Legalización',
                options=tipo_legalizacion_options,
                default=['Todos'] if 'Todos' in tipo_legalizacion_options else [],
                key="filter_tipo_legalizacion"
            )
            # Lógica para manejar 'Todos' en multiselect
            if 'Todos' in tipo_legalizacion_seleccionado and len(tipo_legalizacion_seleccionado) > 1:
                tipo_legalizacion_seleccionado = ['Todos']
                st.info("Cuando 'Todos' está seleccionado, se ignoran las otras selecciones de tipo de legalización.")
            elif not tipo_legalizacion_seleccionado:
                tipo_legalizacion_seleccionado = ['Todos']
                st.info("No se ha seleccionado ningún tipo de legalización. Mostrando todos los tipos.")
        else:
            tipo_legalizacion_seleccionado = ['Todos']
            st.info("Columna 'Tipo_Legalizacion' no encontrada, no se puede filtrar por tipo.")

        # Selector de Periodo de Tiempo (Aplicable a todas las secciones if está visible)
        periodo_tiempo_options = {
            "Día": "D",
            "5 Días": "5D",
            "Semana": "W",
            "Mes": "M",
            "Trimestre": "Q",
            "Año": "Y"
        }
        periodo_seleccionado_label = st.selectbox(
            'Agrupar productividad por:',
            options=list(periodo_tiempo_options.keys()),
            key="filter_periodo"
        )
        periodo_seleccionado_code = periodo_tiempo_options[periodo_seleccionado_label]
    else:
        st.info("Carga un archivo de legalizaciones para acceder a estos filtros.")
        tipo_legalizacion_seleccionado = ['Todos']
        periodo_seleccionado_label = "Día" # Default value even if no data
        periodo_seleccionado_code = "D" # Default value even if no data


# --- FILTROS ESPECÍFICOS DE RIPS (DENTRO DE UN EXPANDER) ---
st.sidebar.header("Filtros de RIPS")
with st.sidebar.expander("Expandir Filtros de RIPS"):
    if st.session_state.df_rips is not None and not st.session_state.df_rips.empty:
        unique_rips_statuses = st.session_state.df_rips['ESTADO'].dropna().astype(str).unique().tolist()
        rips_estado_options = ['Todos'] + sorted(unique_rips_statuses)
        rips_estado_seleccionado = st.multiselect(
            'Filtrar RIPS por Estado:',
            options=rips_estado_options,
            default=['Todos'] if 'Todos' in rips_estado_options else [],
            key="filter_rips_estado"
        )
        # Lógica para manejar 'Todos' en multiselect
        if 'Todos' in rips_estado_seleccionado and len(rips_estado_seleccionado) > 1:
            rips_estado_seleccionado = ['Todos']
            st.info("Cuando 'Todos' está seleccionado, se ignoran las otras selecciones de estado de RIPS.")
        elif not rips_estado_seleccionado:
            rips_estado_seleccionado = ['Todos']
            st.info("No se ha seleccionado ningún estado de RIPS. Mostrando todos los estados.")
    else:
        st.info("Carga el archivo de RIPS para acceder a este filtro.")
        rips_estado_seleccionado = ['Todos']


# --- FILTROS ESPECÍFICOS DE FACTURACIÓN (NUEVO EXPANDER) ---
st.sidebar.header("Filtros Adicionales de Facturación")
with st.sidebar.expander("Expandir Filtros Adicionales de Facturación"):
    if st.session_state.df_facturacion is not None and not st.session_state.df_facturacion.empty:
        if 'Tipo_Facturacion' in st.session_state.df_facturacion.columns and len(st.session_state.df_facturacion['Tipo_Facturacion'].unique()) > 1:
            tipo_facturacion_options = ['Todos'] + list(st.session_state.df_facturacion['Tipo_Facturacion'].unique())
            tipo_facturacion_seleccionado = st.multiselect(
                'Filtrar por Tipo de Facturación',
                options=tipo_facturacion_options,
                default=['Todos'] if 'Todos' in tipo_facturacion_options else [],
                key="filter_tipo_facturacion"
            )
            # Lógica para manejar 'Todos' en multiselect
            if 'Todos' in tipo_facturacion_seleccionado and len(tipo_facturacion_seleccionado) > 1:
                tipo_facturacion_seleccionado = ['Todos']
                st.info("Cuando 'Todos' está seleccionado, se ignoran las otras selecciones de tipo de facturación.")
            elif not tipo_facturacion_seleccionado:
                tipo_facturacion_seleccionado = ['Todos']
                st.info("No se ha seleccionado ningún tipo de facturación. Mostrando todos los tipos.")
        else:
            tipo_facturacion_seleccionado = ['Todos']
            st.info("Columna 'Tipo_Facturacion' no disponible o solo un tipo. Asegúrate que 'PREFIJO' exista y sea válido.")
    else:
        st.info("Carga un archivo de facturación para acceder a estos filtros.")
        tipo_facturacion_seleccionado = ['Todos']

# --- Mostrar mensajes de estado de carga debajo de los filtros ---
st.sidebar.markdown("---")
st.sidebar.subheader("Estado de Carga de Archivos")
for msg_type, msg_text in upload_status_messages:
    if msg_type == "success":
        st.sidebar.success(msg_text)
    elif msg_type == "error":
        st.sidebar.error(msg_text)
    elif msg_type == "info":
        st.sidebar.info(msg_text)


# --- LÓGICA DE FILTRADO Y ANÁLISIS DE LEGALIZACIONES ---
if df_legalizaciones is not None and not df_legalizaciones.empty:
    st.markdown("---")
    st.header("Análisis de Legalizaciones")

    df_base_filtered = df_legalizaciones.copy()

    df_base_filtered = df_base_filtered[
        (df_base_filtered['FECHA_REAL'].dt.date >= start_date) &
        (df_base_filtered['FECHA_REAL'].dt.date <= end_date)
    ]

    if 'Todos' not in tipo_legalizacion_seleccionado and 'Tipo_Legalizacion' in df_base_filtered.columns:
        df_base_filtered = df_base_filtered[df_base_filtered['Tipo_Legalizacion'].isin(tipo_legalizacion_seleccionado)]

    # Filtra por el facturador seleccionado para los gráficos y tablas
    df_filtered_by_facturador_for_display = df_base_filtered.copy()
    if 'Todos' not in facturador_seleccionado:
        df_filtered_by_facturador_for_display = df_filtered_by_facturador_for_display[
            df_filtered_by_facturador_for_display['Usuario'].isin(facturador_seleccionado)
        ]

    if df_base_filtered.empty:
        st.warning("No hay datos de legalizaciones para la selección actual de tipo y rango de fechas. Por favor, ajusta tus filtros.")

    if df_filtered_by_facturador_for_display.empty and 'Todos' not in facturador_seleccionado:
        st.info("No hay datos de legalizaciones para la selección actual de filtros (Tipo, Rango de Fechas, Facturador).")


    # --- NUEVA TABLA: Resumen Acumulado de Legalizaciones por Facturador ---
    st.subheader(f"Resumen Acumulado Total de Legalizaciones por Facturador ({start_date} a {end_date})")

    summary_legalizaciones_facturador = df_filtered_by_facturador_for_display.groupby('Usuario').size().reset_index(name='Total_Legalizaciones_Acumuladas')
    summary_legalizaciones_facturador = summary_legalizaciones_facturador.sort_values(
        'Total_Legalizaciones_Acumuladas', ascending=False
    ).reset_index(drop=True)

    total_general_legalizaciones = summary_legalizaciones_facturador['Total_Legalizaciones_Acumuladas'].sum()
    if total_general_legalizaciones > 0:
        summary_legalizaciones_facturador['Porcentaje_del_Total'] = \
            (summary_legalizaciones_facturador['Total_Legalizaciones_Acumuladas'] / total_general_legalizaciones * 100).round(2)
        summary_legalizaciones_facturador['Porcentaje_del_Total'] = summary_legalizaciones_facturador['Porcentaje_del_Total'].astype(str) + '%'
    else:
        summary_legalizaciones_facturador['Porcentaje_del_Total'] = '0%'

    st.dataframe(summary_legalizaciones_facturador)

    if summary_legalizaciones_facturador.empty:
        st.info("No hay datos de resumen de legalizaciones para el rango de fechas y tipo seleccionados.")


    # --- Gráfico de Productividad de LEGALIZACIONES ---
    st.subheader("Visualización de Productividad de Legalizaciones")

    metric_to_display_for_evolution = 'Total_Legalizaciones'

    # Lógica condicional para mostrar el gráfico de comparación o individual
    # Muestra el gráfico de comparación si hay más de 1 facturador elegido (y 'Todos' NO está seleccionado).
    # Si 'Todos' está seleccionado, no se muestra el gráfico de comparación aquí.
    # Si solo se selecciona un facturador específico, users_to_plot contendrá solo ese facturador.
    users_to_plot_legalizaciones = facturador_seleccionado # Se usará para filtrar el df para el gráfico individual si aplica.

    # Gráfico de barras acumulado para cuando se selecciona "Todos" o más de uno
    if 'Todos' in facturador_seleccionado or len(facturador_seleccionado) > 1:
        st.subheader(f"Total Acumulado de Legalizaciones por Facturador (Periodo: {start_date} a {end_date})")

        plot_df_accumulated = df_filtered_by_facturador_for_display.copy()
        total_legalizaciones_por_usuario_plot = plot_df_accumulated.groupby('Usuario').size().reset_index(name='Total_Legalizaciones_Acumuladas')
        summary_for_accumulated_plot = total_legalizaciones_por_usuario_plot.sort_values(
            'Total_Legalizaciones_Acumuladas', ascending=False
        ).reset_index(drop=True)

        if not summary_for_accumulated_plot.empty:
            fig_all_fact, ax_all_fact = plt.subplots(figsize=(10, max(6, len(summary_for_accumulated_plot) * 0.5)))
            sns.barplot(x='Total_Legalizaciones_Acumuladas', y='Usuario', data=summary_for_accumulated_plot, ax=ax_all_fact, palette='crest', hue='Usuario', legend=False)
            ax_all_fact.set_title(f'Total Acumulado de Legalizaciones por Facturador ({", ".join(tipo_legalizacion_seleccionado)})')
            ax_all_fact.set_xlabel('Total de Legalizaciones Acumuladas')
            ax_all_fact.set_ylabel('Facturador (Usuario)')

            for container in ax_all_fact.containers:
                ax_all_fact.bar_label(container, fmt='%.0f', label_type='edge', padding=5)

            plt.tight_layout()
            st.pyplot(fig_all_fact)
        else:
            st.info("No hay datos para generar el gráfico de total acumulado de legalizaciones con los filtros actuales.")

        # --- Gráfico de Comparación de Evolución de Productividad de Legalizaciones (Múltiples Facturadores) ---
        # Solo se muestra este gráfico si 'Todos' NO está en la selección PERO hay más de un facturador.
        if 'Todos' not in facturador_seleccionado and len(facturador_seleccionado) > 1:
            productivity_comparison_df = df_base_filtered[df_base_filtered['Usuario'].isin(users_to_plot_legalizaciones)].groupby([
                pd.Grouper(key='FECHA_REAL', freq=periodo_seleccionado_code),
                'Usuario'
            ]).size().reset_index(name='Total_Legalizaciones')

            if periodo_seleccionado_code == "D":
                productivity_comparison_df['Periodo'] = productivity_comparison_df['FECHA_REAL'].dt.strftime('%Y-%m-%d')
            elif periodo_seleccionado_code == "5D":
                productivity_comparison_df['Periodo'] = productivity_comparison_df['FECHA_REAL'].dt.strftime('%Y-%m-%d')
            elif periodo_seleccionado_code == "W":
                productivity_comparison_df['Periodo'] = productivity_comparison_df['FECHA_REAL'].dt.strftime('Semana %U - %Y')
            elif periodo_seleccionado_code == "M":
                productivity_comparison_df['Periodo'] = productivity_comparison_df['FECHA_REAL'].dt.strftime('%Y-%m')
            elif periodo_seleccionado_code == "Q":
                productivity_comparison_df['Periodo'] = productivity_comparison_df['FECHA_REAL'].dt.to_period('Q').astype(str)
            elif periodo_seleccionado_code == "Y":
                productivity_comparison_df['Periodo'] = productivity_comparison_df['FECHA_REAL'].dt.year.astype(str)
            else:
                productivity_comparison_df['Periodo'] = productivity_comparison_df['FECHA_REAL'].dt.strftime('%Y-%m-%d')

            productivity_comparison_df.drop(columns=['FECHA_REAL'], inplace=True)
            productivity_comparison_df.sort_values(by=['Periodo', 'Usuario'], inplace=True)

            if not productivity_comparison_df.empty:
                st.subheader(f"Comparación de Evolución de Legalizaciones por Facturador ({periodo_seleccionado_label})")
                fig_comp, ax_comp = plt.subplots(figsize=(14, 7))
                sns.lineplot(x='Periodo', y='Total_Legalizaciones', hue='Usuario', data=productivity_comparison_df, ax=ax_comp, marker='o', palette='tab10')
                ax_comp.set_title(f'Evolución de Legalizaciones por Facturador(es) ({periodo_seleccionado_label}) desde {start_date} hasta {end_date}')
                ax_comp.set_xlabel(f'Periodo ({periodo_seleccionado_label})')
                ax_comp.set_ylabel('Total de Legalizaciones')
                ax_comp.grid(True)
                plt.xticks(rotation=45, ha='right')

                # Añadir los valores a cada punto
                for idx, row in productivity_comparison_df.iterrows():
                    user_list_for_indexing = list(users_to_plot_legalizaciones) # Asegúrate de que esta lista contenga los usuarios reales a graficar
                    if row['Usuario'] in user_list_for_indexing:
                        color_index = user_list_for_indexing.index(row['Usuario']) % len(sns.color_palette('tab10'))
                        ax_comp.text(row['Periodo'], row['Total_Legalizaciones'] + 0.5,
                                     f'{int(row["Total_Legalizaciones"])}',
                                     color=sns.color_palette('tab10')[color_index],
                                     ha='center', va='bottom', fontsize=8)

                plt.tight_layout()
                st.pyplot(fig_comp)
            else:
                st.info("No hay datos para comparar la evolución de legalizaciones con los filtros actuales.")

    # Si solo se selecciona UN facturador específico (y no es 'Todos')
    elif len(facturador_seleccionado) == 1 and 'Todos' not in facturador_seleccionado:
        st.subheader(f"Evolución de Productividad de Legalizaciones para {facturador_seleccionado[0]}")

        # Cálculo de Productividad para la Tabla por Periodo (LEGALIZACIONES) - solo para el gráfico de evolución individual
        productivity_df = df_filtered_by_facturador_for_display.groupby([
            pd.Grouper(key='FECHA_REAL', freq=periodo_seleccionado_code),
            'Usuario'
        ]).size().reset_index(name='Total_Legalizaciones')

        if periodo_seleccionado_code == "D":
            productivity_df['Periodo'] = productivity_df['FECHA_REAL'].dt.strftime('%Y-%m-%d')
        elif periodo_seleccionado_code == "5D":
            productivity_df['Periodo'] = productivity_df['FECHA_REAL'].dt.strftime('%Y-%m-%d')
        elif periodo_seleccionado_code == "W":
            productivity_df['Periodo'] = productivity_df['FECHA_REAL'].dt.strftime('Semana %U - %Y')
        elif periodo_seleccionado_code == "M":
            productivity_df['Periodo'] = productivity_df['FECHA_REAL'].dt.strftime('%Y-%m')
        elif periodo_seleccionado_code == "Q":
            productivity_df['Periodo'] = productivity_df['FECHA_REAL'].dt.to_period('Q').astype(str)
        elif periodo_seleccionado_code == "Y":
            productivity_df['Periodo'] = productivity_df['FECHA_REAL'].dt.year.astype(str)
        else:
            productivity_df['Periodo'] = productivity_df['FECHA_REAL'].dt.strftime('%Y-%m-%d')

        productivity_df.drop(columns=['FECHA_REAL'], inplace=True)
        productivity_df.set_index('Periodo', inplace=True)
        productivity_df.sort_index(inplace=True)

        facturador_evolution_df = productivity_df[productivity_df['Usuario'] == facturador_seleccionado[0]].reset_index().sort_values('Periodo')

        if not facturador_evolution_df.empty:
            fig_fact_evol, ax_fact_evol = plt.subplots(figsize=(12, 6))
            sns.lineplot(x='Periodo', y=metric_to_display_for_evolution, data=facturador_evolution_df, ax=ax_fact_evol, marker='o', color='darkblue')
            ax_fact_evol.set_title(f"Evolución de {metric_to_display_for_evolution.replace('_', ' ')} por {facturador_seleccionado[0]} ({periodo_seleccionado_label}) desde {start_date} hasta {end_date}")
            ax_fact_evol.set_xlabel(f'Periodo ({periodo_seleccionado_label})')
            ax_fact_evol.set_ylabel(metric_to_display_for_evolution.replace("_", " "))
            ax_fact_evol.grid(True)
            plt.xticks(rotation=45, ha='right')

            # Añadir los valores a cada punto
            for x, y in zip(facturador_evolution_df['Periodo'], facturador_evolution_df[metric_to_display_for_evolution]):
                ax_fact_evol.text(x, y + 0.5, f'{int(y)}', color='darkblue', ha='center', va='bottom', fontsize=9)

            plt.tight_layout()
            st.pyplot(fig_fact_evol)
        else:
            st.info("No hay datos para mostrar la evolución del facturador de legalizaciones seleccionado con los filtros actuales.")
    else:
        st.info("No hay datos de legalizaciones válidos cargados para realizar análisis o tu selección de facturadores no es válida.")

else:
    st.info("No hay datos de legalizaciones válidos cargados para realizar análisis.")

# --- LÓGICA DE FILTRADO Y ANÁLISIS DE RIPS ---
if st.session_state.df_rips is not None and not st.session_state.df_rips.empty:
    st.markdown("---")
    st.header("Análisis de RIPS")

    df_rips_base_filtered = st.session_state.df_rips.copy()

    df_rips_base_filtered = df_rips_base_filtered[
        (df_rips_base_filtered['ULTIMA_MODIFICACION'].dt.date >= start_date) &
        (df_rips_base_filtered['ULTIMA_MODIFICACION'].dt.date <= end_date)
    ].copy()

    if 'Todos' not in rips_estado_seleccionado:
        df_rips_filtered_by_estado = df_rips_base_filtered[
            df_rips_base_filtered['ESTADO'].isin(rips_estado_seleccionado)
        ]
    else:
        df_rips_filtered_by_estado = df_rips_base_filtered.copy()

    df_rips_final_filtered = df_rips_filtered_by_estado.copy()
    if 'Todos' not in facturador_seleccionado:
        df_rips_final_filtered = df_rips_final_filtered[
            df_rips_final_filtered['NOMBRE'].isin(facturador_seleccionado)
        ]

    if df_rips_final_filtered.empty:
        st.info(f"No hay RIPS con estado '{', '.join(rips_estado_seleccionado)}' para los filtros de fecha y facturador seleccionados.")
    else:
        st.subheader(f"Resumen Acumulado Total de RIPS ({', '.join(rips_estado_seleccionado)}) por Facturador ({start_date} a {end_date})")

        summary_rips_facturador_table_df = df_rips_filtered_by_estado.copy()
        if 'Todos' not in facturador_seleccionado:
            summary_rips_facturador_table_df = summary_rips_facturador_table_df[
                summary_rips_facturador_table_df['NOMBRE'].isin(facturador_seleccionado)
            ]

        summary_rips_facturador = summary_rips_facturador_table_df.groupby('NOMBRE').size().reset_index(name=f'Total_RIPS_Acumulados')
        summary_rips_facturador = summary_rips_facturador.sort_values(
            f'Total_RIPS_Acumulados', ascending=False
        ).reset_index(drop=True)

        total_general_rips = summary_rips_facturador[f'Total_RIPS_Acumulados'].sum()
        if total_general_rips > 0:
            summary_rips_facturador['Porcentaje_del_Total'] = \
                (summary_rips_facturador[f'Total_RIPS_Acumulados'] / total_general_rips * 100).round(2)
            summary_rips_facturador['Porcentaje_del_Total'] = summary_rips_facturador['Porcentaje_del_Total'].astype(str) + '%'
        else:
            summary_rips_facturador['Porcentaje_del_Total'] = '0%'

        st.dataframe(summary_rips_facturador)
        if summary_rips_facturador.empty:
            st.info("No hay datos de resumen de RIPS para el rango de fechas, estado y facturador seleccionados.")


        st.subheader("Visualización de Productividad de RIPS")

        # Lógica condicional para RIPS
        users_to_plot_rips = facturador_seleccionado

        # Gráfico de barras acumulado para cuando se selecciona "Todos" o más de uno
        if 'Todos' in facturador_seleccionado or len(facturador_seleccionado) > 1:
            st.subheader(f"Total Acumulado de RIPS ({', '.join(rips_estado_seleccionado)}) por Facturador (Periodo: {start_date} a {end_date})")

            plot_df_accumulated_rips = df_rips_filtered_by_estado.copy()
            if 'Todos' not in facturador_seleccionado:
                plot_df_accumulated_rips = plot_df_accumulated_rips[plot_df_accumulated_rips['NOMBRE'].isin(facturador_seleccionado)]

            rips_accumulated_by_user = plot_df_accumulated_rips.groupby('NOMBRE').size().reset_index(name='Total_RIPS_Acumulados')

            if not rips_accumulated_by_user.empty:
                fig_all_rips_fact, ax_all_rips_fact = plt.subplots(figsize=(10, max(6, len(rips_accumulated_by_user) * 0.5)))
                sns.barplot(x=f'Total_RIPS_Acumulados', y='NOMBRE', data=rips_accumulated_by_user, ax=ax_all_rips_fact, palette='viridis', hue='NOMBRE', legend=False)
                ax_all_rips_fact.set_title(f'Total Acumulado de RIPS ({", ".join(rips_estado_seleccionado)}) por Facturador')
                ax_all_rips_fact.set_xlabel(f'Total de RIPS Acumulados')
                ax_all_rips_fact.set_ylabel('Facturador (Nombre)')

                for container in ax_all_rips_fact.containers:
                    ax_all_rips_fact.bar_label(container, fmt='%.0f', label_type='edge', padding=5)

                plt.tight_layout()
                st.pyplot(fig_all_rips_fact)
            else:
                st.info(f"No hay datos para generar el gráfico de total acumulado de RIPS ({', '.join(rips_estado_seleccionado)}) con los filtros actuales.")

            # --- Gráfico de Comparación de Evolución de Productividad de RIPS (Múltiples Facturadores) ---
            if 'Todos' not in facturador_seleccionado and len(facturador_seleccionado) > 1:
                rips_comparison_df = df_rips_filtered_by_estado[df_rips_filtered_by_estado['NOMBRE'].isin(users_to_plot_rips)].groupby([
                    pd.Grouper(key='ULTIMA_MODIFICACION', freq=periodo_seleccionado_code),
                    'NOMBRE'
                ]).size().reset_index(name='Total_RIPS')

                if periodo_seleccionado_code == "D":
                    rips_comparison_df['Periodo'] = rips_comparison_df['ULTIMA_MODIFICACION'].dt.strftime('%Y-%m-%d')
                elif periodo_seleccionado_code == "5D":
                    rips_comparison_df['Periodo'] = rips_comparison_df['ULTIMA_MODIFICACION'].dt.strftime('%Y-%m-%d')
                elif periodo_seleccionado_code == "W":
                    rips_comparison_df['Periodo'] = rips_comparison_df['ULTIMA_MODIFICACION'].dt.strftime('Semana %U - %Y')
                elif periodo_seleccionado_code == "M":
                    rips_comparison_df['Periodo'] = rips_comparison_df['ULTIMA_MODIFICACION'].dt.strftime('%Y-%m')
                elif periodo_seleccionado_code == "Q":
                    rips_comparison_df['Periodo'] = rips_comparison_df['ULTIMA_MODIFICACION'].dt.to_period('Q').astype(str)
                elif periodo_seleccionado_code == "Y":
                    rips_comparison_df['Periodo'] = rips_comparison_df['ULTIMA_MODIFICACION'].dt.year.astype(str)
                else:
                    rips_comparison_df['Periodo'] = rips_comparison_df['ULTIMA_MODIFICACION'].dt.strftime('%Y-%m-%d')

                rips_comparison_df.drop(columns=['ULTIMA_MODIFICACION'], inplace=True)
                rips_comparison_df.sort_values(by=['Periodo', 'NOMBRE'], inplace=True)

                if not rips_comparison_df.empty:
                    st.subheader(f"Comparación de Evolución de RIPS ({', '.join(rips_estado_seleccionado)}) por Facturador ({periodo_seleccionado_label})")
                    fig_rips_comp, ax_rips_comp = plt.subplots(figsize=(14, 7))
                    sns.lineplot(x='Periodo', y='Total_RIPS', hue='NOMBRE', data=rips_comparison_df, ax=ax_rips_comp, marker='o', palette='viridis')
                    ax_rips_comp.set_title(f'Evolución de RIPS ({", ".join(rips_estado_seleccionado)}) por Facturador(es) ({periodo_seleccionado_label}) desde {start_date} hasta {end_date}')
                    ax_rips_comp.set_xlabel(f'Periodo ({periodo_seleccionado_label})')
                    ax_rips_comp.set_ylabel(f'Total de RIPS')
                    ax_rips_comp.grid(True)
                    plt.xticks(rotation=45, ha='right')

                    # Añadir los valores a cada punto
                    for idx, row in rips_comparison_df.iterrows():
                        user_list_for_indexing_rips = list(users_to_plot_rips)
                        if row['NOMBRE'] in user_list_for_indexing_rips:
                            color_index_rips = user_list_for_indexing_rips.index(row['NOMBRE']) % len(sns.color_palette('viridis'))
                            ax_rips_comp.text(row['Periodo'], row['Total_RIPS'] + 0.5,
                                              f'{int(row["Total_RIPS"])}',
                                              color=sns.color_palette('viridis')[color_index_rips],
                                              ha='center', va='bottom', fontsize=8)

                    plt.tight_layout()
                    st.pyplot(fig_rips_comp)
                else:
                    st.info("No hay datos para comparar la evolución de RIPS con los filtros actuales.")


        # Si solo se selecciona UN facturador específico (no 'Todos')
        elif len(facturador_seleccionado) == 1 and 'Todos' not in facturador_seleccionado:
            st.subheader(f"Evolución de Productividad de RIPS para {facturador_seleccionado[0]}")

            rips_evolution_df = df_rips_final_filtered.groupby([
                pd.Grouper(key='ULTIMA_MODIFICACION', freq=periodo_seleccionado_code),
                'NOMBRE'
            ]).size().reset_index(name='Total_RIPS')

            if periodo_seleccionado_code == "D":
                rips_evolution_df['Periodo'] = rips_evolution_df['ULTIMA_MODIFICACION'].dt.strftime('%Y-%m-%d')
            elif periodo_seleccionado_code == "5D":
                rips_evolution_df['Periodo'] = rips_evolution_df['ULTIMA_MODIFICACION'].dt.strftime('%Y-%m-%d')
            elif periodo_seleccionado_code == "W":
                rips_evolution_df['Periodo'] = rips_evolution_df['ULTIMA_MODIFICACION'].dt.strftime('Semana %U - %Y')
            elif periodo_seleccionado_code == "M":
                rips_evolution_df['Periodo'] = rips_evolution_df['ULTIMA_MODIFICACION'].dt.strftime('%Y-%m')
            elif periodo_seleccionado_code == "Q":
                rips_evolution_df['Periodo'] = rips_evolution_df['ULTIMA_MODIFICACION'].dt.to_period('Q').astype(str)
            elif periodo_seleccionado_code == "Y":
                rips_evolution_df['Periodo'] = rips_evolution_df['ULTIMA_MODIFICACION'].dt.year.astype(str)
            else:
                rips_evolution_df['Periodo'] = rips_evolution_df['ULTIMA_MODIFICACION'].dt.strftime('%Y-%m-%d')

            rips_evolution_df.drop(columns=['ULTIMA_MODIFICACION'], inplace=True)
            rips_evolution_df.set_index('Periodo', inplace=True)
            rips_evolution_df.sort_index(inplace=True)

            facturador_rips_evolution_df = rips_evolution_df[rips_evolution_df['NOMBRE'] == facturador_seleccionado[0]].reset_index().sort_values('Periodo')

            if not facturador_rips_evolution_df.empty:
                fig_rips_evol, ax_rips_evol = plt.subplots(figsize=(12, 6))
                sns.lineplot(x='Periodo', y='Total_RIPS', data=facturador_rips_evolution_df, ax=ax_rips_evol, marker='o', color='darkgreen')
                ax_rips_evol.set_title(f'Evolución de RIPS ({", ".join(rips_estado_seleccionado)}) por {facturador_seleccionado[0]} ({periodo_seleccionado_label}) desde {start_date} hasta {end_date}')
                ax_rips_evol.set_xlabel(f'Periodo ({periodo_seleccionado_label})')
                ax_rips_evol.set_ylabel(f'Total de RIPS ({", ".join(rips_estado_seleccionado)})')
                ax_rips_evol.grid(True)
                plt.xticks(rotation=45, ha='right')

                # Añadir los valores a cada punto
                for x, y in zip(facturador_rips_evolution_df['Periodo'], facturador_rips_evolution_df['Total_RIPS']):
                    ax_rips_evol.text(x, y + 0.5, f'{int(y)}', color='darkgreen', ha='center', va='bottom', fontsize=9)

                plt.tight_layout()
                st.pyplot(fig_rips_evol)
            else:
                st.info(f"No hay datos de RIPS ({', '.join(rips_estado_seleccionado)}) para mostrar la evolución del facturador seleccionado con los filtros actuales.")
        else:
            st.info("No hay datos de RIPS válidos cargados para realizar análisis o tu selección de facturadores no es válida.")

else:
    st.info("Por favor, sube el archivo de RIPS para ver el análisis de RIPS por facturador.")


# --- LÓGICA DE FILTRADO Y ANÁLISIS DE FACTURACIÓN ---
if st.session_state.df_facturacion is not None and not st.session_state.df_facturacion.empty:
    st.markdown("---")
    st.header("Análisis de Facturación")

    df_facturacion_base_filtered = st.session_state.df_facturacion[
        (st.session_state.df_facturacion['FECHA FACTURA'].dt.date >= start_date) &
        (st.session_state.df_facturacion['FECHA FACTURA'].dt.date <= end_date)
    ].copy()

    if 'Todos' not in tipo_facturacion_seleccionado and 'Tipo_Facturacion' in df_facturacion_base_filtered.columns:
        df_facturacion_filtered_by_type = df_facturacion_base_filtered[df_facturacion_base_filtered['Tipo_Facturacion'].isin(tipo_facturacion_seleccionado)]
    else:
        df_facturacion_filtered_by_type = df_facturacion_base_filtered.copy()

    df_facturacion_final_display = df_facturacion_filtered_by_type.copy()
    if 'Todos' not in facturador_seleccionado:
        df_facturacion_final_display = df_facturacion_final_display[
            df_facturacion_final_display['USUARIO'].isin(facturador_seleccionado)
        ]


    if df_facturacion_final_display.empty:
        st.info(f"No hay datos de Facturación para el tipo '{', '.join(tipo_facturacion_seleccionado)}', facturador '{', '.join(facturador_seleccionado)}' y el rango de fechas seleccionados.")
    else:
        st.subheader(f"Resumen Acumulado Total de Facturación por Facturador ({start_date} a {end_date})")

        summary_facturacion_facturador_table_df = df_facturacion_filtered_by_type.copy()
        if 'Todos' not in facturador_seleccionado:
            summary_facturacion_facturador_table_df = summary_facturacion_facturador_table_df[
                summary_facturacion_facturador_table_df['USUARIO'].isin(facturador_seleccionado)
            ]

        summary_facturacion_facturador = summary_facturacion_facturador_table_df.groupby('USUARIO').size().reset_index(name='Total_Facturacion_Acumulada')
        summary_facturacion_facturador = summary_facturacion_facturador.sort_values(
            'Total_Facturacion_Acumulada', ascending=False
        ).reset_index(drop=True)

        total_general_facturacion = summary_facturacion_facturador['Total_Facturacion_Acumulada'].sum()
        if total_general_facturacion > 0:
            summary_facturacion_facturador['Porcentaje_del_Total'] = \
                (summary_facturacion_facturador['Total_Facturacion_Acumulada'] / total_general_facturacion * 100).round(2)
            summary_facturacion_facturador['Porcentaje_del_Total'] = summary_facturacion_facturador['Porcentaje_del_Total'].astype(str) + '%'
        else:
            summary_facturacion_facturador['Porcentaje_del_Total'] = '0%'

        st.dataframe(summary_facturacion_facturador)
        if summary_facturacion_facturador.empty:
            st.info("No hay datos de resumen de Facturación para el rango de fechas, tipo y facturador seleccionados.")


        st.subheader("Visualización de Productividad de Facturación")

        # Lógica condicional para Facturación
        users_to_plot_facturacion = facturador_seleccionado

        # Gráfico de barras acumulado para cuando se selecciona "Todos" o más de uno
        if 'Todos' in facturador_seleccionado or len(facturador_seleccionado) > 1:
            st.subheader(f"Total Acumulado de Facturación por Facturador (Periodo: {start_date} a {end_date})")

            plot_df_accumulated_fact = df_facturacion_filtered_by_type.copy()
            if 'Todos' not in facturador_seleccionado:
                plot_df_accumulated_fact = plot_df_accumulated_fact[plot_df_accumulated_fact['USUARIO'].isin(facturador_seleccionado)]

            facturacion_accumulated_by_user = plot_df_accumulated_fact.groupby('USUARIO').size().reset_index(name='Total_Facturacion_Acumulada')

            if not facturacion_accumulated_by_user.empty:
                fig_all_facturacion_fact, ax_all_facturacion_fact = plt.subplots(figsize=(10, max(6, len(facturacion_accumulated_by_user) * 0.5)))
                sns.barplot(x='Total_Facturacion_Acumulada', y='USUARIO', data=facturacion_accumulated_by_user, ax=ax_all_facturacion_fact, palette='cividis', hue='USUARIO', legend=False)
                ax_all_facturacion_fact.set_title(f'Total Acumulado de Facturación por Facturador ({", ".join(tipo_facturacion_seleccionado)})')
                ax_all_facturacion_fact.set_xlabel(f'Total de Facturas Acumuladas')
                ax_all_facturacion_fact.set_ylabel('Facturador (Usuario)')

                for container in ax_all_facturacion_fact.containers:
                    ax_all_facturacion_fact.bar_label(container, fmt='%.0f', label_type='edge', padding=5)

                plt.tight_layout()
                st.pyplot(fig_all_facturacion_fact)
            else:
                st.info("No hay datos para generar el gráfico de total acumulado de facturación con los filtros actuales.")

            # --- Nuevo Gráfico de Comparación de Productividad de Facturación (Múltiples Facturadores) ---
            if 'Todos' not in facturador_seleccionado and len(facturador_seleccionado) > 1:
                facturacion_comparison_df = df_facturacion_filtered_by_type[df_facturacion_filtered_by_type['USUARIO'].isin(users_to_plot_facturacion)].groupby([
                    pd.Grouper(key='FECHA FACTURA', freq=periodo_seleccionado_code),
                    'USUARIO'
                ]).size().reset_index(name='Total_Facturacion')

                if periodo_seleccionado_code == "D":
                    facturacion_comparison_df['Periodo'] = facturacion_comparison_df['FECHA FACTURA'].dt.strftime('%Y-%m-%d')
                elif periodo_seleccionado_code == "5D":
                    facturacion_comparison_df['Periodo'] = facturacion_comparison_df['FECHA FACTURA'].dt.strftime('%Y-%m-%d')
                elif periodo_seleccionado_code == "W":
                    facturacion_comparison_df['Periodo'] = facturacion_comparison_df['FECHA FACTURA'].dt.strftime('Semana %U - %Y')
                elif periodo_seleccionado_code == "M":
                    facturacion_comparison_df['Periodo'] = facturacion_comparison_df['FECHA FACTURA'].dt.strftime('%Y-%m')
                elif periodo_seleccionado_code == "Q":
                    facturacion_comparison_df['Periodo'] = facturacion_comparison_df['FECHA FACTURA'].dt.to_period('Q').astype(str)
                elif periodo_seleccionado_code == "Y":
                    facturacion_comparison_df['Periodo'] = facturacion_comparison_df['FECHA FACTURA'].dt.year.astype(str)
                else:
                    facturacion_comparison_df['Periodo'] = facturacion_comparison_df['FECHA FACTURA'].dt.strftime('%Y-%m-%d')

                facturacion_comparison_df.drop(columns=['FECHA FACTURA'], inplace=True)
                facturacion_comparison_df.sort_values(by=['Periodo', 'USUARIO'], inplace=True)

                if not facturacion_comparison_df.empty:
                    st.subheader(f"Comparación de Evolución de Facturación por Facturador ({periodo_seleccionado_label})")
                    fig_fact_comp, ax_fact_comp = plt.subplots(figsize=(14, 7))
                    sns.lineplot(x='Periodo', y='Total_Facturacion', hue='USUARIO', data=facturacion_comparison_df, ax=ax_fact_comp, marker='o', palette='cividis')
                    ax_fact_comp.set_title(f'Evolución de Facturación por Facturador(es) ({periodo_seleccionado_label}) desde {start_date} hasta {end_date}')
                    ax_fact_comp.set_xlabel(f'Periodo ({periodo_seleccionado_label})')
                    ax_fact_comp.set_ylabel('Total de Facturas')
                    ax_fact_comp.grid(True)
                    plt.xticks(rotation=45, ha='right')

                    # Añadir los valores a cada punto
                    for idx, row in facturacion_comparison_df.iterrows():
                        user_list_for_indexing_facturacion = list(users_to_plot_facturacion)
                        if row['USUARIO'] in user_list_for_indexing_facturacion:
                            color_index_facturacion = user_list_for_indexing_facturacion.index(row['USUARIO']) % len(sns.color_palette('cividis'))
                            ax_fact_comp.text(row['Periodo'], row['Total_Facturacion'] + 0.5,
                                              f'{int(row["Total_Facturacion"])}',
                                              color=sns.color_palette('cividis')[color_index_facturacion],
                                              ha='center', va='bottom', fontsize=8)

                    plt.tight_layout()
                    st.pyplot(fig_fact_comp)
                else:
                    st.info("No hay datos para comparar la evolución de facturación con los filtros actuales.")


        # Si solo se selecciona UN facturador específico (no 'Todos')
        elif len(facturador_seleccionado) == 1 and 'Todos' not in facturador_seleccionado:
            st.subheader(f"Evolución de Productividad de Facturación para {facturador_seleccionado[0]}")

            facturacion_evolution_df = df_facturacion_final_display.groupby([
                pd.Grouper(key='FECHA FACTURA', freq=periodo_seleccionado_code),
                'USUARIO'
            ]).size().reset_index(name='Total_Facturacion')

            if periodo_seleccionado_code == "D":
                facturacion_evolution_df['Periodo'] = facturacion_evolution_df['FECHA FACTURA'].dt.strftime('%Y-%m-%d')
            elif periodo_seleccionado_code == "5D":
                facturacion_evolution_df['Periodo'] = facturacion_evolution_df['FECHA FACTURA'].dt.strftime('%Y-%m-%d')
            elif periodo_seleccionado_code == "W":
                facturacion_evolution_df['Periodo'] = facturacion_evolution_df['FECHA FACTURA'].dt.strftime('Semana %U - %Y')
            elif periodo_seleccionado_code == "M":
                facturacion_evolution_df['Periodo'] = facturacion_evolution_df['FECHA FACTURA'].dt.strftime('%Y-%m')
            elif periodo_seleccionado_code == "Q":
                facturacion_evolution_df['Periodo'] = facturacion_evolution_df['FECHA FACTURA'].dt.to_period('Q').astype(str)
            elif periodo_seleccionado_code == "Y":
                facturacion_evolution_df['Periodo'] = facturacion_evolution_df['FECHA FACTURA'].dt.year.astype(str)
            else:
                facturacion_evolution_df['Periodo'] = facturacion_evolution_df['FECHA FACTURA'].dt.strftime('%Y-%m-%d')

            facturacion_evolution_df.drop(columns=['FECHA FACTURA'], inplace=True)
            facturacion_evolution_df.set_index('Periodo', inplace=True)
            facturacion_evolution_df.sort_index(inplace=True)

            facturador_facturacion_evolution_df = facturacion_evolution_df[facturacion_evolution_df['USUARIO'] == facturador_seleccionado[0]].reset_index().sort_values('Periodo')

            if not facturador_facturacion_evolution_df.empty:
                fig_facturacion_evol, ax_facturacion_evol = plt.subplots(figsize=(12, 6))
                sns.lineplot(x='Periodo', y='Total_Facturacion', data=facturador_facturacion_evolution_df, ax=ax_facturacion_evol, marker='o', color='darkorange')
                ax_facturacion_evol.set_title(f'Evolución de Facturación por {facturador_seleccionado[0]} ({periodo_seleccionado_label}) desde {start_date} hasta {end_date}')
                ax_facturacion_evol.set_xlabel(f'Periodo ({periodo_seleccionado_label})')
                ax_facturacion_evol.set_ylabel(f'Total de Facturas')
                ax_facturacion_evol.grid(True)
                plt.xticks(rotation=45, ha='right')

                # Añadir los valores a cada punto
                for x, y in zip(facturador_facturacion_evolution_df['Periodo'], facturador_facturacion_evolution_df['Total_Facturacion']):
                    ax_facturacion_evol.text(x, y + 0.5, f'{int(y)}', color='darkorange', ha='center', va='bottom', fontsize=9)

                plt.tight_layout()
                st.pyplot(fig_facturacion_evol)
            else:
                st.info(f"No hay datos de Facturación para mostrar la evolución del facturador seleccionado con los filtros actuales.")
        else:
            st.info("No hay datos de Facturación válidos cargados para realizar análisis o tu selección de facturadores no es válida.")

else:
    st.info("Por favor, sube el archivo de Facturación para ver el análisis de Facturación por facturador.")


st.markdown("---")
st.markdown("Creado desde el área de Facturación por Dilan Heredia")
