# Ultimate Data Cleaner Pipeline

Un pipeline de datos robusto y profesional diseñado para ingestar, limpiar y normalizar datasets de diversas fuentes.

## 🚀 Características Principales

*   **Ingestión Agnóstica**: Lee CSV, Excel (.xlsx, .xls), JSON, Parquet y bases de datos SQL automáticamente.
*   **Limpieza Estructural**: Convierte nombres de columnas a `snake_case` limpio y elimina caracteres especiales.
*   **Sanitización de Datos**: Elimina duplicados exactos y filas vacías.
*   **Imputación Inteligente**: Rellena valores nulos basándose en estadísticas (Mediana para numéricos, Moda para categóricos).
*   **Manejo de Outliers Flexible**: Permite elegir entre suavizar (cap) o eliminar registros extremos.
*   **Optimización de Memoria**: Detecta tipos de datos (fechas, categorías) para reducir el uso de RAM.
*   **Logging Integrado**: Trazabilidad completa mediante `logging` en lugar de `print`.

## 📦 Instalación

1.  Clonar el repositorio:
    ```bash
    git clone https://github.com/denislcian/data-cleaner.git
    cd data-cleaner
    ```

2.  Instalar dependencias:
    ```bash
    pip install -r requirements.txt
    ```

## 🛠️ Guía de Uso Detallada

### Inicialización

La clase `UltimateDataPipeline` es el punto de entrada. Al instanciarla, los datos se cargan inmediatamente en un DataFrame de Pandas protegido (copia).

```python
from data_cleaner import UltimateDataPipeline

# Carga desde archivo local
pipeline = UltimateDataPipeline('ventas_2023.csv')

# Carga desde SQL
query_sql = "SELECT * FROM transactions WHERE year = 2023"
pipeline = UltimateDataPipeline('sqlite:///database.db', is_sql=True, query=query_sql)
```

### Métodos del Pipeline

Cada método retorna `self`, permitiendo encadenar operaciones (Method Chaining).

#### 1. `standardize()`
Normaliza los nombres de las columnas y limpia espacios en blanco de textos.
*   **Acciones**: 
    *   Columnas a minúsculas, espacios -> guiones bajos (`_`), elimina caracteres no alfanuméricos.
    *   `strip()` en todas las columnas de texto.
*   **Ejemplo**: `Column Name #1` -> `column_name_1`

#### 2. `handle_garbage()`
Elimina "basura" obvia del dataset.
*   **Acciones**:
    *   Elimina filas duplicadas exactas.
    *   Elimina filas que tienen *todos* sus valores como `NaN`.

#### 3. `impute_missing()`
Trata los valores nulos (`NaN`) sin perder filas.
*   **Lógica**:
    *   **Numéricos**: Rellena con la **mediana** (robusta a outliers).
    *   **Categóricos/Texto**: Rellena con la **moda** (valor más frecuente).

#### 4. `handle_outliers(threshold=1.5, method='cap')`
Maneja valores numéricos extremos usando el Rango Intercuartílico (IQR).

*   **Parámetros**:
    *   `threshold` (float): Sensibilidad. 1.5 es el estándar estadístico. Mayor valor = menos estricto.
    *   `method` (str):
        *   `'cap'` (Por defecto): **Winsorizing**. Reemplaza los valores extremos por los límites superior/inferior calculados. Útil para preservar datos.
        *   `'remove'`: **Eliminación**. Borra la fila completa si contiene algún outlier en sus columnas numéricas. Útil para máxima pureza.

#### 5. `optimize()`
Reduce el uso de memoria y corrige tipos de datos.
*   **Acciones**:
    *   Detecta columnas con nombres como 'fecha', 'date' y las convierte a `datetime`.
    *   Convierte columnas de texto con baja cardinalidad (<10% de valores únicos) a tipo `category`.

#### 6. `export(path, format='csv')`
Guarda el resultado final.
*   **Formatos**: `'csv'`, `'excel'`, `'sql'`.
*   **Nota**: Para SQL, `path` debe ser una connection string de SQLAlchemy.

## 💡 Ejemplo Completo

```python
pipeline = UltimateDataPipeline('raw_customer_data.json')

(pipeline
 .standardize()          # Limpia nombres de columnas
 .handle_garbage()       # Quita duplicados
 .impute_missing()       # Rellena nulos
 .handle_outliers(method='remove', threshold=2.0) # Elimina extremos agresivos
 .optimize()             # Optimiza memoria
 .export('clean_data.parquet', format='parquet')) # (Requiere pequeña adaptación para parquet si se desea)
```

## 📋 Requisitos
*   pandas
*   numpy
*   sqlalchemy
*   openpyxl (para Excel)
