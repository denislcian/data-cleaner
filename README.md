# Ultimate Data Cleaner Pipeline

Un pipeline de datos robusto y profesional diseñado para ingestar, limpiar y normalizar datasets de diversas fuentes (CSV, Excel, SQL, Parquet, JSON).

## Características

-   **Ingestión Universal**: Soporte para CSV, Excel, JSON, Parquet y SQL.
-   **Estandarización**: Normalización automática de nombres de columnas y strings.
-   **Limpieza Inteligente**: Eliminación de duplicados y filas vacías.
-   **Imputación Estadística**: Relleno de valores nulos usando mediana (numéricos) y moda (categóricos).
-   **Manejo de Outliers**: Suavizado de valores extremos mediante IRQ (Winsorizing).
-   **Optimización**: Conversión automática de tipos para ahorrar memoria.
-   **Logging**: Registro detallado de operaciones para depuración.

## Requisitos

-   Python 3.8+
-   Dependencias listadas en `requirements.txt`

## Instalación

1.  Clonar el repositorio:
    ```bash
    git clone https://github.com/denislcian/data-cleaner.git
    cd data-cleaner
    ```

2.  Instalar dependencias:
    ```bash
    pip install -r requirements.txt
    ```

## Uso

```python
from data_cleaner import UltimateDataPipeline

# Inicializar pipeline con un archivo sucio
pipeline = UltimateDataPipeline('datos_crudos.csv')

# Ejecutar el flujo de limpieza y exportar
(pipeline
 .standardize()
 .handle_garbage()
 .impute_missing()
 .handle_outliers()
 .optimize()
 .export('datos_limpios.xlsx', format='excel'))
```
