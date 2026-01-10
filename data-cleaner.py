import pandas as pd
import numpy as np
import os
import logging
from pathlib import Path
from sqlalchemy import create_engine
from typing import Optional, Union, Any, Dict

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class UltimateDataPipeline:
    def __init__(self, source: Union[str, Path], is_sql: bool = False, query: Optional[str] = None):
        """
        Punto de entrada único. Protege la fuente original con .copy()
        """
        self.df: pd.DataFrame = self._ingest(source, is_sql, query)
        self.report: Dict[str, Any] = {"initial_shape": self.df.shape}
        logger.info(f"📥 Datos cargados. Filas: {self.df.shape[0]}, Columnas: {self.df.shape[1]}")

    def _ingest(self, source: Union[str, Path], is_sql: bool, query: Optional[str]) -> pd.DataFrame:
        """Capa de Ingestión: Maneja la complejidad de los formatos."""
        try:
            if is_sql:
                engine = create_engine(str(source))
                if query is None:
                    raise ValueError("Se requiere una query para fuentes SQL")
                return pd.read_sql(query, engine)
            
            source_path = Path(source)
            if not source_path.exists():
                raise FileNotFoundError(f"El archivo {source} no existe.")

            ext = source_path.suffix.lower()
            loaders = {
                '.csv': pd.read_csv,
                '.xlsx': pd.read_excel,
                '.xls': pd.read_excel,
                '.json': pd.read_json,
                '.parquet': pd.read_parquet
            }
            if ext not in loaders:
                raise ValueError(f"Formato {ext} no soportado.")
            
            return loaders[ext](source_path)
        except Exception as e:
            logger.error(f"⚠️ Error en carga: {e}")
            return pd.DataFrame()

    def standardize(self) -> 'UltimateDataPipeline':
        """1. Limpieza Estructural: Nombres de columnas profesionales."""
        if self.df.empty:
            logger.warning("DataFrame vacío, saltando estandarización.")
            return self

        self.df.columns = (self.df.columns
                           .str.strip()
                           .str.lower()
                           .str.replace(' ', '_')
                           .str.replace(r'[^\w\s]', '', regex=True))
        
        # Limpiar strings en las celdas (quitar espacios extra)
        str_cols = self.df.select_dtypes(include=['object']).columns
        for col in str_cols:
            self.df[col] = self.df[col].astype(str).str.strip()
        
        logger.info("✅ Columnas y textos estandarizados.")
        return self

    def handle_garbage(self) -> 'UltimateDataPipeline':
        """2. Eliminación de duplicados y filas vacías."""
        if self.df.empty: 
            return self

        before = len(self.df)
        self.df.drop_duplicates(inplace=True)
        self.df.dropna(how='all', inplace=True) # Elimina filas totalmente vacías
        
        removed = before - len(self.df)
        if removed > 0:
            logger.info(f"✅ Duplicados y filas vacías eliminados ({removed} registros).")
        else:
            logger.info("✅ No se encontraron duplicados ni filas vacías.")
        return self

    def impute_missing(self) -> 'UltimateDataPipeline':
        """3. Tratamiento estadístico de nulos."""
        if self.df.empty:
            return self

        imputed_count = 0
        for col in self.df.columns:
            if self.df[col].isnull().any():
                imputed_count += 1
                if pd.api.types.is_numeric_dtype(self.df[col]):
                    # Usamos mediana: no se ve afectada por outliers
                    median_val = self.df[col].median()
                    self.df[col] = self.df[col].fillna(median_val)
                else:
                    # Usamos la moda para categorías
                    mode_series = self.df[col].mode()
                    if not mode_series.empty:
                        self.df[col] = self.df[col].fillna(mode_series[0])
        
        if imputed_count > 0:
            logger.info(f"✅ Valores nulos imputados estadísticamente en {imputed_count} columnas.")
        else:
            logger.info("✅ No se detectaron valores nulos para imputar.")
        return self

    def handle_outliers(self, threshold: float = 1.5, method: str = 'cap') -> 'UltimateDataPipeline':
        """
        4. Manejo de valores extremos (outliers) mediante IQR.
        
        Args:
            threshold (float): Multiplicador del IQR (rango intercuartílico). Default 1.5.
            method (str): Estrategia de manejo. 
                          'cap': Suaviza valores (Winsorizing) al límite superior/inferior.
                          'remove': Elimina las filas completas que contienen outliers.
        """
        if self.df.empty:
            return self

        num_cols = self.df.select_dtypes(include=[np.number]).columns
        
        if method == 'remove':
            initial_rows = len(self.df)
            mask = pd.Series(True, index=self.df.index)
            
            for col in num_cols:
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                if IQR == 0: continue

                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                
                # Identificar filas que están DENTRO del rango permitido
                col_mask = (self.df[col] >= lower_bound) & (self.df[col] <= upper_bound)
                # Ojo: los NaNs a veces evalúan False en comparaciones, aseguramos no eliminar NaNs aquí (eso es tarea de impute_missing)
                # Si queremos ser estrictos con outliers de lo que NO es NaN:
                col_valid = self.df[col].notna()
                col_mask = col_mask | (~col_valid) # Mantenemos NaNs
                
                mask = mask & col_mask

            self.df = self.df[mask]
            removed = initial_rows - len(self.df)
            if removed > 0:
                logger.info(f"✅ Eliminadas {removed} filas con outliers.")
            else:
                logger.info("✅ No se detectaron outliers para eliminar.")

        elif method == 'cap':
            outlier_cols = 0
            for col in num_cols:
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                if IQR == 0: continue

                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                
                has_outliers = ((self.df[col] < lower_bound) | (self.df[col] > upper_bound)).any()
                if has_outliers:
                    outlier_cols += 1
                    self.df[col] = np.where(self.df[col] < lower_bound, lower_bound, self.df[col])
                    self.df[col] = np.where(self.df[col] > upper_bound, upper_bound, self.df[col])
            
            if outlier_cols > 0:
                logger.info(f"✅ Outliers suavizados (Winsorizing) en {outlier_cols} columnas.")
            else:
                logger.info("✅ No se detectaron outliers significativos.")
        
        else:
            logger.error(f"❌ Método '{method}' no reconocido. Use 'cap' o 'remove'.")
            raise ValueError(f"Método '{method}' no válido.")

        return self

    def optimize(self) -> 'UltimateDataPipeline':
        """5. Optimización de tipos y memoria."""
        if self.df.empty:
            return self

        for col in self.df.columns:
            # Detectar fechas
            if 'fecha' in col.lower() or 'date' in col.lower():
                try:
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                except Exception:
                    pass # Si falla, dejamos como está
            
            # Categorizar si hay pocos valores únicos
            if self.df[col].dtype == 'object':
                if len(self.df) > 0 and (self.df[col].nunique() / len(self.df) < 0.1):
                    self.df[col] = self.df[col].astype('category')
        
        logger.info("✅ Tipos de datos optimizados para memoria.")
        return self

    def export(self, path: Union[str, Path], format: str = 'csv') -> None:
        """6. Capa de Salida: Exportación versátil."""
        if self.df.empty:
            logger.warning("⚠️ El DataFrame está vacío. No se exportará nada.")
            return

        try:
            path_str = str(path)
            if format == 'csv': 
                self.df.to_csv(path_str, index=False)
            elif format == 'excel': 
                self.df.to_excel(path_str, index=False)
            elif format == 'sql':
                engine = create_engine(path_str) #path es connection URL
                self.df.to_sql('data_limpia', engine, if_exists='replace')
            else:
                raise ValueError(f"Formato de exportación '{format}' no soportado.")
            
            logger.info(f"💾 Datos exportados exitosamente a {path_str}")
        except Exception as e:
            logger.error(f"❌ Error al exportar: {e}")

# --- USO DEL PIPELINE ---
if __name__ == "__main__":
    # Ejemplo de uso seguro con manejo de archivos
    file_path = Path('mi_data_sucia.csv')
    
    # Creamos un archivo dummy si no existe para probar
    # Forzamos recreación para probar cambios
    logger.info("Creando archivo dummy para prueba...")
    df_dummy = pd.DataFrame({
        'Nombre ': ['Juan', 'Ana', 'Juan', ' Pedro ', 'OutlierMan'],
        ' Edad': [25, 30, 25, 120, 1500], # 120 y 1500 son outliers
        'Fecha Registro': ['2023-01-01', '2023-02-01', '2023-01-01', None, '2023-05-01'],
        'Score': [10.5, np.nan, 10.5, 5.0, 1000.0] # 1000 es outlier
    })
    df_dummy.to_csv(file_path, index=False)

    print("\n--- PRUEBA 1: Winsorizing (cap) ---")
    pipeline = UltimateDataPipeline(file_path)
    (pipeline
     .standardize()
     .handle_outliers(method='cap')
     .df)
    
    print("\n--- PRUEBA 2: Eliminación (remove) ---")
    pipeline_remove = UltimateDataPipeline(file_path)
    (pipeline_remove
     .standardize()
     .handle_outliers(method='remove')
     .export('resultado_limpio.xlsx', format='excel'))
