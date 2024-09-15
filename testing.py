import csv
import datetime
import pandas as pd

from connections_mit import Oracle,Spark
# COMMAND ----------

# QUERY A ORACLE
sql = """SELECT CD_EMPRESA, 
            CD_SUCURSAL, 
            ST_CHIP, 
            ST_BANDA, 
            ST_EXTRANJERA, 
            IM_MINIMO, 
            IM_MAXIMO, 
            CAST(NU_TDC_X_HRS AS INTEGER) NU_TDC_X_HRS,
            CAST(NU_HRS AS INTEGER) NU_HRS,
            ST_DEBITO, 
            ST_CREDITO, 
            TM_REGISTRO, 
            TM_MODIFICACION, 
            CD_USR_ROOT
FROM PRV03_REGLAS_PRV_SUC""" # --- ESTE CAMPO VARIA DEPENDIENDO DEL ETL --- #

df = Spark.get_oracle_table(sql)

# COMMAND ----------

# --------------------------------------------------------- #
# Si no hay registros en la tabla origen, el job se detiene #
# --------------------------------------------------------- #

if (df.isEmpty()):
  raise Exception('No se obtuvieron nuevos registros de la tabla origen')

# COMMAND ----------

df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").saveAsTable('main.landing.prv03_reglas_prv_suc')

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE main.landing.prv03_reglas_prv_suc;