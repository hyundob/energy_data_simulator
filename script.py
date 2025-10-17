import pandas as pd
import psycopg2
from datetime import datetime, timedelta

# 초기 데이터 설정
start_hour = 9  # 시작 시간 (09:00)
num_records = 10  # 생성할 데이터 수
base_date = datetime(2025, 8, 1)

data = []
qgen_values = [0.45, 0.50, 0.55, 0.60, 0.63, 0.65, 0.62, 0.58, 0.50, 0.42]
qgmx_values = [0.52, 0.58, 0.62, 0.68, 0.70, 0.72, 0.70, 0.66, 0.58, 0.50]
qgmn_values = [0.40, 0.44, 0.48, 0.52, 0.55, 0.58, 0.56, 0.50, 0.44, 0.36]

for i in range(num_records):
    crtn_tm = (base_date + timedelta(hours=start_hour + i)).strftime('%Y%m%d%H%M')
    fcst_tm = (base_date + timedelta(hours=start_hour + i + 2)).strftime('%Y%m%d%H%M')

    data.append({
        'pwr_exc_tp_cd': '01',
        'fuel_tp_cd': 'SOLAR',
        'crtn_tm': crtn_tm,
        'fcst_tm': fcst_tm,
        'lead_tm': '0200',
        'fcst_prod_cd': '01',
        'fcst_qgen': qgen_values[i],
        'fcst_qgmx': qgmx_values[i],
        'fcst_qgmn': qgmn_values[i],
        'fcst_capa': 0.5,
        'ess_chrg': 'NULL',
        'ess_disc': 'NULL',
        'ess_capa': 'NULL',
        'reg_date': 'current_timestamp',
        'upd_date': 'current_timestamp'
    })

df = pd.DataFrame(data)

# SQL 문장 생성
sql_insert = "INSERT INTO rep_data_re_fcst_gen_da (\n" \
            "    pwr_exc_tp_cd,\n" \
            "    fuel_tp_cd,\n" \
            "    crtn_tm,\n" \
            "    fcst_tm,\n" \
            "    lead_tm,\n" \
            "    fcst_prod_cd,\n" \
            "    fcst_qgen,\n" \
            "    fcst_qgmx,\n" \
            "    fcst_qgmn,\n" \
            "    fcst_capa,\n" \
            "    ess_chrg,\n" \
            "    ess_disc,\n" \
            "    ess_capa,\n" \
            "    reg_date,\n" \
            "    upd_date\n" \
            ")\nVALUES\n"

values = []
for row in df.itertuples(index=False):
    value = f"('{row.pwr_exc_tp_cd}', '{row.fuel_tp_cd}', '{row.crtn_tm}', '{row.fcst_tm}', '{row.lead_tm}', " \
            f"'{row.fcst_prod_cd}', {row.fcst_qgen}, {row.fcst_qgmx}, {row.fcst_qgmn}, {row.fcst_capa}, " \
            f"{row.ess_chrg}, {row.ess_disc}, {row.ess_capa}, {row.reg_date}, {row.upd_date})"
    values.append(value)

sql_insert += ",\n".join(values) + ";"

print(sql_insert)