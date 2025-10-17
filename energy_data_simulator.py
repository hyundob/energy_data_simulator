import random
import datetime
import argparse
from decimal import Decimal, ROUND_HALF_UP
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import time
import threading

# 환경변수 로드 (선택사항)
load_dotenv()

def get_db_connection():
    """
    PostgreSQL 데이터베이스 연결
    """
    try:
        # 환경변수에서 설정을 가져오거나 기본값 사용
        host = os.getenv('DB_HOST', '192.168.0.3')
        port = os.getenv('DB_PORT', '15432')
        database = os.getenv('DB_NAME', 'megacitydb')
        user = os.getenv('DB_USER', 'postgres')
        password = os.getenv('DB_PASSWORD', 'tef123!@#')
        
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        return connection
    except psycopg2.Error as e:
        print(f"데이터베이스 연결 오류: {e}")
        return None

def create_table_if_not_exists(connection):
    """
    테이블이 존재하지 않으면 생성
    """
    # REP_DATA_RE_FCST_LFD_DA 테이블 생성
    create_table_lfd_sql = """
    CREATE TABLE IF NOT EXISTS REP_DATA_RE_FCST_LFD_DA (
        id SERIAL PRIMARY KEY,
        CRTN_TM VARCHAR(12) NOT NULL,
        FCST_TM VARCHAR(12) NOT NULL,
        LEAD_TM VARCHAR(5) NOT NULL,
        FCST_PROD_CD VARCHAR(2) NOT NULL,
        FCST_QG01 DECIMAL(13,6),
        FCST_QG02 DECIMAL(13,6),
        FCST_QG03 DECIMAL(13,6),
        FCST_QG04 DECIMAL(13,6),
        FCST_QG05 DECIMAL(13,6),
        FCST_QG06 DECIMAL(13,6),
        FCST_QGEN DECIMAL(13,6),
        FCST_QGMX DECIMAL(13,6),
        FCST_QGMN DECIMAL(13,6),
        REG_DATE TIMESTAMP,
        UPD_DATE TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # REP_DATA_RE_FCST_GEN_DA 테이블 생성
    create_table_gen_sql = """
    CREATE TABLE IF NOT EXISTS REP_DATA_RE_FCST_GEN_DA (
        PWR_EXC_TP_CD VARCHAR(2) NOT NULL,
        FUEL_TP_CD VARCHAR(20) NOT NULL,
        CRTN_TM VARCHAR(12) NOT NULL,
        FCST_TM VARCHAR(12) NOT NULL,
        LEAD_TM VARCHAR(5),
        FCST_PROD_CD VARCHAR(2),
        FCST_QG01 DECIMAL(13,6),
        FCST_QG02 DECIMAL(13,6),
        FCST_QG03 DECIMAL(13,6),
        FCST_QG04 DECIMAL(13,6),
        FCST_QG05 DECIMAL(13,6),
        FCST_QG06 DECIMAL(13,6),
        FCST_QGEN DECIMAL(13,6),
        FCST_QGMX DECIMAL(13,6),
        FCST_QGMN DECIMAL(13,6),
        FCST_CAPA DECIMAL(13,6),
        ESS_CHRG DECIMAL(13,6),
        ESS_DISC DECIMAL(13,6),
        ESS_CAPA DECIMAL(13,6),
        REG_DATE TIMESTAMP DEFAULT SYSDATE,
        UPD_DATE TIMESTAMP DEFAULT SYSDATE,
        PRIMARY KEY (PWR_EXC_TP_CD, FUEL_TP_CD, CRTN_TM, FCST_TM)
    );
    """
    
    # REP_DATA_HG_FCST_NWP_DA 테이블 생성
    create_table_nwp_sql = """
    CREATE TABLE IF NOT EXISTS REP_DATA_HG_FCST_NWP_DA (
        PWR_EXC_TP_CD VARCHAR(2) NOT NULL,
        AREA_GRP_CD VARCHAR(20) NOT NULL,
        AREA_GRP_ID VARCHAR(20) NOT NULL,
        CRTN_TM VARCHAR(12) NOT NULL,
        FCST_TM VARCHAR(12) NOT NULL,
        LEAD_TM VARCHAR(5),
        FCST_PROD_CD VARCHAR(2),
        FCST_SRAD DECIMAL(10,6),
        FCST_TEMP DECIMAL(10,6),
        FCST_HUMI DECIMAL(10,6),
        FCST_WSPD DECIMAL(10,6),
        FCST_PSFC DECIMAL(10,6),
        REG_DATE TIMESTAMP DEFAULT SYSDATE,
        UPD_DATE TIMESTAMP DEFAULT SYSDATE,
        PRIMARY KEY (PWR_EXC_TP_CD, AREA_GRP_CD, AREA_GRP_ID, CRTN_TM, FCST_TM)
    );
    """
    
    # REP_DATA_RE_KPX_JEJU_SUKUB_M 테이블 생성
    create_table_kpx_sql = """
    CREATE TABLE IF NOT EXISTS REP_DATA_RE_KPX_JEJU_SUKUB_M (
        TM VARCHAR(12) NOT NULL PRIMARY KEY,
        SUPP_ABILITY DECIMAL(18,5),
        CURR_PWR_TOT DECIMAL(18,5),
        RENEW_PWR_TOT DECIMAL(18,5),
        RENEW_PWR_SOLAR DECIMAL(18,5),
        RENEW_PWR_WIND DECIMAL(18,5),
        REG_DATE TIMESTAMP DEFAULT SYSDATE,
        UPD_DATE TIMESTAMP DEFAULT SYSDATE
    );
    """
    
    # REP_DATA_P2H_FCST_CURT_DA 테이블 생성
    create_table_curt_sql = """
    CREATE TABLE IF NOT EXISTS REP_DATA_P2H_FCST_CURT_DA (
        CRTN_TM VARCHAR(12) NOT NULL,
        FCST_TM VARCHAR(12) NOT NULL,
        LEAD_TM VARCHAR(5) NOT NULL,
        FCST_MINPW DECIMAL(7,2),
        FCST_CURT DECIMAL(7,2),
        REG_DATE TIMESTAMP DEFAULT SYSDATE,
        UPD_DATE TIMESTAMP DEFAULT SYSDATE,
        PRIMARY KEY (CRTN_TM, FCST_TM)
    );
    """
    
    # REP_DATA_HG_FCST_GEN_GENT_DA 테이블 생성
    create_table_hg_gen_sql = """
    CREATE TABLE IF NOT EXISTS REP_DATA_HG_FCST_GEN_GENT_DA (
        AREA_GRP_CD VARCHAR(20) NOT NULL,
        AREA_GRP_ID VARCHAR(20) NOT NULL,
        CRTN_TM VARCHAR(12) NOT NULL,
        FCST_TM VARCHAR(12) NOT NULL,
        LEAD_TM VARCHAR(5),
        FCST_PROD_CD VARCHAR(2),
        FCST_QGEN DECIMAL(13,6),
        FCST_CAPA DECIMAL(13,6),
        REG_DATE TIMESTAMP DEFAULT SYSDATE,
        UPD_DATE TIMESTAMP DEFAULT SYSDATE,
        PRIMARY KEY (AREA_GRP_CD, AREA_GRP_ID, CRTN_TM, FCST_TM)
    );
    """
    
    # REP_DATA_HG_MEAS_GEM_GENT_DA 테이블 생성
    create_table_hg_meas_sql = """
    CREATE TABLE IF NOT EXISTS REP_DATA_HG_MEAS_GEM_GENT_DA (
        TM VARCHAR(12) NOT NULL,
        AREA_GRP_CD VARCHAR(20) NOT NULL,
        AREA_GRP_ID VARCHAR(20) NOT NULL,
        HGEN_PROD DECIMAL(18,5),
        HGEN_CAPA DECIMAL(18,5),
        REG_DATE TIMESTAMP DEFAULT SYSDATE,
        UPD_DATE TIMESTAMP DEFAULT SYSDATE,
        PRIMARY KEY (TM, AREA_GRP_CD, AREA_GRP_ID)
    );
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_lfd_sql)
        cursor.execute(create_table_gen_sql)
        cursor.execute(create_table_nwp_sql)
        cursor.execute(create_table_kpx_sql)
        cursor.execute(create_table_curt_sql)
        cursor.execute(create_table_hg_gen_sql)
        cursor.execute(create_table_hg_meas_sql)
        connection.commit()
        cursor.close()
        print("일곱 테이블이 성공적으로 생성되었습니다.")
    except psycopg2.Error as e:
        print(f"테이블 생성 오류: {e}")

def truncate_all_tables(connection):
    """
    모든 테이블의 데이터를 삭제하는 함수
    """
    if connection is None:
        print("❌ 데이터베이스 연결이 없어서 테이블 데이터를 삭제할 수 없습니다.")
        return False
    
    try:
        cursor = connection.cursor()
        
        # 모든 테이블 목록
        tables = [
            'REP_DATA_RE_FCST_LFD_DA',
            'REP_DATA_RE_FCST_GEN_DA', 
            'REP_DATA_HG_FCST_NWP_DA',
            'REP_DATA_RE_KPX_JEJU_SUKUB_M',
            'REP_DATA_P2H_FCST_CURT_DA',
            'REP_DATA_HG_FCST_GEN_GENT_DA',
            'REP_DATA_HG_MEAS_GEM_GENT_DA'
        ]
        
        print(f"\n{'='*60}")
        print("테이블 데이터 삭제 시작")
        print(f"{'='*60}")
        
        for table in tables:
            try:
                cursor.execute(f"TRUNCATE TABLE {table}")
                print(f"✅ {table} 테이블 데이터 삭제 완료")
            except psycopg2.Error as e:
                print(f"⚠️ {table} 테이블 데이터 삭제 실패: {e}")
        
        connection.commit()
        print(f"\n✅ 모든 테이블 데이터 삭제가 완료되었습니다! - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return True
        
    except psycopg2.Error as e:
        print(f"❌ 테이블 데이터 삭제 중 오류 발생: {e}")
        connection.rollback()
        return False
    finally:
        if cursor:
            cursor.close()

def insert_data_to_postgresql(test_cases_lfd, test_cases_gen, test_cases_nwp, test_cases_kpx, test_cases_curt, test_cases_hg_gen, test_cases_hg_meas):
    """
    PostgreSQL에 데이터 삽입 (일곱 테이블)
    """
    connection = get_db_connection()
    if not connection:
        print("데이터베이스 연결에 실패했습니다. 데이터 생성만 진행합니다.")
        return False
    
    try:
        # 테이블 생성 확인
        create_table_if_not_exists(connection)
        
        cursor = connection.cursor()
        
        # REP_DATA_RE_FCST_LFD_DA 테이블 데이터 삽입
        insert_lfd_sql = """
        INSERT INTO REP_DATA_RE_FCST_LFD_DA (
            CRTN_TM, FCST_TM, LEAD_TM, FCST_PROD_CD,
            FCST_QG01, FCST_QG02, FCST_QG03, FCST_QG04, FCST_QG05, FCST_QG06,
            FCST_QGEN, FCST_QGMX, FCST_QGMN, REG_DATE, UPD_DATE
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # REP_DATA_RE_FCST_GEN_DA 테이블 데이터 삽입
        insert_gen_sql = """
        INSERT INTO REP_DATA_RE_FCST_GEN_DA (
            PWR_EXC_TP_CD, FUEL_TP_CD, CRTN_TM, FCST_TM, LEAD_TM, FCST_PROD_CD,
            FCST_QG01, FCST_QG02, FCST_QG03, FCST_QG04, FCST_QG05, FCST_QG06,
            FCST_QGEN, FCST_QGMX, FCST_QGMN, FCST_CAPA, ESS_CHRG, ESS_DISC, ESS_CAPA,
            REG_DATE, UPD_DATE
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # REP_DATA_HG_FCST_NWP_DA 테이블 데이터 삽입
        insert_nwp_sql = """
        INSERT INTO REP_DATA_HG_FCST_NWP_DA (
            PWR_EXC_TP_CD, AREA_GRP_CD, AREA_GRP_ID, CRTN_TM, FCST_TM, LEAD_TM, FCST_PROD_CD,
            FCST_SRAD, FCST_TEMP, FCST_HUMI, FCST_WSPD, FCST_PSFC,
            REG_DATE, UPD_DATE
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # REP_DATA_RE_KPX_JEJU_SUKUB_M 테이블 데이터 삽입
        insert_kpx_sql = """
        INSERT INTO REP_DATA_RE_KPX_JEJU_SUKUB_M (
            TM, SUPP_ABILITY, CURR_PWR_TOT, RENEW_PWR_TOT, RENEW_PWR_SOLAR, RENEW_PWR_WIND,
            REG_DATE, UPD_DATE
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # REP_DATA_P2H_FCST_CURT_DA 테이블 데이터 삽입
        # On Conflict 추가
        insert_curt_sql = """
        INSERT INTO REP_DATA_P2H_FCST_CURT_DA (
            CRTN_TM, FCST_TM, LEAD_TM, FCST_MINPW, FCST_CURT,
            REG_DATE, UPD_DATE
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (CRTN_TM, FCST_TM)
        DO UPDATE SET 
            LEAD_TM = EXCLUDED.LEAD_TM,
            FCST_MINPW = EXCLUDED.FCST_MINPW,
            FCST_CURT = EXCLUDED.FCST_CURT,
            REG_DATE = EXCLUDED.REG_DATE,
            UPD_DATE = EXCLUDED.UPD_DATE
        """
        
        # REP_DATA_HG_FCST_GEN_GENT_DA 테이블 데이터 삽입
        insert_hg_gen_sql = """
        INSERT INTO REP_DATA_HG_FCST_GEN_GENT_DA (
            AREA_GRP_CD, AREA_GRP_ID, CRTN_TM, FCST_TM, LEAD_TM, FCST_PROD_CD,
            FCST_QGEN, FCST_CAPA, REG_DATE, UPD_DATE
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # REP_DATA_HG_MEAS_GEM_GENT_DA 테이블 데이터 삽입
        insert_hg_meas_sql = """
        INSERT INTO REP_DATA_HG_MEAS_GEM_GENT_DA (
            TM, AREA_GRP_CD, AREA_GRP_ID, HGEN_PROD, HGEN_CAPA,
            REG_DATE, UPD_DATE
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # LFD 테이블 데이터 삽입
        inserted_lfd_count = 0
        for i, case in enumerate(test_cases_lfd, 1):
            try:
                cursor.execute(insert_lfd_sql, (
                    case['CRTN_TM'],
                    case['FCST_TM'],
                    case['LEAD_TM'],
                    case['FCST_PROD_CD'],
                    case['FCST_QG01'],
                    case['FCST_QG02'],
                    case['FCST_QG03'],
                    case['FCST_QG04'],
                    case['FCST_QG05'],
                    case['FCST_QG06'],
                    case['FCST_QGEN'],
                    case['FCST_QGMX'],
                    case['FCST_QGMN'],
                    case['REG_DATE'],
                    case['UPD_DATE']
                ))
                inserted_lfd_count += 1
                # print(f"LFD 데이터 {i} 삽입 완료")
                
            except psycopg2.Error as e:
                print(f"LFD 데이터 {i} 삽입 실패: {e}")
                connection.rollback()
                continue
        
        # GEN 테이블 데이터 삽입
        inserted_gen_count = 0
        for i, case in enumerate(test_cases_gen, 1):
            try:
                cursor.execute(insert_gen_sql, (
                    case['PWR_EXC_TP_CD'],
                    case['FUEL_TP_CD'],
                    case['CRTN_TM'],
                    case['FCST_TM'],
                    case['LEAD_TM'],
                    case['FCST_PROD_CD'],
                    case['FCST_QG01'],
                    case['FCST_QG02'],
                    case['FCST_QG03'],
                    case['FCST_QG04'],
                    case['FCST_QG05'],
                    case['FCST_QG06'],
                    case['FCST_QGEN'],
                    case['FCST_QGMX'],
                    case['FCST_QGMN'],
                    case['FCST_CAPA'],
                    case['ESS_CHRG'],
                    case['ESS_DISC'],
                    case['ESS_CAPA'],
                    case['REG_DATE'],
                    case['UPD_DATE']
                ))
                inserted_gen_count += 1
                # print(f"GEN 데이터 {i} 삽입 완료")
                
            except psycopg2.Error as e:
                print(f"GEN 데이터 {i} 삽입 실패: {e}")
                connection.rollback()
                continue
        
        # NWP 테이블 데이터 삽입
        inserted_nwp_count = 0
        for i, case in enumerate(test_cases_nwp, 1):
            try:
                cursor.execute(insert_nwp_sql, (
                    case['PWR_EXC_TP_CD'],
                    case['AREA_GRP_CD'],
                    case['AREA_GRP_ID'],
                    case['CRTN_TM'],
                    case['FCST_TM'],
                    case['LEAD_TM'],
                    case['FCST_PROD_CD'],
                    case['FCST_SRAD'],
                    case['FCST_TEMP'],
                    case['FCST_HUMI'],
                    case['FCST_WSPD'],
                    case['FCST_PSFC'],
                    case['REG_DATE'],
                    case['UPD_DATE']
                ))
                inserted_nwp_count += 1
                # print(f"NWP 데이터 {i} 삽입 완료")
                
            except psycopg2.Error as e:
                print(f"NWP 데이터 {i} 삽입 실패: {e}")
                connection.rollback()
                continue
        
        # KPX 테이블 데이터 삽입
        inserted_kpx_count = 0
        for i, case in enumerate(test_cases_kpx, 1):
            try:
                cursor.execute(insert_kpx_sql, (
                    case['TM'],
                    case['SUPP_ABILITY'],
                    case['CURR_PWR_TOT'],
                    case['RENEW_PWR_TOT'],
                    case['RENEW_PWR_SOLAR'],
                    case['RENEW_PWR_WIND'],
                    case['REG_DATE'],
                    case['UPD_DATE']
                ))
                inserted_kpx_count += 1
                # print(f"KPX 데이터 {i} 삽입 완료")
                
            except psycopg2.Error as e:
                print(f"KPX 데이터 {i} 삽입 실패: {e}")
                connection.rollback()
                continue
        
        # CURT 테이블 데이터 삽입
        inserted_curt_count = 0
        for i, case in enumerate(test_cases_curt, 1):
            try:
                cursor.execute(insert_curt_sql, (
                    case['CRTN_TM'],
                    case['FCST_TM'],
                    case['LEAD_TM'],
                    case['FCST_MINPW'],
                    case['FCST_CURT'],
                    case['REG_DATE'],
                    case['UPD_DATE']
                ))
                inserted_curt_count += 1
                # print(f"CURT 데이터 {i} 삽입 완료")
                
            except psycopg2.Error as e:
                print(f"CURT 데이터 {i} 삽입 실패: {e}")
                connection.rollback()
                continue
        
        # HG_GEN 테이블 데이터 삽입
        inserted_hg_gen_count = 0
        for i, case in enumerate(test_cases_hg_gen, 1):
            try:
                cursor.execute(insert_hg_gen_sql, (
                    case['AREA_GRP_CD'],
                    case['AREA_GRP_ID'],
                    case['CRTN_TM'],
                    case['FCST_TM'],
                    case['LEAD_TM'],
                    case['FCST_PROD_CD'],
                    case['FCST_QGEN'],
                    case['FCST_CAPA'],
                    case['REG_DATE'],
                    case['UPD_DATE']
                ))
                inserted_hg_gen_count += 1
                # print(f"HG_GEN 데이터 {i} 삽입 완료")
                
            except psycopg2.Error as e:
                print(f"HG_GEN 데이터 {i} 삽입 실패: {e}")
                connection.rollback()
                continue
        
        # HG_MEAS 테이블 데이터 삽입
        inserted_hg_meas_count = 0
        for i, case in enumerate(test_cases_hg_meas, 1):
            try:
                cursor.execute(insert_hg_meas_sql, (
                    case['TM'],
                    case['AREA_GRP_CD'],
                    case['AREA_GRP_ID'],
                    case['HGEN_PROD'],
                    case['HGEN_CAPA'],
                    case['REG_DATE'],
                    case['UPD_DATE']
                ))
                inserted_hg_meas_count += 1
                # print(f"HG_MEAS 데이터 {i} 삽입 완료")
                
            except psycopg2.Error as e:
                print(f"HG_MEAS 데이터 {i} 삽입 실패: {e}")
                connection.rollback()
                continue
        
        connection.commit()
        cursor.close()
        connection.close()
        
        print(f"\n총 {inserted_lfd_count}개의 LFD 데이터, {inserted_gen_count}개의 GEN 데이터, {inserted_nwp_count}개의 NWP 데이터, {inserted_kpx_count}개의 KPX 데이터, {inserted_curt_count}개의 CURT 데이터, {inserted_hg_gen_count}개의 HG_GEN 데이터, {inserted_hg_meas_count}개의 HG_MEAS 데이터가 PostgreSQL에 성공적으로 삽입되었습니다.")
        return True
        
    except psycopg2.Error as e:
        print(f"데이터 삽입 중 오류 발생: {e}")
        if connection:
            connection.rollback()
            connection.close()
        return False

def generate_random_test_cases(num_cases=10, next_day=False):
    """
    수요예측 데이터 테스트케이스 생성 함수
    00시부터 23시까지 한 시간 간격으로 데이터 생성 (24시간 운영)
    
    Args:
        num_cases (int): 생성할 테스트 케이스 수 (기본값: 10)
        next_day (bool): True이면 다음날 데이터 생성, False이면 오늘 데이터 생성
    """
    test_cases_lfd = []  # REP_DATA_RE_FCST_LFD_DA용
    test_cases_gen = []  # REP_DATA_RE_FCST_GEN_DA용
    test_cases_nwp = []  # REP_DATA_HG_FCST_NWP_DA용
    test_cases_kpx = []  # REP_DATA_RE_KPX_JEJU_SUKUB_M용
    test_cases_curt = [] # REP_DATA_P2H_FCST_CURT_DA용
    test_cases_hg_gen = [] # REP_DATA_HG_FCST_GEN_GENT_DA용
    test_cases_hg_meas = [] # REP_DATA_HG_MEAS_GEM_GENT_DA용
    
    # 날짜 기준으로 00시부터 23시까지 (24시간 운영)
    base_date = datetime.datetime.now()
    if next_day:
        base_date = base_date + datetime.timedelta(days=1)
    today = base_date.replace(hour=0, minute=0, second=0, microsecond=0)
    start_hour = 0
    end_hour = 23
    
    # 신재생 연료 구분 코드들
    fuel_types = ['SOLAR', 'WIND', 'HYDRO', 'BIOMASS', 'GEOTHERMAL']
    
    # 영역 그룹 코드들 (수소 생산단지)
    area_groups = ['SEOUL', 'BUSAN', 'DAEGU', 'INCHON', 'GWANGJU', 'DAEJEON', 'ULSAN', 'SEJONG']
    
    for hour in range(start_hour, end_hour + 1):
        # 생성시간을 해당 시간으로 설정
        crtn_time = today.replace(hour=hour, minute=0, second=0, microsecond=0)
        crtn_tm = crtn_time.strftime("%Y%m%d%H%M")
        
        # 예측시간은 생성시간에서 1시간 후로 설정
        fcst_time = crtn_time + datetime.timedelta(hours=1)
        fcst_tm = fcst_time.strftime("%Y%m%d%H%M")
        
        # 선행시간 (HHHMI 형식)
        lead_hours = random.randint(1, 24)
        lead_minutes = random.randint(0, 59)
        lead_tm = f"{lead_hours:03d}{lead_minutes:02d}"
        
        # 예측생산구분 (2자리 코드)
        fcst_prod_cd = f"{random.randint(1, 99):02d}"
        
        # 시간대별로 다른 기본 수요량 설정 (24시간 운영)
        if 0 <= hour <= 5:  # 새벽
            base_demand = random.uniform(30000, 45000)
        elif 6 <= hour <= 9:  # 오전 피크
            base_demand = random.uniform(60000, 80000)
        elif 10 <= hour <= 16:  # 주간
            base_demand = random.uniform(50000, 70000)
        elif 17 <= hour <= 20:  # 오후 피크
            base_demand = random.uniform(65000, 85000)
        elif 21 <= hour <= 23:  # 저녁
            base_demand = random.uniform(40000, 60000)
        else:  # 기타 시간
            base_demand = random.uniform(35000, 55000)
        
        # 개별 예측량들 (약간의 변동성 추가)
        fcst_qg01 = round(base_demand * random.uniform(0.95, 1.05), 6)
        fcst_qg02 = round(base_demand * random.uniform(0.94, 1.06), 6)
        fcst_qg03 = round(base_demand * random.uniform(0.93, 1.07), 6)
        fcst_qg04 = round(base_demand * random.uniform(0.92, 1.08), 6)
        fcst_qg05 = round(base_demand * random.uniform(0.91, 1.09), 6)
        fcst_qg06 = round(base_demand * random.uniform(0.90, 1.10), 6)
        
        # 최종 수요예측량 (개별 예측량들의 평균)
        fcst_qgen = round((fcst_qg01 + fcst_qg02 + fcst_qg03 + fcst_qg04 + fcst_qg05 + fcst_qg06) / 6, 6)
        
        # 최대값과 최소값
        all_values = [fcst_qg01, fcst_qg02, fcst_qg03, fcst_qg04, fcst_qg05, fcst_qg06]
        fcst_qgmx = round(max(all_values), 6)
        fcst_qgmn = round(min(all_values), 6)
        
        # 등록일시와 수정일시
        reg_date = crtn_time.strftime("%Y-%m-%d %H:%M:%S")
        upd_date = crtn_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # REP_DATA_RE_FCST_LFD_DA용 데이터
        test_case_lfd = {
            'CRTN_TM': crtn_tm,
            'FCST_TM': fcst_tm,
            'LEAD_TM': lead_tm,
            'FCST_PROD_CD': fcst_prod_cd,
            'FCST_QG01': fcst_qg01,
            'FCST_QG02': fcst_qg02,
            'FCST_QG03': fcst_qg03,
            'FCST_QG04': fcst_qg04,
            'FCST_QG05': fcst_qg05,
            'FCST_QG06': fcst_qg06,
            'FCST_QGEN': fcst_qgen,
            'FCST_QGMX': fcst_qgmx,
            'FCST_QGMN': fcst_qgmn,
            'REG_DATE': reg_date,
            'UPD_DATE': upd_date
        }
        
        test_cases_lfd.append(test_case_lfd)
        
        # REP_DATA_RE_FCST_GEN_DA용 데이터 (각 연료 타입별로)
        renewable_totals = {'SOLAR': 0, 'WIND': 0, 'HYDRO': 0, 'BIOMASS': 0, 'GEOTHERMAL': 0}
        
        for fuel_type in fuel_types:
            # 신재생 발전량은 일반 수요량보다 작음
            renewable_base = base_demand * random.uniform(0.1, 0.3)  # 10-30% 수준
            
            # 신재생 발전량들
            renewable_qg01 = round(renewable_base * random.uniform(0.95, 1.05), 6)
            renewable_qg02 = round(renewable_base * random.uniform(0.94, 1.06), 6)
            renewable_qg03 = round(renewable_base * random.uniform(0.93, 1.07), 6)
            renewable_qg04 = round(renewable_base * random.uniform(0.92, 1.08), 6)
            renewable_qg05 = round(renewable_base * random.uniform(0.91, 1.09), 6)
            renewable_qg06 = round(renewable_base * random.uniform(0.90, 1.10), 6)
            
            # 최종 신재생 발전량
            renewable_qgen = round((renewable_qg01 + renewable_qg02 + renewable_qg03 + renewable_qg04 + renewable_qg05 + renewable_qg06) / 6, 6)
            
            # 최대값과 최소값
            renewable_all_values = [renewable_qg01, renewable_qg02, renewable_qg03, renewable_qg04, renewable_qg05, renewable_qg06]
            renewable_qgmx = round(max(renewable_all_values), 6)
            renewable_qgmn = round(min(renewable_all_values), 6)
            
            # ESS 관련 데이터
            fcst_capa = round(renewable_base * random.uniform(0.8, 1.2), 6)  # 설비용량
            ess_chrg = round(renewable_base * random.uniform(0.05, 0.15), 6)  # ESS 충전
            ess_disc = round(renewable_base * random.uniform(0.05, 0.15), 6)  # ESS 방전
            ess_capa = round(renewable_base * random.uniform(0.1, 0.3), 6)    # ESS 용량
            
            # 합계 계산용
            renewable_totals[fuel_type] = renewable_qgen
            
            test_case_gen = {
                'PWR_EXC_TP_CD': f"{random.randint(1, 99):02d}",
                'FUEL_TP_CD': fuel_type,
                'CRTN_TM': crtn_tm,
                'FCST_TM': fcst_tm,
                'LEAD_TM': lead_tm,
                'FCST_PROD_CD': fcst_prod_cd,
                'FCST_QG01': renewable_qg01,
                'FCST_QG02': renewable_qg02,
                'FCST_QG03': renewable_qg03,
                'FCST_QG04': renewable_qg04,
                'FCST_QG05': renewable_qg05,
                'FCST_QG06': renewable_qg06,
                'FCST_QGEN': renewable_qgen,
                'FCST_QGMX': renewable_qgmx,
                'FCST_QGMN': renewable_qgmn,
                'FCST_CAPA': fcst_capa,
                'ESS_CHRG': ess_chrg,
                'ESS_DISC': ess_disc,
                'ESS_CAPA': ess_capa,
                'REG_DATE': reg_date,
                'UPD_DATE': upd_date
            }
            
            test_cases_gen.append(test_case_gen)
        
        # REP_DATA_HG_FCST_NWP_DA용 데이터 (시간당 하나씩만 생성)
        # 기상 예측 데이터 생성
        # 일사량 (W/m²) - 시간대별로 다른 값
        if 0 <= hour <= 5:  # 새벽
            fcst_srad = round(random.uniform(0, 50), 6)  # 0-50 W/m²
        elif 6 <= hour <= 9:  # 오전
            fcst_srad = round(random.uniform(200, 600), 6)  # 200-600 W/m²
        elif 10 <= hour <= 16:  # 주간
            fcst_srad = round(random.uniform(600, 1000), 6)  # 600-1000 W/m²
        elif 17 <= hour <= 20:  # 오후
            fcst_srad = round(random.uniform(300, 700), 6)  # 300-700 W/m²
        elif 21 <= hour <= 23:  # 저녁
            fcst_srad = round(random.uniform(0, 200), 6)  # 0-200 W/m²
        else:  # 기타 시간
            fcst_srad = round(random.uniform(0, 100), 6)  # 0-100 W/m²
        
        # 기온 (°C) - 시간대별로 다른 값
        if 0 <= hour <= 5:  # 새벽
            fcst_temp = round(random.uniform(10, 18), 6)
        elif 6 <= hour <= 9:  # 오전
            fcst_temp = round(random.uniform(15, 25), 6)
        elif 10 <= hour <= 16:  # 주간
            fcst_temp = round(random.uniform(20, 30), 6)
        elif 17 <= hour <= 20:  # 오후
            fcst_temp = round(random.uniform(18, 28), 6)
        elif 21 <= hour <= 23:  # 저녁
            fcst_temp = round(random.uniform(15, 22), 6)
        else:  # 기타 시간
            fcst_temp = round(random.uniform(12, 20), 6)
        
        # 습도 (%) - 시간대별로 다른 값
        if 0 <= hour <= 5:  # 새벽
            fcst_humi = round(random.uniform(70, 90), 6)
        elif 6 <= hour <= 9:  # 오전
            fcst_humi = round(random.uniform(60, 80), 6)
        elif 10 <= hour <= 16:  # 주간
            fcst_humi = round(random.uniform(40, 60), 6)
        elif 17 <= hour <= 20:  # 오후
            fcst_humi = round(random.uniform(50, 70), 6)
        elif 21 <= hour <= 23:  # 저녁
            fcst_humi = round(random.uniform(60, 80), 6)
        else:  # 기타 시간
            fcst_humi = round(random.uniform(65, 85), 6)
        
        # 풍속 (m/s) - 시간대별로 다른 값
        if 0 <= hour <= 5:  # 새벽
            fcst_wspd = round(random.uniform(1, 3), 6)
        elif 6 <= hour <= 9:  # 오전
            fcst_wspd = round(random.uniform(2, 5), 6)
        elif 10 <= hour <= 16:  # 주간
            fcst_wspd = round(random.uniform(3, 7), 6)
        elif 17 <= hour <= 20:  # 오후
            fcst_wspd = round(random.uniform(2, 6), 6)
        elif 21 <= hour <= 23:  # 저녁
            fcst_wspd = round(random.uniform(1, 4), 6)
        else:  # 기타 시간
            fcst_wspd = round(random.uniform(1, 3), 6)
        
        # 기압 (hPa) - 상대적으로 안정적
        fcst_psfc = round(random.uniform(1010, 1020), 6)
        
        test_case_nwp = {
            'PWR_EXC_TP_CD': '9',  # 무조건 9
            'AREA_GRP_CD': '1',    # 무조건 1
            'AREA_GRP_ID': '1',    # 무조건 1
            'CRTN_TM': crtn_tm,
            'FCST_TM': fcst_tm,
            'LEAD_TM': lead_tm,
            'FCST_PROD_CD': fcst_prod_cd,
            'FCST_SRAD': fcst_srad,
            'FCST_TEMP': fcst_temp,
            'FCST_HUMI': fcst_humi,
            'FCST_WSPD': fcst_wspd,
            'FCST_PSFC': fcst_psfc,
            'REG_DATE': reg_date,
            'UPD_DATE': upd_date
        }
        
        test_cases_nwp.append(test_case_nwp)
        
        # REP_DATA_RE_KPX_JEJU_SUKUB_M용 데이터 (제주 계통 운영 정보)
        # 기준일시 (YYYYMMDDHHMI 형식)
        tm = crtn_time.strftime("%Y%m%d%H%M")
        
        # 공급능력 (MW) - 현재 수요보다 약간 높게
        supp_ability = round(base_demand * random.uniform(1.1, 1.3), 5)
        
        # 현재수요 (MW) - 기존 수요예측량과 유사
        curr_pwr_tot = round(base_demand * random.uniform(0.95, 1.05), 5)
        
        # 신재생합계 (MW) - 모든 신재생 발전량의 합
        renew_pwr_tot = round(sum(renewable_totals.values()), 5)
        
        # 태양광합계 (MW) - SOLAR 발전량
        renew_pwr_solar = round(renewable_totals['SOLAR'], 5)
        
        # 풍력합계 (MW) - WIND 발전량
        renew_pwr_wind = round(renewable_totals['WIND'], 5)
        
        test_case_kpx = {
            'TM': tm,
            'SUPP_ABILITY': supp_ability,
            'CURR_PWR_TOT': curr_pwr_tot,
            'RENEW_PWR_TOT': renew_pwr_tot,
            'RENEW_PWR_SOLAR': renew_pwr_solar,
            'RENEW_PWR_WIND': renew_pwr_wind,
            'REG_DATE': reg_date,
            'UPD_DATE': upd_date
        }
        
        test_cases_kpx.append(test_case_kpx)

        # REP_DATA_P2H_FCST_CURT_DA용 데이터 (각 시간대별로)
        for i in range(24): # 00시부터 23시까지
            crtn_tm_curt_dt = crtn_time.replace(hour=i, minute=0, second=0, microsecond=0)
            crtn_tm_curt = crtn_tm_curt_dt.strftime("%Y%m%d%H%M")
            fcst_tm_curt = (crtn_tm_curt_dt + datetime.timedelta(hours=1)).strftime("%Y%m%d%H%M")
            lead_tm_curt = f"{i:03d}00" # 선행시간은 시간대와 동일
            fcst_minpw = round(base_demand * random.uniform(0.8, 1.2), 2) # 예측 최소 수요
            fcst_curt = round(base_demand * random.uniform(0.9, 1.1), 2) # 예측 최대 수요

            test_case_curt = {
                'CRTN_TM': crtn_tm_curt,
                'FCST_TM': fcst_tm_curt,
                'LEAD_TM': lead_tm_curt,
                'FCST_MINPW': fcst_minpw,
                'FCST_CURT': fcst_curt,
                'REG_DATE': reg_date,
                'UPD_DATE': upd_date
            }
            test_cases_curt.append(test_case_curt)
        
        # REP_DATA_HG_FCST_GEN_GENT_DA용 데이터 (수소발전단지 수소 예측 생산량)
        for area_group in area_groups:
            for fuel_type in ['HYDROGEN']:  # 수소 발전만
                # 수소 생산량 (MWh) - 시간대별로 다른 값
                if 6 <= hour <= 18:  # 주간 (수소 생산 활발)
                    fcst_qgen = round(random.uniform(50, 200), 6)
                    fcst_capa = round(random.uniform(100, 300), 6)
                else:  # 야간 (수소 생산 감소)
                    fcst_qgen = round(random.uniform(20, 80), 6)
                    fcst_capa = round(random.uniform(50, 150), 6)
                
                test_case_hg_gen = {
                    'AREA_GRP_CD': area_group,
                    'AREA_GRP_ID': f"{area_group}_H2_{random.randint(1, 999):03d}",
                    'CRTN_TM': crtn_tm,
                    'FCST_TM': fcst_tm,
                    'LEAD_TM': lead_tm,
                    'FCST_PROD_CD': fcst_prod_cd,
                    'FCST_QGEN': fcst_qgen,
                    'FCST_CAPA': fcst_capa,
                    'REG_DATE': reg_date,
                    'UPD_DATE': upd_date
                }
                test_cases_hg_gen.append(test_case_hg_gen)
        
        # REP_DATA_HG_MEAS_GEM_GENT_DA용 데이터 (수소발전단지 수소 생산량 정보)
        for area_group in area_groups:
            # 수소 생산량 (KG) - 시간대별로 다른 값
            if 6 <= hour <= 18:  # 주간 (수소 생산 활발)
                hgen_prod = round(random.uniform(1000, 5000), 5)
                hgen_capa = round(random.uniform(2000, 8000), 5)
            else:  # 야간 (수소 생산 감소)
                hgen_prod = round(random.uniform(500, 2000), 5)
                hgen_capa = round(random.uniform(1000, 4000), 5)
            
            test_case_hg_meas = {
                'TM': tm,
                'AREA_GRP_CD': area_group,
                'AREA_GRP_ID': f"{area_group}_H2_{random.randint(1, 999):03d}",
                'HGEN_PROD': hgen_prod,
                'HGEN_CAPA': hgen_capa,
                'REG_DATE': reg_date,
                'UPD_DATE': upd_date
            }
            test_cases_hg_meas.append(test_case_hg_meas)
    
    return test_cases_lfd, test_cases_gen, test_cases_nwp, test_cases_kpx, test_cases_curt, test_cases_hg_gen, test_cases_hg_meas

def print_test_cases(test_cases_lfd, test_cases_gen, test_cases_nwp, test_cases_kpx, test_cases_curt, test_cases_hg_gen, test_cases_hg_meas):
    """
    테스트케이스를 보기 좋게 출력
    """
    print("=" * 120)
    print("수요예측 데이터 테스트케이스 (REP_DATA_RE_FCST_LFD_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_lfd, 1):
        print(f"\n[LFD 테스트케이스 {i}]")
        print("-" * 80)
        
        for key, value in case.items():
            if key in ['FCST_QG01', 'FCST_QG02', 'FCST_QG03', 'FCST_QG04', 'FCST_QG05', 'FCST_QG06', 'FCST_QGEN', 'FCST_QGMX', 'FCST_QGMN']:
                print(f"{key:12} : {value:>15.6f} MWh")
            else:
                print(f"{key:12} : {value}")
    
    print("\n" + "=" * 120)
    print("신재생 예측 발전량 데이터 테스트케이스 (REP_DATA_RE_FCST_GEN_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_gen, 1):
        print(f"\n[GEN 테스트케이스 {i}]")
        print("-" * 80)
        
        for key, value in case.items():
            if key in ['FCST_QG01', 'FCST_QG02', 'FCST_QG03', 'FCST_QG04', 'FCST_QG05', 'FCST_QG06', 'FCST_QGEN', 'FCST_QGMX', 'FCST_QGMN', 'FCST_CAPA', 'ESS_CHRG', 'ESS_DISC', 'ESS_CAPA']:
                print(f"{key:12} : {value:>15.6f} MWh")
            else:
                print(f"{key:12} : {value}")
    
    print("\n" + "=" * 120)
    print("수소 생산단지 기상 예측 데이터 테스트케이스 (REP_DATA_HG_FCST_NWP_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_nwp, 1):
        print(f"\n[NWP 테스트케이스 {i}]")
        print("-" * 80)
        
        for key, value in case.items():
            if key in ['FCST_SRAD']:
                print(f"{key:12} : {value:>15.6f} W/m²")
            elif key in ['FCST_TEMP']:
                print(f"{key:12} : {value:>15.6f} °C")
            elif key in ['FCST_HUMI']:
                print(f"{key:12} : {value:>15.6f} %")
            elif key in ['FCST_WSPD']:
                print(f"{key:12} : {value:>15.6f} m/s")
            elif key in ['FCST_PSFC']:
                print(f"{key:12} : {value:>15.6f} hPa")
            else:
                print(f"{key:12} : {value}")
    
    print("\n" + "=" * 120)
    print("제주 계통 운영 정보 데이터 테스트케이스 (REP_DATA_RE_KPX_JEJU_SUKUB_M)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_kpx, 1):
        print(f"\n[KPX 테스트케이스 {i}]")
        print("-" * 80)
        
        for key, value in case.items():
            if key in ['SUPP_ABILITY', 'CURR_PWR_TOT', 'RENEW_PWR_TOT', 'RENEW_PWR_SOLAR', 'RENEW_PWR_WIND']:
                print(f"{key:12} : {value:>15.5f} MW")
            else:
                print(f"{key:12} : {value}")
    
    print("\n" + "=" * 120)
    print("제주전체 예측 출력제어량 데이터 테스트케이스 (REP_DATA_P2H_FCST_CURT_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_curt, 1):
        print(f"\n[CURT 테스트케이스 {i}]")
        print("-" * 80)
        
        for key, value in case.items():
            if key in ['FCST_MINPW', 'FCST_CURT']:
                print(f"{key:12} : {value:>15.2f} MW/m²")
            else:
                print(f"{key:12} : {value}")
    
    print("\n" + "=" * 120)
    print("수소발전단지 수소 예측 생산량 데이터 테스트케이스 (REP_DATA_HG_FCST_GEN_GENT_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_hg_gen, 1):
        print(f"\n[HG_GEN 테스트케이스 {i}]")
        print("-" * 80)
        
        for key, value in case.items():
            if key in ['FCST_QGEN', 'FCST_CAPA']:
                print(f"{key:12} : {value:>15.6f} MWh")
            else:
                print(f"{key:12} : {value}")
    
    print("\n" + "=" * 120)
    print("수소발전단지 수소 생산량 정보 데이터 테스트케이스 (REP_DATA_HG_MEAS_GEM_GENT_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_hg_meas, 1):
        print(f"\n[HG_MEAS 테스트케이스 {i}]")
        print("-" * 80)
        
        for key, value in case.items():
            if key in ['HGEN_PROD', 'HGEN_CAPA']:
                print(f"{key:12} : {value:>15.5f} KG")
            else:
                print(f"{key:12} : {value}")

def generate_sql_insert_statements(test_cases_lfd, test_cases_gen, test_cases_nwp, test_cases_kpx, test_cases_curt, test_cases_hg_gen, test_cases_hg_meas):
    """
    SQL INSERT 문 생성
    """
    print("\n" + "=" * 120)
    print("SQL INSERT 문 (REP_DATA_RE_FCST_LFD_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_lfd, 1):
        print(f"\n-- LFD 테스트케이스 {i}")
        print("INSERT INTO REP_DATA_RE_FCST_LFD_DA (")
        print("    CRTN_TM, FCST_TM, LEAD_TM, FCST_PROD_CD,")
        print("    FCST_QG01, FCST_QG02, FCST_QG03, FCST_QG04, FCST_QG05, FCST_QG06,")
        print("    FCST_QGEN, FCST_QGMX, FCST_QGMN, REG_DATE, UPD_DATE")
        print(") VALUES (")
        print(f"    '{case['CRTN_TM']}', '{case['FCST_TM']}', '{case['LEAD_TM']}', '{case['FCST_PROD_CD']}',")
        print(f"    {case['FCST_QG01']}, {case['FCST_QG02']}, {case['FCST_QG03']}, {case['FCST_QG04']}, {case['FCST_QG05']}, {case['FCST_QG06']},")
        print(f"    {case['FCST_QGEN']}, {case['FCST_QGMX']}, {case['FCST_QGMN']}, TO_TIMESTAMP('{case['REG_DATE']}', 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('{case['UPD_DATE']}', 'YYYY-MM-DD HH24:MI:SS')")
        print(");")
    
    print("\n" + "=" * 120)
    print("SQL INSERT 문 (REP_DATA_RE_FCST_GEN_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_gen, 1):
        print(f"\n-- GEN 테스트케이스 {i}")
        print("INSERT INTO REP_DATA_RE_FCST_GEN_DA (")
        print("    PWR_EXC_TP_CD, FUEL_TP_CD, CRTN_TM, FCST_TM, LEAD_TM, FCST_PROD_CD,")
        print("    FCST_QG01, FCST_QG02, FCST_QG03, FCST_QG04, FCST_QG05, FCST_QG06,")
        print("    FCST_QGEN, FCST_QGMX, FCST_QGMN, FCST_CAPA, ESS_CHRG, ESS_DISC, ESS_CAPA,")
        print("    REG_DATE, UPD_DATE")
        print(") VALUES (")
        print(f"    '{case['PWR_EXC_TP_CD']}', '{case['FUEL_TP_CD']}', '{case['CRTN_TM']}', '{case['FCST_TM']}', '{case['LEAD_TM']}', '{case['FCST_PROD_CD']}',")
        print(f"    {case['FCST_QG01']}, {case['FCST_QG02']}, {case['FCST_QG03']}, {case['FCST_QG04']}, {case['FCST_QG05']}, {case['FCST_QG06']},")
        print(f"    {case['FCST_QGEN']}, {case['FCST_QGMX']}, {case['FCST_QGMN']}, {case['FCST_CAPA']}, {case['ESS_CHRG']}, {case['ESS_DISC']}, {case['ESS_CAPA']},")
        print(f"    TO_TIMESTAMP('{case['REG_DATE']}', 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('{case['UPD_DATE']}', 'YYYY-MM-DD HH24:MI:SS')")
        print(");")
    
    print("\n" + "=" * 120)
    print("SQL INSERT 문 (REP_DATA_HG_FCST_NWP_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_nwp, 1):
        print(f"\n-- NWP 테스트케이스 {i}")
        print("INSERT INTO REP_DATA_HG_FCST_NWP_DA (")
        print("    PWR_EXC_TP_CD, AREA_GRP_CD, AREA_GRP_ID, CRTN_TM, FCST_TM, LEAD_TM, FCST_PROD_CD,")
        print("    FCST_SRAD, FCST_TEMP, FCST_HUMI, FCST_WSPD, FCST_PSFC,")
        print("    REG_DATE, UPD_DATE")
        print(") VALUES (")
        print(f"    '{case['PWR_EXC_TP_CD']}', '{case['AREA_GRP_CD']}', '{case['AREA_GRP_ID']}', '{case['CRTN_TM']}', '{case['FCST_TM']}', '{case['LEAD_TM']}', '{case['FCST_PROD_CD']}',")
        print(f"    {case['FCST_SRAD']}, {case['FCST_TEMP']}, {case['FCST_HUMI']}, {case['FCST_WSPD']}, {case['FCST_PSFC']},")
        print(f"    TO_TIMESTAMP('{case['REG_DATE']}', 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('{case['UPD_DATE']}', 'YYYY-MM-DD HH24:MI:SS')")
        print(");")
    
    print("\n" + "=" * 120)
    print("SQL INSERT 문 (REP_DATA_RE_KPX_JEJU_SUKUB_M)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_kpx, 1):
        print(f"\n-- KPX 테스트케이스 {i}")
        print("INSERT INTO REP_DATA_RE_KPX_JEJU_SUKUB_M (")
        print("    TM, SUPP_ABILITY, CURR_PWR_TOT, RENEW_PWR_TOT, RENEW_PWR_SOLAR, RENEW_PWR_WIND,")
        print("    REG_DATE, UPD_DATE")
        print(") VALUES (")
        print(f"    '{case['TM']}', {case['SUPP_ABILITY']}, {case['CURR_PWR_TOT']}, {case['RENEW_PWR_TOT']}, {case['RENEW_PWR_SOLAR']}, {case['RENEW_PWR_WIND']},")
        print(f"    TO_TIMESTAMP('{case['REG_DATE']}', 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('{case['UPD_DATE']}', 'YYYY-MM-DD HH24:MI:SS')")
        print(");")
    
    print("\n" + "=" * 120)
    print("SQL INSERT 문 (REP_DATA_P2H_FCST_CURT_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_curt, 1):
        print(f"\n-- CURT 테스트케이스 {i}")
        print("INSERT INTO REP_DATA_P2H_FCST_CURT_DA (")
        print("    CRTN_TM, FCST_TM, LEAD_TM, FCST_MINPW, FCST_CURT,")
        print("    REG_DATE, UPD_DATE")
        print(") VALUES (")
        print(f"    '{case['CRTN_TM']}', '{case['FCST_TM']}', '{case['LEAD_TM']}', {case['FCST_MINPW']}, {case['FCST_CURT']},")
        print(f"    TO_TIMESTAMP('{case['REG_DATE']}', 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('{case['UPD_DATE']}', 'YYYY-MM-DD HH24:MI:SS')")
        print(");")
    
    print("\n" + "=" * 120)
    print("SQL INSERT 문 (REP_DATA_HG_FCST_GEN_GENT_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_hg_gen, 1):
        print(f"\n-- HG_GEN 테스트케이스 {i}")
        print("INSERT INTO REP_DATA_HG_FCST_GEN_GENT_DA (")
        print("    AREA_GRP_CD, AREA_GRP_ID, CRTN_TM, FCST_TM, LEAD_TM, FCST_PROD_CD,")
        print("    FCST_QGEN, FCST_CAPA, REG_DATE, UPD_DATE")
        print(") VALUES (")
        print(f"    '{case['AREA_GRP_CD']}', '{case['AREA_GRP_ID']}', '{case['CRTN_TM']}', '{case['FCST_TM']}', '{case['LEAD_TM']}', '{case['FCST_PROD_CD']}',")
        print(f"    {case['FCST_QGEN']}, {case['FCST_CAPA']},")
        print(f"    TO_TIMESTAMP('{case['REG_DATE']}', 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('{case['UPD_DATE']}', 'YYYY-MM-DD HH24:MI:SS')")
        print(");")
    
    print("\n" + "=" * 120)
    print("SQL INSERT 문 (REP_DATA_HG_MEAS_GEM_GENT_DA)")
    print("=" * 120)
    
    for i, case in enumerate(test_cases_hg_meas, 1):
        print(f"\n-- HG_MEAS 테스트케이스 {i}")
        print("INSERT INTO REP_DATA_HG_MEAS_GEM_GENT_DA (")
        print("    TM, AREA_GRP_CD, AREA_GRP_ID, HGEN_PROD, HGEN_CAPA,")
        print("    REG_DATE, UPD_DATE")
        print(") VALUES (")
        print(f"    '{case['TM']}', '{case['AREA_GRP_CD']}', '{case['AREA_GRP_ID']}', {case['HGEN_PROD']}, {case['HGEN_CAPA']},")
        print(f"    TO_TIMESTAMP('{case['REG_DATE']}', 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('{case['UPD_DATE']}', 'YYYY-MM-DD HH24:MI:SS')")
        print(");")

def run_daily_simulation(next_day=False):
    """
    일일 시뮬레이션 실행 함수
    
    Args:
        next_day (bool): True이면 다음날 데이터 생성, False이면 오늘 데이터 생성
    """
    date_label = "다음날" if next_day else "오늘"
    print(f"\n{'='*60}")
    print(f"일일 에너지 데이터 시뮬레이션 시작 ({date_label}) - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    # 랜덤 시드 설정 (재현 가능한 결과를 위해)
    random.seed(int(time.time()))
    
    # 00시부터 23시까지의 데이터 생성 (24시간 운영, 24개 시간대)
    test_cases_lfd, test_cases_gen, test_cases_nwp, test_cases_kpx, test_cases_curt, test_cases_hg_gen, test_cases_hg_meas = generate_random_test_cases(next_day=next_day)
    
    # 결과 출력 (주석처리)
    # print_test_cases(test_cases_lfd, test_cases_gen, test_cases_nwp, test_cases_kpx, test_cases_curt, test_cases_hg_gen, test_cases_hg_meas)
    
    # PostgreSQL에 데이터 삽입 시도
    print(f"\n{'='*60}")
    print("PostgreSQL 데이터 삽입 시도")
    print(f"{'='*60}")
    
    success = insert_data_to_postgresql(test_cases_lfd, test_cases_gen, test_cases_nwp, test_cases_kpx, test_cases_curt, test_cases_hg_gen, test_cases_hg_meas)
    
    if success:
        print(f"\n✅ 일일 시뮬레이션이 성공적으로 완료되었습니다! - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print(f"\n⚠️ 데이터베이스 연결에 실패했지만 데이터는 성공적으로 생성되었습니다. - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # SQL INSERT 문도 함께 생성 (참고용) - 주석처리
    # generate_sql_insert_statements(test_cases_lfd, test_cases_gen, test_cases_nwp, test_cases_kpx, test_cases_curt, test_cases_hg_gen, test_cases_hg_meas)

def wait_until_midnight():
    """
    다음 자정까지 대기하는 함수
    """
    now = datetime.datetime.now()
    next_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
    wait_seconds = (next_midnight - now).total_seconds()
    
    print(f"다음 실행 시간: {next_midnight.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"대기 시간: {wait_seconds:.0f}초")
    
    time.sleep(wait_seconds)

def scheduler_loop():
    """
    스케줄러 루프
    """
    while True:
        try:
            # 다음 자정까지 대기
            wait_until_midnight()
            
            # 시뮬레이션 실행
            run_daily_simulation()
            
        except KeyboardInterrupt:
            print("\n프로그램이 종료되었습니다.")
            break
        except Exception as e:
            print(f"오류 발생: {e}")
            print("1분 후 다시 시도합니다...")
            time.sleep(60)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--manual":
            # 수동 실행 모드
            print("수동 실행 모드")
            next_day = "--next-day" in sys.argv
            if next_day:
                print("📅 다음날 데이터를 생성합니다.")
            run_daily_simulation(next_day=next_day)
        elif sys.argv[1] == "--truncate":
            # 테이블 데이터 삭제 모드
            print("테이블 데이터 삭제 모드")
            connection = get_db_connection()
            if connection:
                create_table_if_not_exists(connection)
                truncate_all_tables(connection)
                connection.close()
            else:
                print("❌ 데이터베이스 연결에 실패했습니다.")
        elif sys.argv[1] == "--help":
            # 도움말 표시
            print("에너지 데이터 시뮬레이터")
            print("=" * 50)
            print("사용법:")
            print("  python energy_data_simulator.py                    # 스케줄링 모드 (매일 24시 자동 실행)")
            print("  python energy_data_simulator.py --manual          # 수동 실행 모드 (오늘 데이터)")
            print("  python energy_data_simulator.py --manual --next-day # 수동 실행 모드 (다음날 데이터)")
            print("  python energy_data_simulator.py --truncate        # 모든 테이블 데이터 삭제")
            print("  python energy_data_simulator.py --help            # 도움말 표시")
            print("")
            print("옵션 설명:")
            print("  --manual     : 수동으로 데이터를 생성합니다.")
            print("  --next-day   : 다음날 데이터를 생성합니다. (--manual과 함께 사용)")
            print("  --truncate   : 모든 테이블의 데이터를 삭제합니다.")
            print("  --help       : 이 도움말을 표시합니다.")
        else:
            print("사용법:")
            print("  python energy_data_simulator.py                    # 스케줄링 모드 (매일 24시 자동 실행)")
            print("  python energy_data_simulator.py --manual          # 수동 실행 모드 (오늘 데이터)")
            print("  python energy_data_simulator.py --manual --next-day # 수동 실행 모드 (다음날 데이터)")
            print("  python energy_data_simulator.py --truncate        # 모든 테이블 데이터 삭제")
            print("  python energy_data_simulator.py --help            # 도움말 표시")
    else:
        # 스케줄링 모드 (24시에 자동 실행)
        print("스케줄링 모드 - 매일 24시(자정)에 자동 실행됩니다.")
        print("수동 실행을 원하시면 'python energy_data_simulator.py --manual' 명령어를 사용하세요.")
        print("다음날 데이터를 생성하려면 'python energy_data_simulator.py --manual --next-day' 명령어를 사용하세요.")
        print("테이블 데이터를 삭제하려면 'python energy_data_simulator.py --truncate' 명령어를 사용하세요.")
        print("프로그램을 종료하려면 Ctrl+C를 누르세요.")
        
        # 스케줄러 시작
        scheduler_loop()
