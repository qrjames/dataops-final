import psycopg2
import pandas as pd
from pymongo import MongoClient
#==============================================================#
#========GENERACION DE CHEQUES=================================*
#==============================================================*

# =============== CONFIG ===============
PG_CONFIGS = {
    "dev_inclub": {
        "dbname": "prod_inclub",
        "user": "postgres",
        "password": "#Intech2026$",
        "host": "psql-prod-backoffice.postgres.database.azure.com",
        "port": 5432
    },
    "dev_bo_admin": {
        "dbname": "prod_bo_admin",
        "user": "postgres",
        "password": "#Intech2026$",
        "host": "psql-prod-backoffice.postgres.database.azure.com",
        "port": 5432
    }
}

MONGO_URI = ("mongodb://boadmin:S1DJtC5nQKYtuu7w@212.56.44.91:31986/?retryWrites=true&loadBalanced=false&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256")
MONGO_DB = "Prod-Dashboard"           # BD Mongo

#Variables seteadas para el procesamiento
PERIOD_ID = 341                       # period_id para las colecciones Mongo
OUTPUT_FILE = "reporte_cheques_V3.xlsx"          
CHUNK_SIZE_INCLUB = 100000            

# =============== QUERIES ===============
QUERY_INCLUB = """
select
    us.id,
    us.username,
    us.name,
    us.lastname,
    sum(wt.amount) as total_amount
from bo_account."user" us
inner join bo_wallet.wallet wa on us.id = wa.iduser
inner join bo_wallet.wallettransaction wt on wa.idwallet = wt.idwallet
inner join bo_wallet.typewallettransaction tw on wt.idtypewallettransaction = tw.idtypewallettransaction
where wt.idtypewallettransaction = 8
group by us.id, us.username, us.name, us.lastname
order by us.id;
"""

QUERY_ADMIN = """
SELECT
    uc.iduser,
    uc.name,
    uc.lastname,
    uc.username,
    uc.createdate,
    af.idsponsor,
    uu.iduser AS "uu.iduser",
    uu.name AS "uu.name",
    uu.lastname AS "uu.lastname",
    uu.username AS "uu.username"
FROM usercustomer uc
LEFT JOIN affiliate af        ON uc.iduser = af.idson
LEFT JOIN usercustomer uu     ON af.idsponsor = uu.iduser
ORDER BY uc.iduser;
"""

def get_postgres_df(db_key: str, query: str, chunksize: int | None = None) -> pd.DataFrame:
    cfg = PG_CONFIGS[db_key]
    conn = psycopg2.connect(**cfg)
    try:
        if chunksize:
            parts = []
            for chunk in pd.read_sql(query, conn, chunksize=chunksize):
                parts.append(chunk)
            return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        else:
            return pd.read_sql(query, conn)
    finally:
        conn.close()

def get_mongo_df(collection_name: str, period_id: int) -> pd.DataFrame:
    client = MongoClient(MONGO_URI)
    try:
        db = client[MONGO_DB]
        cur = db[collection_name].find(
            {"period_id": period_id},
            {"_id": 0, "id_user": 1, "range": 1}
        )
        df = pd.DataFrame(list(cur))
        if not df.empty:
            df["id_user"] = pd.to_numeric(df["id_user"], errors="coerce")
        return df
    finally:
        client.close()

def main():
    print("Extrayendo dev_inclub (con chunks)...")
    df_inclub = get_postgres_df("dev_inclub", QUERY_INCLUB, chunksize=CHUNK_SIZE_INCLUB)
    
    df_inclub = df_inclub.rename(columns={
        "id": "us.id",
        "username": "us.username",
        "name": "us.name",
        "lastname": "us.lastname",
        "total_amount": "total_amount"
    })

    print("Extrayendo dev_bo_admin...")
    df_admin = get_postgres_df("dev_bo_admin", QUERY_ADMIN)
    
    df_admin = df_admin.rename(columns={
        "iduser": "uc.iduser",
        "name": "uc.name",
        "lastname": "uc.lastname",
        "username": "uc.username",
        "createdate" : "uc.createdate",
        "idsponsor": "af.idsponsor"
    })
    
    df_admin["uc.iduser"] = pd.to_numeric(df_admin["uc.iduser"], errors="coerce")

    print("Extrayendo Mongo period_compound...")
    df_compound = get_mongo_df("period_compound", PERIOD_ID).rename(columns={"range": "range_compound"})

    print("Extrayendo Mongo period_residual...")
    df_residual = get_mongo_df("period_residual", PERIOD_ID).rename(columns={"range": "range_residual"})

    print("Uniendo datasets...")
    
    merged = df_inclub.merge(
        df_admin,
        left_on="us.id",
        right_on="uc.iduser",
        how="left"
    )

    
    if not df_compound.empty:
        merged = merged.merge(
            df_compound[["id_user", "range_compound"]],
            left_on="us.id",
            right_on="id_user",
            how="left"
        ).drop(columns=["id_user"])

    
    if not df_residual.empty:
        merged = merged.merge(
            df_residual[["id_user", "range_residual"]],
            left_on="us.id",
            right_on="id_user",
            how="left"
        ).drop(columns=["id_user"])

    
    desired_cols = [
        "us.id", "us.username", "us.name", "us.lastname","uc.createdate", "total_amount",
        "uc.iduser", "uc.name", "uc.lastname", "uc.username",
        "af.idsponsor", "uu.iduser", "uu.name", "uu.lastname", "uu.username",
        "range_compound", "range_residual"
    ]
    
    for c in desired_cols:
        if c not in merged.columns:
            merged[c] = pd.NA

    merged = merged[desired_cols]

    print(f"Exportando a {OUTPUT_FILE} ...")
    merged.to_excel(OUTPUT_FILE, index=False)
    print(f"Listo. Filas exportadas: {len(merged)}")

if __name__ == "__main__":
    main()
