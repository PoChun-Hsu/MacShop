# 20260407_001 - PoChun Hsu - [Add]     Create database and dataset for superset.
# 20260408_001 - PoChun Hsu - [Alter]   Turn off Async. No need for current situation.
# 20260408_002 - PoChun Hsu - [Add]     Add dataset for superset.

from superset import db
from superset.models.core import Database
from superset.connectors.sqla.models import SqlaTable

print("🔥 INIT DB START")

db_uri = "trino://trino@trino:8080/iceberg"

database_name = "Trino"

existing = db.session.query(Database).filter_by(database_name=database_name).first()

if not existing:
    database = Database(
        database_name=database_name,
        sqlalchemy_uri=db_uri,
        expose_in_sqllab=True,
        allow_run_async=False, # 20260408_001
    )

    database.allow_dml = True

    db.session.add(database)
    db.session.commit()
    print("✅ Database created")

    # 20260408_002 >>
    table_name = "test"
    schema = "default"

    dataset = db.session.query(SqlaTable).filter_by(
        table_name=table_name,
        schema=schema
    ).first()

    if not dataset:
        dataset = SqlaTable(
            table_name=table_name,
            schema=schema,
            database=database,
        )
        db.session.add(dataset)
        db.session.commit()
        print("✅ Dataset created")
    # 20260408_002 <<
else:
    print("⚠️ Database already exists")
