# 20260407_001 - PoChun Hsu - [Add]     Create database and dataset for superset.

from superset import db
from superset.models.core import Database

db_uri = "trino://trino@trino:8080/iceberg"

database_name = "Trino"

existing = db.session.query(Database).filter_by(database_name=database_name).first()

if not existing:
    database = Database(
        database_name=database_name,
        sqlalchemy_uri=db_uri,
        expose_in_sqllab=True,
        allow_run_async=True,
    )

    database.allow_dml = True

    db.session.add(database)
    db.session.commit()
    print("✅ Database created")
else:
    print("⚠️ Database already exists")
