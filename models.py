import datetime
import peewee

db = peewee.SqliteDatabase('alerts.db', threadlocals=True)

class BaseModel(peewee.Model):
    class Meta:
        database = db

class Alert(BaseModel):
    alert_id = peewee.CharField()
    title = peewee.CharField()
    event = peewee.CharField()
    expires = peewee.DateTimeField()
    expires_utc_ts = peewee.DoubleField()
    url = peewee.CharField()
    fips_codes = peewee.TextField(null=True)
    ugc_codes = peewee.TextField(null=True)
    created = peewee.DateTimeField()

    def __repr__(self):
        return self.title

if __name__ == "__main__":
    db.connect()
    db.create_tables([Alert,])
